package main

import (
	// "bytes"
	"os"
	"context"
	"fmt"
	"math/big"
	"time"
	// "strconv"
	"database/sql"
	"runtime/pprof"

	"github.com/ledgerwatch/turbo-geth/cmd/utils"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/params"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/consensus/misc"
	// "github.com/ledgerwatch/turbo-geth/rlp"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/vm"
	"github.com/ledgerwatch/turbo-geth/core/vm/stack"
	"github.com/ledgerwatch/turbo-geth/core/state"
	"github.com/ledgerwatch/turbo-geth/consensus/ethash"
	"github.com/ledgerwatch/turbo-geth/turbo/shards"

	"github.com/spf13/cobra"
	_ "github.com/mattn/go-sqlite3"
)

type Flags struct {
	StartBlock uint64
	Block uint64
	EndBlock uint64
	Chaindata            string
}

var rootCmd = &cobra.Command{
	Use:   "accesses",
	Short: "Emits all accesses in the given block",
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		return nil
	},
	PersistentPostRunE: func(cmd *cobra.Command, args []string) error {
		return nil
	},
}

var singleCmd = &cobra.Command{
  Use:   "single",
  Short: "Run analysis over a single block",
}

func RootCommand() (*cobra.Command, *Flags) {
	utils.CobraFlags(rootCmd, nil)

	cfg := &Flags{}
	rootCmd.PersistentFlags().StringVar(&cfg.Chaindata, "chaindata", "", "path to the database")
	rootCmd.PersistentFlags().Uint64Var(&cfg.StartBlock, "start", 0, "The first block to be processed")
	rootCmd.PersistentFlags().Uint64Var(&cfg.Block, "block", 0, "The block to be processed")
	rootCmd.PersistentFlags().Uint64Var(&cfg.EndBlock, "end", 0, "The last block to be processed")
	return rootCmd, cfg
}

func main() {
	cpuprofile := "access.pprof"
        f, err := os.Create(cpuprofile)

        if err != nil {
            panic(err)
        }
        defer f.Close() // error handling omitted for example
        if err := pprof.StartCPUProfile(f); err != nil {
            panic(err)
        }
        defer pprof.StopCPUProfile()

	cmd, cfg := RootCommand()

	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		accesses(cfg.Chaindata, cfg.StartBlock, cfg.EndBlock)
		return nil
	}

	cmd.AddCommand(singleCmd)

	singleCmd.RunE = func(cmd *cobra.Command, args []string) error {
		singleBlock(cfg.Chaindata, cfg.Block)
		return nil
	}

	if err := cmd.ExecuteContext(utils.RootContext()); err != nil {
		fmt.Errorf(err.Error())
		os.Exit(1)
	}
}


func singleBlock(chainname string, blockNum uint64) {
	fmt.Println("Single block run", chainname, blockNum)

	db := ethdb.MustOpen(chainname)
	defer db.Close()

	analyzeBlock(db, blockNum)
}


func accesses(chainname string, startBlock uint64, endBlock uint64) {
	db := ethdb.MustOpen(chainname)
	defer db.Close()

	start := time.Now()
	for blockNum := startBlock; blockNum <= endBlock; blockNum ++ {
		if blockNum != startBlock {
			duration := time.Now().Sub(start).Seconds()
			rate := float64(blockNum - startBlock) / duration

			// I want a rate in terms of blocks / second

			// estimate is remaining blocks / rate

			var estimate float64

			if endBlock == blockNum {
				estimate = 0
			} else {
				estimate = float64(endBlock - blockNum) / rate
			}

			remaining := endBlock - blockNum
			fmt.Printf(
				"block=%d remaining=%d rate(blk/s)=%f ETA(s)=%f\n",
				blockNum,
				remaining,
				rate,
				estimate,
			)
		}

		analyzeBlock(db, blockNum)
	}
}


func analyzeBlock(db ethdb.Database, blockNum uint64) {
	// This is a copy of the parts of ExecuteBlockEphemerally (core/blockchain.go) and
	// it's callers which seemed relevant

	sm, err := ethdb.GetStorageModeFromDB(db)
	if err != nil {
		panic(err)
	}

	chainConfig := params.MainnetChainConfig

	tracer := NewAccessTracer()
	tracer.BlockNum = blockNum

	// If you're not passing in a Tracer you must use { Debug: false } unless you like
	// segfaults ;)
	vmConfig := &vm.Config {
		Debug: true, ReadOnly: true, NoReceipts: !sm.Receipts, Tracer: tracer,
	}

	faker := ethash.NewFaker()
	blockchain, err := core.NewBlockChain(db, nil, chainConfig, faker, *vmConfig, nil, nil)

	if err != nil {
		panic(err)
	}
	if blockchain == nil {
		panic("no blockchain")
	}
	defer blockchain.Stop()

	var tx ethdb.DbWithPendingMutations

	tx, err = db.Begin(context.Background(), ethdb.RW)
	if err != nil {
		panic(err)
	}
	defer tx.Rollback()

	blockHash, err := rawdb.ReadCanonicalHash(tx, blockNum)
	if err != nil {
		panic(fmt.Errorf("getting canonical blockhash for block %d: %v", blockNum, err))
	}

	// fmt.Println("found hash for block", blockHash.Hex())

	block := rawdb.ReadBlock(tx, blockHash, blockNum)
	if block == nil {
		panic(fmt.Errorf("getting block failed %d", blockNum))
	}

	// no idea what this is for.
	// I'm copying stage_call_traces.go:promoteCallTraces
	senders := rawdb.ReadSenders(tx, blockHash, blockNum)
	block.Body().SendersToTxs(senders)

	cache := shards.NewStateCache(32, 0)

	var stateReader state.StateReader
	reader := state.NewPlainDBState(tx, blockNum-1)
	stateReader = state.NewCachedReader(reader, cache)

	ibs := state.New(stateReader)
	ibs.SetTracer(tracer)

	noop := state.NewNoopWriter()
	gp := new(core.GasPool).AddGas(block.GasLimit())
	header := block.Header()
	usedGas := new(uint64)

	if chainConfig.DAOForkSupport && chainConfig.DAOForkBlock != nil && chainConfig.DAOForkBlock.Cmp(block.Number()) == 0 {
		misc.ApplyDAOHardFork(ibs)
	}

	tracer.OpenDB()
	defer tracer.CloseDB()

	tracer.beginTx()

	// expectedReceipts := blockchain.GetReceiptsByHash(blockHash)

	for _, txn := range block.Transactions() {
		tracer.InstructionCounter = 1
		tracer.CurrentTxn = txn.Hash()
		/*
		usedStr := strconv.FormatUint(*usedGas, 10)
		fmt.Printf(
			"ApplyTransaction(%d) tx=%x usedGas=%s limit=%d gasLeft=%d\n",
			i, txn.Hash(), usedStr, block.GasLimit(),
			gp.Gas(),
		)
		*/
		// receipt, err := core.ApplyTransaction(chainConfig, blockchain, nil, gp, ibs, noop, header, txn, usedGas, *vmConfig)
		_, err := core.ApplyTransaction(chainConfig, blockchain, nil, gp, ibs, noop, header, txn, usedGas, *vmConfig)
		if err != nil {
			panic(fmt.Errorf("tx %x failed: %v", txn.Hash(), err))
		}

		// expectedReceipt := expectedReceipts[i]

		/*
		expectedRLP, err := rlp.EncodeToBytes(expectedReceipt)
		if err != nil {
			panic(err)
		}
		actualRLP, err := rlp.EncodeToBytes(receipt)
		if err != nil {
			panic(err)
		}

		if bytes.Compare(expectedRLP, actualRLP) != 0 {
			fmt.Printf(
				"expected=%d actual=%d\n",
				expectedReceipt, receipt,
			)
			fmt.Printf(
				"expected=%d actual=%d\n",
				expectedReceipt.GasUsed, receipt.GasUsed,
			)
			panic(fmt.Errorf(
				"receipt mismatch(%d) tx=%x expectedUsed=%d actualUsed=%d",
				i, txn.Hash(),
				expectedReceipt.GasUsed, receipt.GasUsed,
			))
		}
		*/

	}

	tracer.endTx()

	// TOOD: Does this also record the coinbase value transfer?
	// TODO: contract code is also required for executing blocks, record it?
}


type AccessTracer struct {
	BlockNum uint64
	CurrentTxn common.Hash
	DB *sql.DB

	InstructionCounter uint32
	tx *sql.Tx
	insert *sql.Stmt
	insertSlot *sql.Stmt
}

func NewAccessTracer() *AccessTracer {
	return &AccessTracer {
	}
}

const CREATE_TABLE = `
CREATE TABLE IF NOT EXISTS account (
	blockNum INTEGER NOT NULL,
	account TEXT NOT NULL,
	PRIMARY KEY (blockNum, account)
);
CREATE TABLE IF NOT EXISTS slot (
	blockNum INTEGER NOT NULL,
	account TEXT NOT NULL,
	slot TEXT NOT NULL,
	PRIMARY KEY (blockNum, account, slot)
);
`

const INSERT_ACCESS = `
INSERT OR IGNORE INTO account(blockNum, account)
VALUES (?, ?);
`

const INSERT_ACCESS_SLOT = `
INSERT OR IGNORE INTO slot
VALUES (?, ?, ?);
`

func (ct *AccessTracer) OpenDB() {
	// I'm not sure why but on my machine anything but _sync=0 is comically slow. It
	// seems to be ignoring the transaction logic I have here and does an fsync after
	// every single command. _sync=0 disables fsync entirely which is regrettable but
	// much easier than figuring out what's going on.
	db, err := sql.Open("sqlite3", "./blockreads.db?_sync=0")

	if err != nil {
		panic(err)
	}

	err = db.Ping()
	if err != nil {
		panic(err)
	}

	_, err = db.Exec(CREATE_TABLE)
	if err != nil {
		panic(err)
	}

	ct.DB = db
}

func (ct *AccessTracer) beginTx() {
	tx, err := ct.DB.Begin()
	if err != nil {
		panic(err)
	}

	insert, err := ct.DB.Prepare(INSERT_ACCESS)
	if err != nil {
		panic(err)
	}

	insertSlot, err := ct.DB.Prepare(INSERT_ACCESS_SLOT)
	if err != nil {
		panic(err)
	}

	ct.tx = tx
	ct.insert = insert
	ct.insertSlot = insertSlot
}

func (ct *AccessTracer) endTx() {
	ct.tx.Commit()
	ct.insert.Close()
	ct.insertSlot.Close()
}

func (ct *AccessTracer) recordAccess(account string) {
	_, err := ct.insert.Exec(ct.BlockNum, account)
	if err != nil {
		panic(err)
	}
}

func (ct *AccessTracer) CloseDB() {
	ct.DB.Close()
}

func (ct *AccessTracer) recordAccessSlot(account string, slot string) {
	_, err := ct.insertSlot.Exec(ct.BlockNum, account, slot)
	if err != nil {
		panic(err)
	}
}


func (ct *AccessTracer) CaptureStart(depth int, from common.Address, to common.Address, precompile bool, create bool, calltype vm.CallType, input []byte, gas uint64, value *big.Int) error {
	// fmt.Printf("block=%d txn=%s depth=%d Start from=%s to=%s\n", ct.BlockNum, ct.CurrentTxn.Hex(), depth, from.Hex(), to.Hex())
	return nil
}
func (ct *AccessTracer) CaptureState(env *vm.EVM, pc uint64, op vm.OpCode, gas, cost uint64, memory *vm.Memory, stack *stack.Stack, _ *stack.ReturnStack, rData []byte, contract *vm.Contract, depth int, err error) error {

	ct.InstructionCounter += 1

	if op == vm.SLOAD {
		if stack.Len() < 1 {
			// TODO: what should happen here, this means the txn will fail?
			panic("no stack!")
		}
		slot := common.Hash(stack.Data[stack.Len()-1].Bytes32())
		address := contract.Address()
		// fmt.Printf("block=%d txn=%s SLOAD addr=%s slot=%s\n", ct.BlockNum, ct.CurrentTxn.Hex(), address.Hex(), slot.Hex())
		ct.recordAccessSlot(address.Hex(), slot.Hex())
	}

	if op == vm.SSTORE {
		if stack.Len() < 2 {
			panic("no stack!")
		}
		slot := common.Hash(stack.Data[stack.Len()-1].Bytes32())
		address := contract.Address()
		// fmt.Printf("block=%d txn=%s SSTORE addr=%s slot=%s\n", ct.BlockNum, ct.CurrentTxn.Hex(), address.Hex(), slot.Hex())
		ct.recordAccessSlot(address.Hex(), slot.Hex())
	}
	return nil
}
func (ct *AccessTracer) CaptureFault(env *vm.EVM, pc uint64, op vm.OpCode, gas, cost uint64, memory *vm.Memory, stack *stack.Stack, _ *stack.ReturnStack, contract *vm.Contract, depth int, err error) error {
	// fmt.Println("fault", depth, err)
	return nil
}
func (ct *AccessTracer) CaptureEnd(depth int, output []byte, gasUsed uint64, t time.Duration, err error) error {
	// fmt.Println("end", depth)
	return nil
}
func (ct *AccessTracer) CaptureSelfDestruct(from common.Address, to common.Address, value *big.Int) {
	// fmt.Println("SelfDestruct:", from, to)
}
func (ct *AccessTracer) CaptureAccountRead(account common.Address) error {
	// fmt.Printf("block=%d txn=%s CaptureAccountRead acct=%s\n", ct.BlockNum, ct.CurrentTxn.Hex(), account.Hex())
	ct.recordAccess(account.Hex())
	return nil
}
func (ct *AccessTracer) CaptureAccountWrite(account common.Address) error {
	// fmt.Printf("block=%d txn=%s CaptureAccountWrite acct=%s\n", ct.BlockNum, ct.CurrentTxn.Hex(), account.Hex())
	ct.recordAccess(account.Hex())
	return nil
}
