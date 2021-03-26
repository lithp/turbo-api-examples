package main

import (
	"context"
	"os"
	"fmt"
	"time"

	"github.com/ledgerwatch/turbo-geth/cmd/utils"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
	"github.com/ledgerwatch/turbo-geth/crypto"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/rlp"
	"github.com/ledgerwatch/turbo-geth/turbo/trie"
	"github.com/ledgerwatch/turbo-geth/turbo/rlphacks"
	"github.com/holiman/uint256"

	"github.com/spf13/cobra"
	_ "github.com/mattn/go-sqlite3"
)

type Flags struct {
	Chaindata            string
	Debug bool
}

var rootCmd = &cobra.Command{
	Use:   "export",
	Short: "Exports every hash in the state trie",
}

var DEBUG bool

func RootCommand() (*cobra.Command, *Flags) {
	utils.CobraFlags(rootCmd, nil)

	cfg := &Flags{}
	rootCmd.PersistentFlags().BoolVar(&cfg.Debug, "debug", false, "emit a lot more logging?")
	rootCmd.PersistentFlags().StringVar(&cfg.Chaindata, "chaindata", "", "path to the database")
	return rootCmd, cfg
}

func main() {
	cmd, cfg := RootCommand()

	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		DEBUG = cfg.Debug
		scanHashes(cfg.Chaindata)
		return nil
	}

	if err := cmd.ExecuteContext(utils.RootContext()); err != nil {
		fmt.Errorf(err.Error())
		os.Exit(1)
	}
}


type LoggingReceiver struct {
	defaultReceiver *trie.RootHashAggregator
}


func NewLoggingReceiver(defaultReceiver *trie.RootHashAggregator) *LoggingReceiver {
	return &LoggingReceiver{defaultReceiver: defaultReceiver}
}


func (receiver *LoggingReceiver) Receive(
		itemType trie.StreamItem,
		accountKey []byte,
		storageKey []byte,
		accountValue *accounts.Account,
		storageValue []byte,
		hash []byte,
		cutoff int,
	) error {

	var compressed []byte = []byte{}

	trie.CompressNibbles(accountKey, &compressed)

	if DEBUG {
		fmt.Printf(
			"receive type=%d addr=%x slot=%x acctRoot=%x item=%x h=%x c=%d dec=%x\n",
			itemType,
			accountKey,
			storageKey,
			accountValue.Root,
			storageValue,
			hash,
			cutoff,
			compressed,
		)
	}

	return receiver.defaultReceiver.Receive(
		itemType,
		accountKey,
		storageKey,
		accountValue,
		storageValue,
		hash,
		cutoff,
	)
}

func (receiver *LoggingReceiver) Result() trie.SubTries {
	return receiver.defaultReceiver.Result()
}
func (receiver *LoggingReceiver) Root() common.Hash {
	// this throws a panic, why is it called
	return receiver.defaultReceiver.Root()
}


type Decider struct {}

func (decider *Decider) Retain([]byte) bool {
	return true
}

func (decider *Decider) IsCodeTouched(common.Hash) bool {
	return false
}



type ProxyHashBuilder struct {
	hashBuilder *trie.HashBuilder
	currPath []byte
}

func (phb *ProxyHashBuilder) Reset() {
	phb.hashBuilder.Reset()
}

func (phb *ProxyHashBuilder) RootHash() (common.Hash, error) {
	return phb.hashBuilder.RootHash()
}

func (phb *ProxyHashBuilder) SetTrace(trace bool) {
	phb.hashBuilder.SetTrace(trace)
}

func (phb *ProxyHashBuilder) SetPath(path []byte) {
	phb.currPath = path
}

func (phb *ProxyHashBuilder) Leaf(length int, keyHex []byte, val rlphacks.RlpSerializable) error {
	// Leaf is never called because we retain none, only LeafHash is called

	// fmt.Printf("Leaf keyHex=%x\n", keyHex)
	err := phb.hashBuilder.Leaf(length, keyHex, val)
	if err != nil {
		return err
	}

	fmt.Printf("hb Leaf            hash=%x path=%x\n", phb.TopHash(), phb.currPath)

	//result := phb.hashBuilder.NodeStack[len(phb.hashBuilder.NodeStack)-1]
	//rlp := result.EncodeRLP()
	//fmt.Println("rlp=%x", rlp)
	return nil
}
func (phb *ProxyHashBuilder) LeafHash(length int, keyHex []byte, val rlphacks.RlpSerializable) error {
	phb.hashBuilder.LastNode.Reset()
	err := phb.hashBuilder.LeafHash(length, keyHex, val)
	if err != nil {
		return err
	}

	withoutTerminator := phb.currPath[:len(phb.currPath)-1]  // trim the trailing 0x10

	acct := withoutTerminator[:common.HashLength * 2]
	slot := withoutTerminator[common.HashLength * 2 + common.IncarnationLength * 2:]

	/*
	var compressed []byte = []byte{}
	trie.CompressNibbles(withoutTerminator, &compressed)

	acct, _, slot := dbutils.ParseCompositeStorageKey(compressed)
	*/

	contentKey := [][]byte { phb.TopHash(), acct, slot }
	encodedKey, err := rlp.EncodeToBytes(contentKey)
	if err != nil {
		return nil
	}

	contentID := crypto.Keccak256(encodedKey)

	nodebytes := phb.hashBuilder.LastNode.Bytes()
	phb.hashBuilder.LastNode.Reset()

	fmt.Printf("hb LeafHash        hash=%x acct=%x slot=%x val=%x content_key=%x content_id=%x\n", phb.TopHash(), acct, slot, nodebytes, encodedKey, contentID)
	return nil
}
func (phb *ProxyHashBuilder) AccountLeaf(length int, keyHex []byte, balance *uint256.Int, nonce uint64, incarnation uint64, fieldset uint32, codeSize int) error {
	// should never be called

	err := phb.hashBuilder.AccountLeaf(length, keyHex, balance, nonce, incarnation, fieldset, codeSize)
	if err != nil {
		return err
	}
	fmt.Printf("hb AccountLeaf     hash=%x path=%x\n", phb.TopHash(), phb.currPath)
	return nil
}
func (phb *ProxyHashBuilder) AccountLeafHash(length int, keyHex []byte, balance *uint256.Int, nonce uint64, incarnation uint64, fieldset uint32) error {
	phb.hashBuilder.LastNode.Reset()
	err := phb.hashBuilder.AccountLeafHash(length, keyHex, balance, nonce, incarnation, fieldset)
	if err != nil {
		return err
	}

	withoutTerminator := phb.currPath[:len(phb.currPath)-1]  // trim the trailing 0x10

	contentKey := [][]byte { phb.TopHash(), withoutTerminator }
	encodedKey, err := rlp.EncodeToBytes(contentKey)
	if err != nil {
		return nil
	}

	contentID := crypto.Keccak256(encodedKey)

	// var compressed []byte = []byte{}
	// trie.CompressNibbles(withoutTerminator, &compressed)

	nodebytes := phb.hashBuilder.LastNode.Bytes()
	phb.hashBuilder.LastNode.Reset()

	fmt.Printf("hb AccountLeafHash hash=%x acct=%x val=%x content_key=%x content_id=%x\n", phb.TopHash(), withoutTerminator, nodebytes, encodedKey, contentID)
	return nil
}
func (phb *ProxyHashBuilder) Extension(key []byte) error {
	phb.hashBuilder.LastNode.Reset()
	err := phb.hashBuilder.Extension(key)
	if err != nil {
		return err
	}
	fmt.Printf("hb Extension       hash=%x path=%x key=%x\n", phb.TopHash(), phb.currPath, key)
	return nil
}
func (phb *ProxyHashBuilder) ExtensionHash(key []byte) error {
	phb.hashBuilder.LastNode.Reset()
	err := phb.hashBuilder.ExtensionHash(key)
	if err != nil {
		return err
	}

	if len(phb.currPath) > common.HashLength * 2 /* the path is uncompressed */ {
		// we're in a storage trie

		acct := phb.currPath[:common.HashLength * 2]
		slot := phb.currPath[common.HashLength * 2 + common.IncarnationLength * 2:]

		contentKey := [][]byte { phb.TopHash(), acct, slot }
		encodedKey, err := rlp.EncodeToBytes(contentKey)
		if err != nil {
			return nil
		}

		contentID := crypto.Keccak256(encodedKey)

		nodebytes := phb.hashBuilder.LastNode.Bytes()
		phb.hashBuilder.LastNode.Reset()

		fmt.Printf("hb ExtensionHash   hash=%x path=%x key=%x val=%x content_key=%x content_id=%x\n", phb.TopHash(), phb.currPath, key, nodebytes, encodedKey, contentID)
		return nil
	} else {
		// we're in the main account trie
		contentKey := [][]byte { phb.TopHash(), phb.currPath }
		encodedKey, err := rlp.EncodeToBytes(contentKey)
		if err != nil {
			return nil
		}

		contentID := crypto.Keccak256(encodedKey)

		nodebytes := phb.hashBuilder.LastNode.Bytes()
		phb.hashBuilder.LastNode.Reset()

		fmt.Printf("hb ExtensionHash   hash=%x path=%x key=%x val=%x content_key=%x content_id=%x\n", phb.TopHash(), phb.currPath, key, nodebytes, encodedKey, contentID)
		return nil

	}
}
func (phb *ProxyHashBuilder) Branch(set uint16) error {
	phb.hashBuilder.LastNode.Reset()
	err := phb.hashBuilder.Branch(set)
	if err != nil {
		return err
	}
	fmt.Printf("hb Branch          hash=%x path=%x\n", phb.TopHash(), phb.currPath)
	return nil
}
func (phb *ProxyHashBuilder) BranchHash(set uint16) error {
	phb.hashBuilder.LastNode.Reset()
	err := phb.hashBuilder.BranchHash(set)
	if err != nil {
		return err
	}

	nodebytes := phb.hashBuilder.LastNode.Bytes()
	phb.hashBuilder.LastNode.Reset()

	if len(phb.currPath) > common.HashLength * 2 /* the path is uncompressed */ {
		// we're in a storage trie

		acct := phb.currPath[:common.HashLength * 2]
		slot := phb.currPath[common.HashLength * 2 + common.IncarnationLength * 2:]

		contentKey := [][]byte { phb.TopHash(), acct, slot }
		encodedKey, err := rlp.EncodeToBytes(contentKey)
		if err != nil {
			return nil
		}

		contentID := crypto.Keccak256(encodedKey)

		fmt.Printf(
			"hb BranchHash      hash=%x acct=%x slot=%x val=%x content_key=%x content_id=%x\n",
			phb.TopHash(), acct, slot, nodebytes, encodedKey, contentID,
		)

		return nil
	} else {
		// we're in the main account trie

		contentKey := [][]byte { phb.TopHash(), phb.currPath }
		encodedKey, err := rlp.EncodeToBytes(contentKey)
		if err != nil {
			return nil
		}

		contentID := crypto.Keccak256(encodedKey)

		fmt.Printf(
			"hb BranchHash      hash=%x path=%x val=%x content_key=%x content_id=%x\n",
			phb.TopHash(), phb.currPath, nodebytes, encodedKey, contentID,
		)
		return nil
	}

}
func (phb *ProxyHashBuilder) Hash(hash []byte) error {
	phb.hashBuilder.LastNode.Reset()
	err := phb.hashBuilder.Hash(hash)
	if err != nil {
		return err
	}
	if DEBUG {
		fmt.Printf("hb Hash            hash=%x path=%x\n", phb.TopHash(), phb.currPath)
	}
	return nil
}
func (phb *ProxyHashBuilder) TopHash() []byte {
	return phb.hashBuilder.TopHash()
}
func (phb *ProxyHashBuilder) HasRoot() bool {
	return phb.hashBuilder.HasRoot()
}



func scanHashes(chainname string) {
	ethDB := ethdb.MustOpen(chainname)
	// ethDB := ethdb.MustOpen("/home/brian/.local/share/turbogeth/tg/chaindata/")
	defer ethDB.Close()

	tx, err := ethDB.Begin(context.Background(), 0)
	if err != nil {
		panic(err)
	}
	defer tx.Rollback()

	startTime := time.Now()

	loader := trie.NewFlatDBTrieLoader(
		"scanHashes",  // logPrefix
		dbutils.CurrentStateBucket,
		dbutils.IntermediateTrieHashBucket,
	)

	loader.Reset(
		&Decider{},
		nil, /* hashcollector, which is almost what we want but doesn't include extensions*/
		false,
	)

	myPHB := &ProxyHashBuilder{trie.NewHashBuilder(false), []byte{}}

	defaultReceiver := &trie.RootHashAggregator{
		Hb: myPHB,
	}

	receiver := NewLoggingReceiver(defaultReceiver)
	receiver.defaultReceiver.Reset(
		nil,  /* hashcollector */
		false,
	)
	loader.SetStreamReceiver(receiver)

	root, err := loader.CalcTrieRoot(
		tx,
		nil, // a channel for stopping this thing
	)
	if err != nil {
		panic(err)
	}
	fmt.Printf("root=%x\n", root)
	fmt.Printf("time=%s\n", time.Since(startTime))
}
