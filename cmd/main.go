package main

import (
	"encoding/hex"
	"flag"
	"fmt"
	"github.com/ruqqq/blockchainparser"
	"github.com/ruqqq/carbonchain"
	"log"
	"os"
	"runtime"
)

func main() {
	var help bool
	var gomaxprocs int
	var testnet bool
	var datadir string
	flag.BoolVar(&testnet, "testnet", false, "Use testnet")
	flag.StringVar(&datadir, "datadir", "", "Bitcoin data path")
	flag.IntVar(&gomaxprocs, "gomaxprocs", -1, "Number of threads to use")
	flag.BoolVar(&help, "help", help, "Show help")
	flag.Parse()

	log.Printf("testnet: %v\n", testnet)
	log.Printf("datadir: %s\n", datadir)
	args := flag.Args()

	runtime.GOMAXPROCS(gomaxprocs)
	log.Printf("GOMAXPROCS: %d\n", runtime.GOMAXPROCS(-1))

	showHelp := func() {
		fmt.Fprint(os.Stderr, "carbonchain\n(c)2017 Faruq Rasid\n\n"+
			"Commands:\n"+
			"  GetHeight\n"+
			"  GetBlock <hash>\n"+
			"  GetTx <hash>\n"+
			"\n"+
			"Options:\n")
		flag.PrintDefaults()
	}

	if len(args) == 0 || help {
		showHelp()
		return
	}

	cc, err := carbonchain.NewCarbonChainWithDb(&carbonchain.CarbonChainOptions{Testnet: testnet, DataDir: datadir})
	if err != nil {
		log.Fatal(err)
	}

	if len(args) > 0 && args[0] == "GetHeight" {
		height, err := cc.GetBlockchainHeight()
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("Blockchain Height: %d\n", height)

		blockHash, err := cc.GetBlockHashAtHeight(height)
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("Top Most Block: %x\n", blockchainparser.ReverseHex(blockHash))

		return
	} else if len(args) == 2 && args[0] == "GetBlock" {
		h, err := hex.DecodeString(args[1])
		if err != nil {
			log.Fatal(err)
		}
		hash := blockchainparser.ReverseHex(h)
		block, err := cc.GetBlock(hash)
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("Block:\n%+v\n", block)

		nextHash, err := cc.GetNextHashInChain(hash)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("Next Block: %x\n", blockchainparser.ReverseHex(nextHash))

		return
	} else if len(args) == 2 && args[0] == "GetTx" {
		h, err := hex.DecodeString(args[1])
		if err != nil {
			log.Fatal(err)
		}
		hash := blockchainparser.ReverseHex(h)
		transaction, err := cc.GetTransaction(hash)
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("Transaction:\n%+v\n", transaction)

		blockHash, err := cc.GetTransactionBlockHash(hash)
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("Block Hash:\n%x\n", blockchainparser.ReverseHex(blockHash))

		confirmations, err := cc.GetBlockConfirmation(blockHash)
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("Confirmations: %d\n", confirmations)

		return
	}
	//} else if len(args) > 0 && args[0] == "rebuildChain" {
	//	err := rebuild_chain(cc.BlockDb, cc.ChainDb)
	//	if err != nil {
	//		panic(err)
	//	}
	//
	//	return
}
