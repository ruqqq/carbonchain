package main

import (
	"encoding/hex"
	"flag"
	"fmt"
	"github.com/ruqqq/blockchainparser"
	"github.com/ruqqq/blockchainparser/rpc"
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
	var bitcoindIp string
	var bitcoindPort string
	var bitcoindRpcUser string
	var bitcoindRpcPass string
	var opReturnBtcFee float64
	flag.BoolVar(&testnet, "testnet", false, "Use testnet")
	flag.StringVar(&datadir, "datadir", "", "Bitcoin data path")
	flag.IntVar(&gomaxprocs, "gomaxprocs", -1, "Number of threads to use")
	flag.StringVar(&bitcoindIp, "rpcip", bitcoindIp, "Bitcoind RPC IP\n\t\t* REQUIRED ONLY FOR STORE COMMAND")
	flag.StringVar(&bitcoindPort, "rpcport", bitcoindPort, "Bitcoind RPC Port (Default for testnet set to append 1 to this variable)\n\t\t* REQUIRED ONLY FOR store COMMAND")
	flag.StringVar(&bitcoindRpcUser, "rpcuser", bitcoindRpcUser, "User for bitcoind RPC\n\t\t* REQUIRED ONLY FOR store COMMAND")
	flag.StringVar(&bitcoindRpcPass, "rpcpassword", bitcoindRpcPass, "Password for bitcoind RPC\n\t\t* REQUIRED ONLY FOR store COMMAND")
	flag.Float64Var(&opReturnBtcFee, "fee", opReturnBtcFee, "Transaction fee to use\n\t\t* REQUIRED ONLY FOR store COMMAND")
	flag.BoolVar(&help, "help", help, "Show help")
	flag.Parse()

	fmt.Printf("testnet: %v\n", testnet)
	fmt.Printf("datadir: %s\n", datadir)
	args := flag.Args()

	runtime.GOMAXPROCS(gomaxprocs)
	log.Printf("GOMAXPROCS: %d\n", runtime.GOMAXPROCS(-1))

	showHelp := func() {
		fmt.Fprint(os.Stderr, "carbonchain\n(c)2017 Faruq Rasid\n\n"+
			"Commands:\n"+
			"  GetHeight\n"+
			"  GetBlock <hash>\n"+
			"  Store <data string>\n"+
			"  Start\n"+
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
	} else if len(args) > 0 && args[0] == "Store" {
		options := &rpc.RpcOptions{
			Host:    bitcoindIp,
			Port:    bitcoindPort,
			User:    bitcoindRpcUser,
			Pass:    bitcoindRpcPass,
			Testnet: testnet,
		}
		_, err := carbonchain.Store([]byte(args[1]), 0xfa, opReturnBtcFee, options)
		if err != nil {
			log.Fatal(err)
		}
	} else if len(args) > 0 && args[0] == "Start" {
		err = cc.Init()
		if err != nil {
			panic(err)
		}

		err = cc.Watch()
		if err != nil {
			panic(err)
		}

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
