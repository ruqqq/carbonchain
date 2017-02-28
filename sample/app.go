package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"github.com/boltdb/bolt"
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
	flag.Float64Var(&opReturnBtcFee, "fee", 0, "Transaction fee to use\n\t\t* REQUIRED ONLY FOR store COMMAND")
	flag.BoolVar(&help, "help", help, "Show help")
	flag.Parse()

	log.Printf("testnet: %v\n", testnet)
	log.Printf("datadir: %s\n", datadir)
	wd, _ := os.Getwd()
	log.Printf("app datadir: %s\n", wd)
	args := flag.Args()

	runtime.GOMAXPROCS(gomaxprocs)
	log.Printf("GOMAXPROCS: %d\n", runtime.GOMAXPROCS(-1))

	showHelp := func() {
		fmt.Fprint(os.Stderr, "Sample Carbonchain App\n(c)2017 Faruq Rasid\n\n"+
			"Commands:\n"+
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

	if len(args) > 0 && args[0] == "Store" {
		options := &rpc.RpcOptions{
			Host:    bitcoindIp,
			Port:    bitcoindPort,
			User:    bitcoindRpcUser,
			Pass:    bitcoindRpcPass,
			Testnet: testnet,
		}
		var data []byte
		if len(args) == 1 {
			reader := bufio.NewReader(os.Stdin)
			log.Printf("Enter string data: ")
			input, _ := reader.ReadString('\n')
			data = []byte(input[:len(input)-1])
		} else {
			data = []byte(args[1])
		}
		_, err := carbonchain.Store(data, 0xfa, opReturnBtcFee, options)
		if err != nil {
			log.Fatal(err)
		}

		return
	} else if len(args) > 0 && args[0] == "Start" {
		cc, err := carbonchain.NewCarbonChainWithDb(&carbonchain.CarbonChainOptions{LogLevel: carbonchain.LOG_LEVEL_DEBUG, Testnet: testnet, DataDir: datadir, PacketId: 0xfa, ProcessFunc: ProcessDatapack})
		if err != nil {
			log.Fatal(err)
		}
		defer cc.Close()

		if _, err := os.Stat(cc.Options.DataDir); err != nil {
			if os.IsNotExist(err) {
				panic(errors.New("Bitcoin data directory not found or is invalid."))
			}
		}

		if _, err := os.Stat(cc.Options.DataDir + "/blocks"); err != nil {
			if os.IsNotExist(err) {
				panic(errors.New("Bitcoin data directory not found or is invalid."))
			}
		}

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
}

func ProcessDatapack(cc *carbonchain.CarbonChain, carbonDb *bolt.DB) {
	datapacks := make([]carbonchain.Datapack, 0)
	datapackIds := make(map[int][]byte)

	// Get all available datapacks
	err := carbonDb.View(func(tx *bolt.Tx) error {
		bDatas := tx.Bucket([]byte("datas"))

		c := bDatas.Cursor()
		for i, datapackByte := c.First(); i != nil; i, datapackByte = c.Next() {
			datapack := *carbonchain.NewDatapackFromBytes(datapackByte)

			datapacks = append(datapacks, datapack)
			datapackIds[len(datapacks)-1] = i
		}

		return nil
	})
	if err != nil {
		log.Fatal(err)
	}

	// Consume datapacks
	if len(datapacks) > 0 {
		// Open datas file for writing our datapacks data
		var f *os.File
		if _, err := os.Stat("datas.txt"); err != nil {
			if os.IsNotExist(err) {
				var err error
				f, err = os.Create("datas.txt")
				if err != nil {
					log.Fatal(err)
				}
			} else {
				f, err = os.OpenFile("datas.txt", os.O_APPEND, 666)
				if err != nil {
					log.Fatal(err)
				}
			}
		}

		fmt.Printf("Datapacks (%d):\n", len(datapacks))
		for _, datapack := range datapacks {
			blockHash, err := cc.GetTransactionBlockHash(datapack.TxIds[0])
			if err != nil {
				log.Fatal(err)
			}
			confirmations, err := cc.GetBlockConfirmation(blockHash)
			if err != nil {
				log.Fatal(err)
			}
			out := fmt.Sprintf("[%s (c: %d)] %s\r\n", datapack.OutputAddr, confirmations, datapack.Data)
			fmt.Print("\t" + out)

			// Write data to file
			_, err = f.WriteString(out)
			if err != nil {
				log.Println("Unable to write to datas.txt!")
				log.Fatal(err)
			}
		}

		f.Close()

		// Delete the datapacks after consume
		err = carbonDb.Batch(func(tx *bolt.Tx) error {
			bDatas := tx.Bucket([]byte("datas"))
			for i := 0; i < len(datapacks); i++ {
				bDatas.Delete(datapackIds[i])
			}

			return nil
		})
		if err != nil {
			log.Fatal(err)
		}
	}
}
