package carbonchain

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/boltdb/bolt"
	"github.com/fsnotify/fsnotify"
	"github.com/lunixbochs/struc"
	"github.com/ruqqq/blockchainparser"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"time"
)

const (
	LOG_LEVEL_VERBOSE = iota
	LOG_LEVEL_INFO    = iota
	LOG_LEVEL_DEBUG   = iota
	LOG_LEVEL_ERROR   = iota
)

type CarbonChain struct {
	Options         *CarbonChainOptions
	BlockDb         *bolt.DB
	ChainDb         *bolt.DB
	CarbonDb        *bolt.DB
	curBlockFileNum uint32
	curBlockFilePos int64
}

type CarbonChainOptions struct {
	Testnet      bool
	BlockMagicId blockchainparser.MagicId
	PacketId     byte
	GenesisBlock blockchainparser.Hash256
	DataDir      string
	ProcessFunc  func(cc *CarbonChain, carbonDb *bolt.DB)
	LogLevel     int
}

type BlockMeta struct {
	FileNum  uint32                   `struc:"little"`
	Pos      uint64                   `struc:"little"`
	HashPrev blockchainparser.Hash256 `struc:"little,[32]byte"`
}

type TransactionMeta struct {
	FileNum   uint32                   `struc:"little"`
	BlockHash blockchainparser.Hash256 `struc:"little,[32]byte"`
	Pos       uint64                   `struc:"little"`
	Index     uint32                   `struc:"little"`
}

type BlockScanRequest struct {
	Index         int
	BlockStartPos int64
	FileNum       uint32
}

type BlockScanResult struct {
	Index int
}

func NewCarbonChain(blockDb *bolt.DB, chainDb *bolt.DB, carbonDb *bolt.DB, options *CarbonChainOptions) (*CarbonChain, error) {
	cc := &CarbonChain{BlockDb: blockDb, ChainDb: chainDb, CarbonDb: carbonDb, Options: options}
	if options == nil {
		options = &CarbonChainOptions{}
	}

	if options.BlockMagicId == blockchainparser.MagicId(0) {
		options.BlockMagicId = blockchainparser.BLOCK_MAGIC_ID_BITCOIN
	}

	if options.PacketId == 0 {
		options.PacketId = 0xfa
	}

	if options.GenesisBlock == nil {
		genesisBlockHash := "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f"
		if options.Testnet {
			genesisBlockHash = "000000000933ea01ad0ee984209779baaec3ced90fa3f408719526f8d77f4943"
		}
		genesisHex, _ := hex.DecodeString(genesisBlockHash)
		genesisHex = blockchainparser.ReverseHex(genesisHex)
		options.GenesisBlock = genesisHex
	}

	if options.DataDir == "" {
		options.DataDir = blockchainparser.BitcoinDir()
	}

	if options.Testnet {
		options.DataDir += "/testnet3"
		options.BlockMagicId = blockchainparser.BLOCK_MAGIC_ID_TESTNET
	}

	cc.Options = options

	// check directories
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

	// create buckets if needed
	err := cc.BlockDb.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte("meta"))
		if err != nil {
			log.Fatal(err)
			return fmt.Errorf("error create/open bucket: %s", err)
		}
		return err
	})
	if err != nil {
		return nil, err
	}
	err = cc.BlockDb.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte("blocks"))
		if err != nil {
			log.Fatal(err)
			return fmt.Errorf("error create/open bucket: %s", err)
		}
		return err
	})
	if err != nil {
		return nil, err
	}
	err = cc.BlockDb.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte("transactions"))
		if err != nil {
			log.Fatal(err)
			return fmt.Errorf("error create/open bucket: %s", err)
		}
		return err
	})
	if err != nil {
		return nil, err
	}
	err = cc.ChainDb.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte("chain"))
		if err != nil {
			log.Fatal(err)
			return fmt.Errorf("error create/open bucket: %s", err)
		}
		return err
	})
	if err != nil {
		return nil, err
	}
	err = cc.ChainDb.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte("forks"))
		if err != nil {
			log.Fatal(err)
			return fmt.Errorf("error create/open bucket: %s", err)
		}
		return err
	})
	if err != nil {
		return nil, err
	}
	err = cc.ChainDb.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte("heights"))
		if err != nil {
			log.Fatal(err)
			return fmt.Errorf("error create/open bucket: %s", err)
		}
		return err
	})
	if err != nil {
		return nil, err
	}
	err = cc.CarbonDb.Update(func(tx *bolt.Tx) error {
		//tx.DeleteBucket([]byte("packets"))
		_, err := tx.CreateBucketIfNotExists([]byte("packets"))
		if err != nil {
			log.Fatal(err)
			return fmt.Errorf("error create/open bucket: %s", err)
		}
		return err
	})
	if err != nil {
		return nil, err
	}
	err = cc.CarbonDb.Update(func(tx *bolt.Tx) error {
		//tx.DeleteBucket([]byte("datas"))
		_, err := tx.CreateBucketIfNotExists([]byte("datas"))
		if err != nil {
			log.Fatal(err)
			return fmt.Errorf("error create/open bucket: %s", err)
		}
		return err
	})
	if err != nil {
		return nil, err
	}

	return cc, nil
}

func NewCarbonChainWithDb(options *CarbonChainOptions) (*CarbonChain, error) {
	prefix := ""
	if options != nil && options.Testnet {
		prefix = "testnet_"
	}
	blockDb, err := bolt.Open(prefix+"database.db", 0600, nil)
	if err != nil {
		log.Fatal(err)
	}
	blockDb.NoSync = true

	chainDb, err := bolt.Open(prefix+"chain.db", 0600, nil)
	if err != nil {
		log.Fatal(err)
	}
	chainDb.NoSync = true

	carbonDb, err := bolt.Open(prefix+"carbon.db", 0600, nil)
	if err != nil {
		log.Fatal(err)
	}
	carbonDb.NoSync = true

	return NewCarbonChain(blockDb, chainDb, carbonDb, options)
}

func (cc *CarbonChain) Close() {
	cc.BlockDb.Close()
	cc.ChainDb.Close()
	cc.CarbonDb.Close()
}

func (cc *CarbonChain) Init() error {
	var err error

	// get metadata from db
	cc.BlockDb.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("meta"))
		blockFileNumByte := b.Get([]byte("lastBlockFileNum"))
		if blockFileNumByte != nil {
			cc.curBlockFileNum = binary.LittleEndian.Uint32(blockFileNumByte)
		}
		return nil
	})

	// Get last block file number
	blockFilepaths, _ := ioutil.ReadDir(cc.Options.DataDir + "/blocks/")
	lastBlockFileNum := 0
	for _, f := range blockFilepaths {
		if f.Name()[0:3] == "blk" {
			fileNum, _ := strconv.Atoi(f.Name()[3:8])
			//fmt.Printf("%d\n", fileNum)

			if fileNum > lastBlockFileNum {
				lastBlockFileNum = fileNum
			}
		}
	}
	if cc.Options.LogLevel <= LOG_LEVEL_INFO {
		log.Printf("Last block file number in data dir: %d\n", lastBlockFileNum)
		log.Printf("Last block file number scanned: %d\n", cc.curBlockFileNum)
	}

	log.Println("------PROCESSING BLOCKS------")
	for cc.curBlockFileNum <= uint32(lastBlockFileNum) {
		log.Printf("--> START BLOCKFILENUM: %d <--\n", cc.curBlockFileNum)

		// process the blocks we haven't
		cc.curBlockFilePos, err = cc.processBlocksForFileNum(cc.curBlockFileNum, 0)
		if err != nil {
			return err
		}

		log.Printf("--> END BLOCKFILENUM: %d <--\n", cc.curBlockFileNum)

		if cc.curBlockFileNum == uint32(lastBlockFileNum) {
			err = cc.BlockDb.Sync()
			if err != nil {
				return err
			}

			break
		} else {
			cc.curBlockFileNum++

			// save curBlockFileNum to bucket
			cc.BlockDb.Update(func(tx *bolt.Tx) error {
				b := tx.Bucket([]byte("meta"))
				data := make([]byte, 4)
				binary.LittleEndian.PutUint32(data, cc.curBlockFileNum)
				err := b.Put([]byte("lastBlockFileNum"), data)
				return err
			})

			err = cc.BlockDb.Sync()
			if err != nil {
				return err
			}

			err = cc.ChainDb.Sync()
			if err != nil {
				return err
			}

			err = cc.CarbonDb.Sync()
			if err != nil {
				return err
			}
		}
	}

	err = cc.processPacketQueue()
	if err != nil {
		return err
	}

	if cc.Options.ProcessFunc != nil {
		cc.Options.ProcessFunc(cc, cc.CarbonDb)
	}

	log.Println("----------END INIT----------")

	return nil
}

func (cc *CarbonChain) Watch(done chan bool) error {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Fatal(err)
	}
	defer watcher.Close()

	go func() {
		for {
			select {
			case event := <-watcher.Events:
				//log.Println("event:", event)
				if event.Op&fsnotify.Write == fsnotify.Write {
					_, filename := filepath.Split(event.Name)
					if filename[0:3] == "blk" {
						fileNum, _ := strconv.Atoi(filename[3:8])
						log.Printf("blk fileNum %d updated\n", fileNum)

						// Update lastBlockFileNum
						if uint32(fileNum) > cc.curBlockFileNum {
							// save curBlockFileNum to bucket
							err = cc.BlockDb.Update(func(tx *bolt.Tx) error {
								b := tx.Bucket([]byte("meta"))
								data := make([]byte, 4)
								binary.LittleEndian.PutUint32(data, cc.curBlockFileNum)
								err := b.Put([]byte("lastBlockFileNum"), data)
								return err
							})
							if err != nil {
								log.Fatal(err)
								return
							}
						}

						log.Println("------PROCESSING BLOCKS------")
						cc.curBlockFilePos, err = cc.processBlocksForFileNum(cc.curBlockFileNum, cc.curBlockFilePos)
						if err != nil {
							log.Fatal(err)
							return
						}
						log.Println("--------END PROCESSING-------")

						err = cc.processPacketQueue()
						if err != nil {
							log.Fatal(err)
							return
						}

						if cc.Options.ProcessFunc != nil {
							cc.Options.ProcessFunc(cc, cc.CarbonDb)
						}

						// Sync DBs
						cc.BlockDb.Sync()
						cc.ChainDb.Sync()
						cc.CarbonDb.Sync()
					} else if filename[0:3] == "rev" {
						//filenum, _ := strconv.Atoi(filename[3:8])
						//fmt.Printf("rev filenum: %d\n", filenum)
					} else {
						//fmt.Printf("changed:\n\tdir: %s\n\tfile: %s\n", dir, filename)
					}
				}
			case err := <-watcher.Errors:
				log.Println("error:", err)
			}
		}
	}()

	log.Println("Carbonchain started.")
	err = watcher.Add(cc.Options.DataDir + "/blocks")
	if err != nil {
		return err
	}
	<-done

	return nil
}

func (cc *CarbonChain) GetBlock(hash []byte) (*blockchainparser.Block, error) {
	var block *blockchainparser.Block
	err := cc.BlockDb.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("blocks"))
		blockMetaBytes := b.Get(hash)
		if blockMetaBytes == nil {
			return errors.New("Error: Block not found")
		}
		buf := bytes.NewBuffer(blockMetaBytes)
		blockMeta := &BlockMeta{}
		struc.Unpack(buf, blockMeta)
		//fmt.Printf("key=%x, value=%v\n", blockchainparser.ReverseHex(hash), blockMeta)

		var err error
		block, err = cc.readBlockFromBlockFile(blockMeta.FileNum, int64(blockMeta.Pos-4))
		if err != nil {
			return err
		}
		//fmt.Printf("block:\n%v\n", block)

		return nil
	})
	if err != nil {
		return nil, err
	}

	return block, nil
}

func (cc *CarbonChain) GetNextHashInChain(hash []byte) ([]byte, error) {
	var nextHash []byte
	err := cc.ChainDb.View(func(tx *bolt.Tx) error {
		bChain := tx.Bucket([]byte("chain"))
		nextHash = bChain.Get(hash)
		if nextHash == nil {
			return errors.New("Error: Hash not found in chain")
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return nextHash, nil
}

func (cc *CarbonChain) GetNextBlockInChain(hash []byte) (*blockchainparser.Block, error) {
	nextHash, err := cc.GetNextHashInChain(hash)
	if err != nil {
		return nil, err
	}
	block, err := cc.GetBlock(nextHash)
	if err != nil {
		return nil, err
	}

	return block, nil
}

func (cc *CarbonChain) GetBlockchainHeight() (int, error) {
	var height int
	err := cc.ChainDb.View(func(tx *bolt.Tx) error {
		bHeights := tx.Bucket([]byte("heights"))
		heightByte := bHeights.Get([]byte("maxHeight"))
		if heightByte == nil {
			return errors.New("Error: maxHeight not found")
		}
		height = int(binary.LittleEndian.Uint32(heightByte))

		return nil
	})
	if err != nil {
		return 0, err
	}

	return height, nil
}

func (cc *CarbonChain) GetBlockHashAtHeight(height int) ([]byte, error) {
	var hash []byte
	err := cc.ChainDb.View(func(tx *bolt.Tx) error {
		bHeights := tx.Bucket([]byte("heights"))
		heightByte := make([]byte, 4)
		binary.LittleEndian.PutUint32(heightByte, uint32(height))
		hash = bHeights.Get(heightByte)
		if hash == nil {
			return errors.New("Error: No block found at height")
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	return hash, nil
}

func (cc *CarbonChain) GetBlockAtHeight(height int) (*blockchainparser.Block, error) {
	hash, err := cc.GetBlockHashAtHeight(height)
	if err != nil {
		return nil, err
	}
	block, err := cc.GetBlock(hash)
	if err != nil {
		return nil, err
	}

	return block, nil
}

func (cc *CarbonChain) GetBlockConfirmation(hash []byte) (int, error) {
	confirmations := 0
	err := cc.ChainDb.View(func(tx *bolt.Tx) error {
		bHeights := tx.Bucket([]byte("heights"))
		heightByte := bHeights.Get(hash)
		if heightByte == nil {
			return errors.New("Error: Block not found in chain")
		}

		// do a reverse fetch (key = height)
		hash2 := bHeights.Get(heightByte)
		if hash2 == nil {
			return errors.New("Error: No block found at height")
		}

		// if block hash at height does not match the one we are querying, the block is orphaned
		if !bytes.Equal(hash, hash2) {
			return nil
		}

		height := binary.LittleEndian.Uint32(heightByte)

		maxHeightByte := bHeights.Get([]byte("maxHeight"))
		if maxHeightByte == nil {
			return errors.New("Error: maxHeight not found")
		}
		maxHeight := binary.LittleEndian.Uint32(maxHeightByte)

		confirmations = int(maxHeight-height) + 1

		return nil
	})
	if err != nil {
		return 0, err
	}

	return confirmations, nil
}

func (cc *CarbonChain) GetTransaction(hash []byte) (*blockchainparser.Transaction, error) {
	var transaction *blockchainparser.Transaction
	err := cc.BlockDb.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("transactions"))
		transactionMetaBytes := b.Get(hash)
		if transactionMetaBytes == nil {
			return errors.New("Error: Transaction not found")
		}
		buf := bytes.NewBuffer(transactionMetaBytes)
		transactionMeta := &TransactionMeta{}
		struc.Unpack(buf, transactionMeta)
		//fmt.Printf("key=%x, value=%v\n", blockchainparser.ReverseHex(hash), transactionMeta)

		var err error
		transaction, err = cc.readTransactionFromBlockFile(transactionMeta.FileNum, int64(transactionMeta.Pos))
		if err != nil {
			return err
		}
		//fmt.Printf("transaction:\n%v\n", transaction)

		return nil
	})
	if err != nil {
		return nil, err
	}

	return transaction, nil
}

func (cc *CarbonChain) GetTransactionBlockHash(hash []byte) ([]byte, error) {
	var blockHash []byte
	err := cc.BlockDb.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("transactions"))
		transactionMetaBytes := b.Get(hash)
		if transactionMetaBytes == nil {
			return errors.New("Error: Transaction not found")
		}
		buf := bytes.NewBuffer(transactionMetaBytes)
		transactionMeta := &TransactionMeta{}
		struc.Unpack(buf, transactionMeta)
		//fmt.Printf("key=%x, value=%v\n", blockchainparser.ReverseHex(hash), transactionMeta)

		blockHash = transactionMeta.BlockHash
		return nil
	})
	if err != nil {
		return nil, err
	}

	return blockHash, nil
}

func (cc *CarbonChain) GetTransactionMeta(hash []byte) (*TransactionMeta, error) {
	var transactionMeta *TransactionMeta
	err := cc.BlockDb.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("transactions"))
		transactionMetaBytes := b.Get(hash)
		if transactionMetaBytes == nil {
			return errors.New("Error: Transaction not found")
		}
		buf := bytes.NewBuffer(transactionMetaBytes)
		transactionMeta = &TransactionMeta{}
		struc.Unpack(buf, transactionMeta)
		//fmt.Printf("key=%x, value=%v\n", blockchainparser.ReverseHex(hash), transactionMeta)

		return nil
	})
	if err != nil {
		return nil, err
	}

	return transactionMeta, nil
}

func (cc *CarbonChain) readTransactionFromBlockFile(fileNum uint32, offset int64) (*blockchainparser.Transaction, error) {
	// Open the block file for processing
	blockFile, err := blockchainparser.NewBlockFile(cc.Options.DataDir, fileNum)
	if err != nil {
		return nil, err
	}
	defer blockFile.Close()

	_, err = blockFile.Seek(offset, 0)
	if err != nil {
		return nil, err
	}

	transaction, err := blockchainparser.ParseBlockTransactionFromFile(blockFile)
	if transaction != nil {
		transaction.StartPos = uint64(offset)
	}

	return transaction, err
}

func (cc *CarbonChain) readBlockFromBlockFile(fileNum uint32, offset int64) (*blockchainparser.Block, error) {
	// Open the block file for processing
	blockFile, err := blockchainparser.NewBlockFile(cc.Options.DataDir, fileNum)
	if err != nil {
		return nil, err
	}
	defer blockFile.Close()

	_, err = blockFile.Seek(offset, 0)
	if err != nil {
		return nil, err
	}

	block, err := blockchainparser.ParseBlockFromFile(blockFile, cc.Options.BlockMagicId)
	if block != nil {
		block.StartPos = uint64(offset + 4)
	}

	return block, err
}

func (cc *CarbonChain) processBlocksForFileNum(fileNum uint32, skip int64) (int64, error) {
	// Open the block file for processing
	blockFile, err := blockchainparser.NewBlockFile(cc.Options.DataDir, fileNum)
	if err != nil {
		return 0, err
	}
	defer blockFile.Close()

	// Get file length
	length, _ := blockFile.Size()
	if cc.Options.LogLevel <= LOG_LEVEL_INFO {
		log.Printf("Length: %d\n", length)
	}

	// Skip if needed
	if skip > 0 {
		_, err := blockFile.Seek(skip, 0)
		if err != nil {
			return 0, err
		}
	}

	// Create bucket for checking scanning if needed
	err = cc.BlockDb.Update(func(tx *bolt.Tx) error {
		bucketName := fmt.Sprintf("blockFile%dScannedBlockPos", fileNum)
		_, err := tx.CreateBucketIfNotExists([]byte(bucketName))
		if err != nil {
			log.Fatal(err)
			return fmt.Errorf("Error create/open bucket: %s", err)
		}
		return nil
	})
	if err != nil {
		return 0, err
	}

	// Record start time for measuring later
	startTime := time.Now()

	// Find all the blocks in the file
	blockPos := make([]int64, 0)
	lastBlockEndPos := int64(0)
	for true {
		blockStartPos, blockEndPos, err := cc.findNextBlockFromBlockFile(blockFile)
		if err != nil {
			if err.Error() == "Invalid block header: Can't find Magic ID" {
				break
			}
			return 0, err
		}

		blockPos = append(blockPos, blockStartPos)
		lastBlockEndPos = blockEndPos
	}
	if cc.Options.LogLevel <= LOG_LEVEL_INFO {
		log.Printf("%d blocks found. Took: %d sec\n", len(blockPos), time.Now().Unix()-startTime.Unix())
	}

	startTime = time.Now()

	// Create workers to scan the block in parallel
	numWorkers := runtime.GOMAXPROCS(-1) * 50 // e.g. 16 PROCS * 50 = 400 workers

	blockScanRequestChan := make(chan BlockScanRequest, numWorkers)
	blockScanResultChan := make(chan BlockScanResult, numWorkers)

	if cc.Options.LogLevel <= LOG_LEVEL_INFO {
		log.Println("Parsing...")
	}

	for w := 0; w < numWorkers; w++ {
		go cc.blockScanWorker(blockScanRequestChan, blockScanResultChan)
	}

	for index, blockStartPos := range blockPos {
		blockScanRequestChan <- BlockScanRequest{index, blockStartPos, blockFile.FileNum}
	}
	close(blockScanRequestChan)

	for completed := range blockPos {
		result := <-blockScanResultChan

		if cc.Options.LogLevel <= LOG_LEVEL_VERBOSE {
			fmt.Printf("\rProgress: %.2f%% (index: %d)", float64(completed)*100.0/float64(len(blockPos)), result.Index)
		}
	}
	if cc.Options.LogLevel <= LOG_LEVEL_VERBOSE {
		fmt.Println("")
		log.Println("")
	}
	close(blockScanResultChan)

	if cc.Options.LogLevel <= LOG_LEVEL_INFO {
		log.Printf("%d blocks scanned. Took: %d sec\n", len(blockPos), time.Now().Unix()-startTime.Unix())
	}

	forks := make(map[string][][]byte)

	// TODO: Fork resolving should be done with block verification; block verification is NOT implemented yet
	// Detect forks
	cc.ChainDb.View(func(tx *bolt.Tx) error {
		bFork := tx.Bucket([]byte("forks"))
		if bFork.Stats().KeyN == 0 {
			return nil
		}

		c := bFork.Cursor()
		for hash, hashPrev := c.First(); hash != nil; hash, hashPrev = c.Next() {
			if forks[hex.EncodeToString(hashPrev)] == nil {
				forks[hex.EncodeToString(hashPrev)] = make([][]byte, 0)
			}

			found := false
			for i := 0; i < len(forks[hex.EncodeToString(hashPrev)]); i++ {
				if bytes.Equal(forks[hex.EncodeToString(hashPrev)][i], hash) {
					found = true
					break
				}
			}

			if !found {
				forks[hex.EncodeToString(hashPrev)] = append(forks[hex.EncodeToString(hashPrev)], hash)
			}
		}

		if cc.Options.LogLevel <= LOG_LEVEL_INFO {
			log.Printf("Forks detected: %d\n", len(forks))
			log.Println("Will attempt to fix...")
		}

		return nil
	})

	// Fix forks
	for prev, fork := range forks {
		hashPrev, _ := hex.DecodeString(prev)

		winningHash := make([]byte, len(fork[0]))
		copy(winningHash, fork[0])
		winningHeight := 0
		resolved := false

		if cc.Options.LogLevel <= LOG_LEVEL_VERBOSE {
			log.Printf("Fixing chain: %x, candidates: %d\n", blockchainparser.ReverseHex(hashPrev), len(fork))
		}

		for _, nextHash := range fork {
			if cc.Options.LogLevel <= LOG_LEVEL_VERBOSE {
				log.Printf("\tTracing %x\n", blockchainparser.ReverseHex(nextHash))
			}

			height := 0
			hash := nextHash

			cc.ChainDb.View(func(tx *bolt.Tx) error {
				bChain := tx.Bucket([]byte("chain"))

				// Trace at max 6 height
				for hash != nil && height <= 6 {
					hash = bChain.Get(hash)
					height++
				}

				return nil
			})

			if cc.Options.LogLevel <= LOG_LEVEL_VERBOSE {
				log.Printf("\tPossible chain: %x -> %x\n", blockchainparser.ReverseHex(hashPrev), blockchainparser.ReverseHex(nextHash))
			}

			if height > winningHeight {
				winningHeight = height
				copy(winningHash, nextHash)
				resolved = true
			}
		}

		if resolved {
			err = cc.ChainDb.Update(func(tx *bolt.Tx) error {
				bChain := tx.Bucket([]byte("chain"))
				bHeights := tx.Bucket([]byte("heights"))

				// Relink the chain
				err = bChain.Put(hashPrev, winningHash)
				if err != nil {
					return err
				}

				// Backtrack the maxHeight so we can recalculate heights for subsequent chains
				maxHeightByte := bHeights.Get([]byte("maxHeight"))
				hashPrevHeightByte := bHeights.Get(hashPrev)
				if maxHeightByte != nil && hashPrevHeightByte != nil {
					if binary.LittleEndian.Uint32(hashPrevHeightByte) < binary.LittleEndian.Uint32(maxHeightByte) {
						err = bHeights.Put([]byte("maxHeight"), hashPrevHeightByte)
						if err != nil {
							return err
						}
					}
				}

				return nil
			})
			if err != nil {
				return 0, err
			}

			if cc.Options.LogLevel <= LOG_LEVEL_INFO {
				log.Printf("Mainchain: %x -> %x\n", blockchainparser.ReverseHex(hashPrev), blockchainparser.ReverseHex(winningHash))
			}
		}
	}

	// Update heights
	startTime = time.Now()
	err = cc.ChainDb.Update(func(tx *bolt.Tx) error {
		bChain := tx.Bucket([]byte("chain"))
		bHeights := tx.Bucket([]byte("heights"))
		height := uint32(0)

		// Build height from last known maxHeight or Genesis block
		var firstBlockHash []byte
		maxHeightByte := bHeights.Get([]byte("maxHeight"))
		if maxHeightByte != nil {
			height = binary.LittleEndian.Uint32(maxHeightByte)
			topBlockHashByte := bHeights.Get(maxHeightByte)
			if topBlockHashByte == nil {
				return errors.New("Error: Have maxHeight but cannot find last block")
			}
			firstBlockHash = topBlockHashByte
		} else {
			firstBlockHash = cc.Options.GenesisBlock

			heightByte := make([]byte, 4)
			binary.LittleEndian.PutUint32(heightByte, height)
			err = bHeights.Put(firstBlockHash, heightByte)
			if err != nil {
				return err
			}
			err = bHeights.Put(heightByte, firstBlockHash)
			if err != nil {
				return err
			}
		}

		if firstBlockHash == nil {
			return errors.New("Error: Cannot find firstBlockHash")
		}

		oldHeight := height
		if cc.Options.LogLevel <= LOG_LEVEL_INFO {
			log.Println("Calculating heights...")
			log.Printf("Start from block: %x, Current height: %d\n", blockchainparser.ReverseHex(firstBlockHash), height)
		}
		if cc.Options.LogLevel <= LOG_LEVEL_VERBOSE {
			fmt.Println("")
		}

		hash := firstBlockHash
		completed := 0
		var lastHash []byte
		for hash != nil {
			hash = bChain.Get(hash)
			if hash == nil {
				break
			}
			height++

			heightByte := make([]byte, 4)
			binary.LittleEndian.PutUint32(heightByte, height)
			err = bHeights.Put(hash, heightByte)
			if err != nil {
				return err
			}
			err = bHeights.Put(heightByte, hash)
			if err != nil {
				return err
			}

			lastHash = hash
			completed++
			if cc.Options.LogLevel <= LOG_LEVEL_VERBOSE {
				fmt.Printf("\rProgress: %.2f%% - %d: %x", float64(completed)*100.0/float64(len(blockPos)), height, blockchainparser.ReverseHex(hash))
			}
		}

		maxHeightByte = make([]byte, 4)
		binary.LittleEndian.PutUint32(maxHeightByte, height)
		err = bHeights.Put([]byte("maxHeight"), maxHeightByte)
		if err != nil {
			return err
		}

		if cc.Options.LogLevel <= LOG_LEVEL_VERBOSE {
			fmt.Println("")
			log.Println("")
		}
		if oldHeight != height {
			if cc.Options.LogLevel <= LOG_LEVEL_INFO {
				log.Printf("New Height: %d - %x\n", height, blockchainparser.ReverseHex(lastHash))
			}
		}

		return nil
	})
	if err != nil {
		return 0, err
	}

	// Clean up old forks
	deletedForks := 0
	err = cc.ChainDb.Update(func(tx *bolt.Tx) error {
		bFork := tx.Bucket([]byte("forks"))
		bHeights := tx.Bucket([]byte("heights"))

		maxHeightByte := bHeights.Get([]byte("maxHeight"))

		// Delete fork records if it is a confirmed block (current max height - block height > 6)
		for _, fork := range forks {
			isDeleted := false
			for _, nextHash := range fork {
				hashHeightByte := bHeights.Get(nextHash)
				if hashHeightByte == nil || binary.LittleEndian.Uint32(maxHeightByte)-binary.LittleEndian.Uint32(hashHeightByte) > 6 {
					err = bFork.Delete(nextHash)
					if err != nil {
						return err
					}
					isDeleted = true
				}
			}
			if isDeleted {
				deletedForks++
			}
		}

		return nil
	})
	if err != nil {
		return 0, err
	}
	if deletedForks > 0 && cc.Options.LogLevel <= LOG_LEVEL_INFO {
		log.Printf("Deleted Old Forks: %d\n", deletedForks)
	}

	if cc.Options.LogLevel <= LOG_LEVEL_INFO {
		log.Printf("Chain + Height updated. Took: %d sec\n", time.Now().Unix()-startTime.Unix())
	}

	return lastBlockEndPos, nil
}

func (cc *CarbonChain) findNextBlockFromBlockFile(blockFile *blockchainparser.BlockFile) (int64, int64, error) {
	size, err := blockFile.Size()
	if err != nil {
		return 0, 0, err
	}

	curPos, err := blockFile.Seek(0, 1)
	if err != nil {
		return 0, 0, err
	}

	var magicId blockchainparser.MagicId

	// Read and validate Magic ID
	for magicId != cc.Options.BlockMagicId {
		magicId = blockchainparser.MagicId(blockFile.ReadUint32())
		pos, err := blockFile.Seek(0, 1)
		if err != nil {
			// Seek back to last pos before error occur
			blockFile.Seek(curPos, 0)
			return 0, 0, err
		}
		if pos == size {
			// Seek back to last pos before error occur
			blockFile.Seek(curPos, 0)
			return 0, 0, errors.New("Invalid block header: Can't find Magic ID")
		}
	}

	// Get pos after magic header
	startPos, err := blockFile.Seek(0, 1)
	if err != nil {
		// Seek back to last pos before error occur
		blockFile.Seek(curPos, 0)
		return 0, 0, err
	}

	length := blockFile.ReadUint32()
	endPos, err := blockFile.Seek(int64(length), 1)
	if err != nil {
		// Seek back to last pos before error occur
		blockFile.Seek(curPos, 0)
		return 0, 0, err
	}

	return startPos, endPos, nil
}

func (cc *CarbonChain) blockScanWorker(blockScanRequestChan chan BlockScanRequest, blockScanResultChan chan BlockScanResult) {
	for blockScanRequest := range blockScanRequestChan {
		index := blockScanRequest.Index
		blockStartPos := blockScanRequest.BlockStartPos
		fileNum := blockScanRequest.FileNum

		if blockStartPos == 0 {
			continue
		}

		// check if block already scanned
		blockFileScannedBlockPosBucketName := fmt.Sprintf("blockFile%dScannedBlockPos", fileNum)
		isScanned := false

		cc.BlockDb.View(func(bTx *bolt.Tx) error {
			b := bTx.Bucket([]byte(blockFileScannedBlockPosBucketName))
			if b != nil {
				key := make([]byte, 8)
				binary.LittleEndian.PutUint64(key, uint64(blockStartPos))
				result := b.Get(key)
				if result != nil {
					isScanned = true
				}
			}

			return nil
		})

		if isScanned {
			blockScanResultChan <- BlockScanResult{index}
			continue
		}

		// parse block data
		block, err := cc.readBlockFromBlockFile(fileNum, blockStartPos-4)
		if err != nil {
			panic(err)
			blockScanResultChan <- BlockScanResult{index}
			continue
		}
		blockHash := block.Hash()

		// save txs pos in db
		for i, tx := range block.Transactions {
			// save transaction pos in db
			var buf bytes.Buffer
			transactionMeta := &TransactionMeta{FileNum: fileNum, BlockHash: blockHash, Pos: tx.StartPos, Index: uint32(i)}
			err := struc.Pack(&buf, transactionMeta)
			if err != nil {
				log.Fatalln(err)
				blockScanResultChan <- BlockScanResult{index}
				continue
			}

			err = cc.BlockDb.Batch(func(bTx *bolt.Tx) error {
				b := bTx.Bucket([]byte("transactions"))
				err = b.Put(tx.Txid(), buf.Bytes())
				return err
			})
			if err != nil {
				log.Fatalln(err)
				blockScanResultChan <- BlockScanResult{index}
				continue
			}

			var outputAddr []byte
			// find vout with OP_DUP OP_HASH160 [20] OP_EQUALVERIFY OP_CHECKSIG
			for _, vout := range tx.Vout {
				if len(vout.Script) == 25 && vout.Script[0] == 0x76 && vout.Script[1] == 0xa9 && vout.Script[len(vout.Script)-2] == 0x88 && vout.Script[len(vout.Script)-1] == 0xac {
					outputAddr = vout.Script[3 : len(vout.Script)-2]
					break
				}
			}

			if outputAddr == nil {
				continue
			}

			for _, vout := range tx.Vout {
				// find out if we have OP_RETURN (and minimum len of script for a valid packet)
				if len(vout.Script) <= 20 || vout.Script[0] != 0x6a {
					continue
				}

				//log.Printf("OP_RETURN: %v\n", vout.Script)

				var payload []byte

				if vout.Script[1] == byte(0x4d) {
					payload = vout.Script[4:]
				} else if vout.Script[1] == byte(0x4c) {
					payload = vout.Script[3:]
				} else {
					payload = vout.Script[2:]
				}

				// don't have enough bytes to parse, unlikely our packet
				if len(payload) <= 20 {
					continue
				}

				// is this the payload we're looking for
				if payload[0] != cc.Options.PacketId {
					continue
				}

				packet := NewPacketFromBytes(payload)
				if len(packet.Checksum[:]) == 0 || !bytes.Equal(packet.CalculateChecksum(), packet.Checksum[:]) || bytes.Equal([]byte{0, 0, 0, 0, 0, 0, 0, 0}, packet.Checksum[:]) {
					// drop packets with invalid checksum
					//log.Printf("Checksum mismatch: %+v\n", packet)
					continue
				}
				packet.Txid = tx.Txid()
				copy(packet.OutputAddr[:], outputAddr)
				packet.Timestamp = time.Now().Unix()

				if cc.Options.LogLevel <= LOG_LEVEL_INFO {
					log.Printf("Received packet in Txid: %v\n", packet.Txid)
				}
				if cc.Options.LogLevel <= LOG_LEVEL_VERBOSE {
					log.Printf("\tData (%x, %d): %s\n", outputAddr, packet.Sequence, packet.Data)
					//log.Printf("\tChecksum: %x vs %x\n", packet.Checksum[:], packet.CalculateChecksum())
					//log.Printf("\tOutput Addr: %x (%d)\n", outputAddr, len(outputAddr))
					//log.Printf("\tOutput Script: %x (%d)\n", script, len(script))
				}

				err = cc.CarbonDb.Batch(func(tx *bolt.Tx) error {
					bPackets := tx.Bucket([]byte("packets"))
					p := bPackets.Bucket(outputAddr)
					if p == nil {
						id, _ := bPackets.NextSequence()
						bId := make([]byte, 8)
						binary.BigEndian.PutUint64(bId, uint64(id))
						err = bPackets.Put(bId, outputAddr)
						if err != nil {
							return err
						}
						if cc.Options.LogLevel <= LOG_LEVEL_VERBOSE {
							log.Printf("\tCreated bucket: %x\n", outputAddr)
						}

						p, err = bPackets.CreateBucket(outputAddr)
						if err != nil {
							return err
						}
					}

					packetByte := packet.DbBytes()
					id, _ := p.NextSequence()
					bId := make([]byte, 8)
					binary.BigEndian.PutUint64(bId, uint64(id))
					err = p.Put(bId, packetByte)
					if err != nil {
						return err
					}

					return err
				})
				if err != nil {
					log.Fatalln(err)
					blockScanResultChan <- BlockScanResult{index}
					continue
				}
			}
		}

		// prepare to save block meta in db
		var buf bytes.Buffer
		blockMeta := &BlockMeta{FileNum: fileNum, Pos: block.StartPos, HashPrev: block.HashPrev}
		err = struc.Pack(&buf, blockMeta)
		if err != nil {
			panic(err)
			blockScanResultChan <- BlockScanResult{index}
			continue
		}

		// update chain
		err = cc.ChainDb.Batch(func(tx *bolt.Tx) error {
			bChain := tx.Bucket([]byte("chain"))
			bFork := tx.Bucket([]byte("forks"))

			hash := block.Hash()

			hashNext := bChain.Get(block.HashPrev)
			if hashNext == nil {
				// if not a competing block (chain), do a normal insert
				err = bChain.Put(block.HashPrev, hash)
				if err != nil {
					return err
				}
			} else {
				// else ...

				//fmt.Printf("\nDetected conflict: %x\n", blockchainparser.ReverseHex(hashNext))
				if !bytes.Equal(hashNext, hash) {
					err = bFork.Put(hashNext, block.HashPrev)
					if err != nil {
						return err
					}
					err = bFork.Put(hash, block.HashPrev)
					if err != nil {
						return err
					}
				}
			}

			return nil
		})
		if err != nil {
			panic(err)
			return
		}

		// prepare to save block pos to db
		blockStartPosAsKey := make([]byte, 8)
		binary.LittleEndian.PutUint64(blockStartPosAsKey, uint64(blockStartPos))

		// do the actual saving in batch
		err = cc.BlockDb.Batch(func(bTx *bolt.Tx) error {
			b := bTx.Bucket([]byte("blocks"))
			err = b.Put(block.Hash(), buf.Bytes())
			if err != nil {
				return err
			}

			// record block as scanned in db
			b = bTx.Bucket([]byte(blockFileScannedBlockPosBucketName))
			err = b.Put(blockStartPosAsKey, []byte{1})
			if err != nil {
				return err
			}

			return nil
		})
		if err != nil {
			panic(err)
			blockScanResultChan <- BlockScanResult{index}
			continue
			//return err
		}

		blockScanResultChan <- BlockScanResult{index}
	}
}

func (cc *CarbonChain) processPacketQueue() error {
	packetQueue := make([][]Packet, 0)

	err := cc.CarbonDb.View(func(tx *bolt.Tx) error {
		bPackets := tx.Bucket([]byte("packets"))

		outputAddrToPacketsMap := make(map[string][]Packet)
		packetQueueToOutputAddrMap := make(map[int][]byte)

		c := bPackets.Cursor()
		for id, outputAddr := c.First(); id != nil; id, outputAddr = c.Next() {
			b := bPackets.Bucket(outputAddr)
			// TODO: This is not supposed to be nil at all. Check why bucket has empty outputAddr as key
			if b == nil {
				continue
			}
			//log.Printf("Getting packets for %d: %x\n", binary.BigEndian.Uint64(id), outputAddr)

			c2 := b.Cursor()
			for id2, packetByte := c2.First(); id2 != nil; id2, packetByte = c2.Next() {
				packet := *NewPacketFromDbBytes(packetByte)
				//log.Printf("%d: %x\n", binary.BigEndian.Uint64(id2), packetByte)
				//log.Printf("%d: %+v\n", binary.BigEndian.Uint64(id2), packet)
				//log.Printf("%d: %s\n", binary.BigEndian.Uint64(id2), packet.Txid)

				//if bytes.Equal([]byte{0, 0, 0, 0, 0, 0, 0, 0}, packet.Checksum[:]) {
				//	continue
				//}

				// ignore packets added more than 3 days ago
				if time.Unix(packet.Timestamp, 0).Before(time.Now().AddDate(0, 0, -3)) {
					log.Printf("\tIgnoring %d: %+v (more than 3 days old!)\n", binary.BigEndian.Uint64(id2), packet)
					continue
				}

				if outputAddrToPacketsMap[hex.EncodeToString(outputAddr)] == nil {
					queue := make([]Packet, 0)
					packetQueue = append(packetQueue, queue)
					outputAddrToPacketsMap[hex.EncodeToString(outputAddr)] = queue
					packetQueueToOutputAddrMap[len(packetQueue)-1] = outputAddr
				}

				outputAddrToPacketsMap[hex.EncodeToString(outputAddr)] = append(outputAddrToPacketsMap[hex.EncodeToString(outputAddr)], packet)
				//log.Printf("%s, %x\n", packet.Txid, outputAddr)
			}
		}

		for i, outputAddr := range packetQueueToOutputAddrMap {
			packetQueue[i] = outputAddrToPacketsMap[hex.EncodeToString(outputAddr)]
		}

		return nil
	})
	if err != nil {
		return err
	}

	//log.Printf("%+v\n", packetQueue)

	datapacks := make([]Datapack, 0)

	for m := 0; m < len(packetQueue); m++ {
		outputAddr := packetQueue[m][0].OutputAddr
		packets := packetQueue[m]

		usedPackets := make([]int, 0)
		data := make([]byte, 0)
		txids := make([]blockchainparser.Hash256, 0)
		var lastPacket Packet
		var nextChecksum []byte
		nextSequence := int16(0)
		counter := 0
		missingPackets := false

		// find the first packet first; packets[0] not always guaranteed to be the first
		var firstPacket *Packet
		for i := 0; i < len(packets); i++ {
			if packets[i].Sequence == 0 {
				firstPacket = &packets[i]

				// marked the packet as used
				usedPackets = append(usedPackets, i)
				break
			}
		}
		if firstPacket == nil {
			log.Printf("\tWARNING: Cannot find first packet outputAddr %s: %+v!\n", outputAddr, packets[0])
			continue
		}

		// we will use the last packet timestamp as the datapack timestamp; keep track of it
		lastTimestamp := firstPacket.Timestamp

		// assemble packets
		for true {
			found := false
			var p Packet
			if firstPacket != nil {
				p = *firstPacket
				firstPacket = nil

				found = true
			} else {
				for i := 0; i < len(packets); i++ {
					p = packets[i]

					// find the packet which is the next sequence
					if nextSequence != p.Sequence {
						continue
					}

					// verify checksum
					if nextChecksum != nil && !bytes.Equal(p.Checksum[:], nextChecksum) {
						continue
					}

					// mark the packet as used
					usedPackets = append(usedPackets, i)

					found = true
					break
				}
			}

			if found {
				nextChecksum = p.NextChecksum[:]
				nextSequence++

				// append data and txids for merging
				data = append(data, p.Data...)
				txids = append(txids, p.Txid)

				lastTimestamp = p.Timestamp

				lastPacket = p
			} else {
				//log.Println("Not found")
				missingPackets = true
				break
			}

			// Stop if we found the last packet
			if bytes.Equal([]byte{0, 0, 0, 0, 0, 0, 0, 0}, nextChecksum) {
				break
			}

			// Probably redundant
			if counter >= 32767 {
				missingPackets = true
				break
			}

			counter++
		}

		// check if the last packet has the termination signal
		if missingPackets {
			log.Printf("\tWARNING: Missing packets for outputAddr %s. Last packet: %+v!\n", outputAddr, lastPacket)
			continue
		}

		//log.Printf("Merged Packet Data: %s\n", data)
		datapack := Datapack{TxIds: txids, Data: data, Timestamp: lastTimestamp}
		copy(datapack.OutputAddr[:], outputAddr[:])
		datapacks = append(datapacks, datapack)

		// delete used packets
		oldPackets := packets
		packets = make([]Packet, 0)
		for i := 0; i < len(oldPackets); i++ {
			used := false
			for j := 0; j < len(usedPackets); j++ {
				if i == usedPackets[j] {
					used = true
					break
				}
			}

			if !used {
				packets = append(packets, oldPackets[i])
			}
		}

		// delete if no more packets from this outputAddr
		if len(packets) == 0 {
			packetQueue = append(packetQueue[:m], packetQueue[m+1:]...)
			m--
		} else {
			log.Printf("\tWARNING: More packets in outputAddr?: %+v\n", packets)
			m--
		}
	}

	// sort datapack based on their height in chain and transaction index in block
	sort.Slice(datapacks, func(i, j int) bool {
		iTransaction, err := cc.GetTransactionMeta(datapacks[i].TxIds[0])
		if err != nil {
			log.Fatal(err)
		}
		iBlockHash := iTransaction.BlockHash
		jTransaction, err := cc.GetTransactionMeta(datapacks[j].TxIds[0])
		if err != nil {
			log.Fatal(err)
		}
		jBlockHash := jTransaction.BlockHash

		if bytes.Equal(iBlockHash, jBlockHash) {
			return iTransaction.Index < jTransaction.Index
		} else {
			var heightI uint32
			var heightJ uint32
			cc.ChainDb.View(func(tx *bolt.Tx) error {
				bChain := tx.Bucket([]byte("heights"))
				heightIByte := bChain.Get(iBlockHash)
				heightI = binary.LittleEndian.Uint32(heightIByte)
				heightJByte := bChain.Get(jBlockHash)
				heightJ = binary.LittleEndian.Uint32(heightJByte)

				return nil
			})

			return heightI < heightJ
		}
	})

	// save datapacks to db
	err = cc.CarbonDb.Batch(func(tx *bolt.Tx) error {
		bDatas := tx.Bucket([]byte("datas"))

		for _, datapack := range datapacks {
			datapackByte := datapack.Bytes()

			id, _ := bDatas.NextSequence()
			b := make([]byte, 8)
			binary.BigEndian.PutUint64(b, uint64(id))
			err = bDatas.Put(b, datapackByte)
			if err != nil {
				return err
			}
		}

		return nil
	})
	if err != nil {
		return err
	}

	if cc.Options.LogLevel <= LOG_LEVEL_INFO {
		if len(datapacks) > 0 {
			log.Println("Datapacks:")
			for _, datapack := range datapacks {
				log.Printf("\t%s (%s - %+v)", datapack.Data, datapack.OutputAddr, datapack)
			}
		}
	}
	//if len(packetQueue) > 0 {
	//	log.Println("Unable to process:")
	//	for _, packets := range packetQueue {
	//		log.Printf("\t%s: %+v", packets[0].OutputAddr, packets)
	//	}
	//}

	// Save the unable to process packetQueues back to db
	err = cc.CarbonDb.Batch(func(tx *bolt.Tx) error {
		tx.DeleteBucket([]byte("packets"))
		bPackets, err := tx.CreateBucketIfNotExists([]byte("packets"))
		if err != nil {
			return err
		}

		for _, packets := range packetQueue {
			outputAddr := packets[0].OutputAddr[:]

			p := bPackets.Bucket(outputAddr)
			if p == nil {
				id, _ := bPackets.NextSequence()
				bId := make([]byte, 8)
				binary.BigEndian.PutUint64(bId, uint64(id))
				err = bPackets.Put(bId, outputAddr)
				if err != nil {
					return err
				}

				p, err = bPackets.CreateBucket(outputAddr)
				if err != nil {
					return err
				}
			}

			for _, packet := range packets {
				packetByte := packet.DbBytes()

				id, _ := p.NextSequence()
				bId := make([]byte, 8)
				binary.BigEndian.PutUint64(bId, uint64(id))
				err = p.Put(bId, packetByte)
				if err != nil {
					return err
				}
			}
		}

		return nil
	})
	if err != nil {
		return err
	}

	return nil
}
