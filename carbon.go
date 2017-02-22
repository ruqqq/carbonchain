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
	"path/filepath"
	"runtime"
	"strconv"
	"time"
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
		genesisHex, _ := hex.DecodeString("000000000933ea01ad0ee984209779baaec3ced90fa3f408719526f8d77f4943")
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
	log.Printf("Last block file number in data dir: %d\n", lastBlockFileNum)
	log.Printf("Last block file number scanned: %d\n", cc.curBlockFileNum)

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
		go cc.Options.ProcessFunc(cc, cc.CarbonDb)
	}

	log.Println("----------END INIT----------")

	return err
}

func (cc *CarbonChain) Watch() error {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Fatal(err)
	}
	defer watcher.Close()

	done := make(chan bool)
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
						cc.processBlocksForFileNum(cc.curBlockFileNum, cc.curBlockFilePos)
						log.Println("--------END PROCESSING-------")

						err = cc.processPacketQueue()
						if err != nil {
							log.Fatal(err)
							return
						}

						if cc.Options.ProcessFunc != nil {
							go cc.Options.ProcessFunc(cc, cc.CarbonDb)
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

		confirmations = int(maxHeight - height)

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
	log.Printf("Length: %d\n", length)

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
	log.Printf("%d blocks found. Took: %d sec\n", len(blockPos), time.Now().Unix()-startTime.Unix())

	startTime = time.Now()

	// Scan every block in the file that we have found
	blockScanRequestChan := make(chan BlockScanRequest, len(blockPos))
	blockScanResultChan := make(chan BlockScanResult, len(blockPos))

	numWorkers := runtime.GOMAXPROCS(-1) * 50 // e.g. 16 PROCS * 50 = 400 workers
	doneChan := make(chan bool, numWorkers)

	log.Println("Parsing...")

	go func() {
		for index, blockStartPos := range blockPos {
			blockScanRequestChan <- BlockScanRequest{index, blockStartPos, blockFile.FileNum}
		}
	}()

	for w := 0; w < numWorkers; w++ {
		go cc.blockScanWorker(blockScanRequestChan, blockScanResultChan, doneChan)
	}

	for completed := range blockPos {
		result := <-blockScanResultChan

		fmt.Printf("\rProgress: %.2f%% (index: %d)", float64(completed)*100.0/float64(len(blockPos)), result.Index)
	}
	fmt.Println("")
	log.Println("")

	log.Printf("%d blocks scanned. Took: %d sec\n", len(blockPos), time.Now().Unix()-startTime.Unix())
	for w := 0; w < numWorkers; w++ {
		doneChan <- true
	}

	close(blockScanRequestChan)
	close(blockScanResultChan)
	close(doneChan)

	forks := make(map[string][][]byte)

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

			forks[hex.EncodeToString(hashPrev)] = append(forks[hex.EncodeToString(hashPrev)], hash)
		}

		log.Printf("Forks detected: %d\n", len(forks))
		log.Println("Will attempt to fix...")

		return nil
	})

	// Fix forks
	for prev, fork := range forks {
		hashPrev, _ := hex.DecodeString(prev)

		chainHeights := make(chan struct {
			Hash   []byte
			Height int
		}, len(fork))
		for _, nextHash := range fork {
			go func(nextHash []byte) {
				height := 0
				hash := nextHash

				cc.ChainDb.View(func(tx *bolt.Tx) error {
					bChain := tx.Bucket([]byte("chain"))

					for hash != nil {
						hash = bChain.Get(hash)
						height++
					}

					return nil
				})

				chainHeights <- struct {
					Hash   []byte
					Height int
				}{nextHash, height - 1}
			}(nextHash)

			log.Printf("%x -> %x\n", blockchainparser.ReverseHex(hashPrev), blockchainparser.ReverseHex(nextHash))
		}

		winningHash := fork[0]
		winningHeight := 0
		resolved := false
		for range fork {
			chainHeight := <-chainHeights
			if chainHeight.Height > winningHeight {
				winningHeight = chainHeight.Height
				winningHash = chainHeight.Hash
				resolved = true
			}
		}

		//chain[hex.EncodeToString(hashPrev)] = winningHash
		if resolved {
			err = cc.ChainDb.Update(func(tx *bolt.Tx) error {
				bChain := tx.Bucket([]byte("chain"))
				bFork := tx.Bucket([]byte("forks"))

				err = bChain.Put(hashPrev, winningHash)
				if err != nil {
					return err
				}

				for _, h := range fork {
					err = bFork.Delete(h)
					if err != nil {
						return err
					}
				}

				return nil
			})
			if err != nil {
				return 0, err
			}

			fmt.Printf("Mainchain: %x -> %x\n", blockchainparser.ReverseHex(hashPrev), blockchainparser.ReverseHex(winningHash))
		}
	}

	// Update heights
	startTime = time.Now()
	err = cc.ChainDb.Update(func(tx *bolt.Tx) error {
		bChain := tx.Bucket([]byte("chain"))
		bHeights := tx.Bucket([]byte("heights"))
		height := uint32(0)

		// find first in chain for this file
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
		log.Println("Calculating heights...")
		log.Printf("Start from block: %x, Current height: %d\n", blockchainparser.ReverseHex(firstBlockHash), height)
		fmt.Println("")

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
			fmt.Printf("\rProgress: %.2f%% - %d: %x", float64(completed)*100.0/float64(len(blockPos)), height, blockchainparser.ReverseHex(hash))
		}

		maxHeightByte = make([]byte, 4)
		binary.LittleEndian.PutUint32(maxHeightByte, height)
		err = bHeights.Put([]byte("maxHeight"), maxHeightByte)
		if err != nil {
			return err
		}

		fmt.Println("")
		log.Println("")
		if oldHeight != height {
			log.Printf("New Height: %d - %x\n", height, blockchainparser.ReverseHex(lastHash))
		}

		return nil
	})
	if err != nil {
		return 0, err
	}

	log.Printf("Chain + Height updated. Took: %d sec\n", time.Now().Unix()-startTime.Unix())

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

func (cc *CarbonChain) blockScanWorker(blockScanRequestChan chan BlockScanRequest, blockScanResultChan chan BlockScanResult, doneChan chan bool) {
	for true {
		select {
		case <-doneChan:
			return
		case blockScanRequest := <-blockScanRequestChan:
			index := blockScanRequest.Index
			blockStartPos := blockScanRequest.BlockStartPos
			fileNum := blockScanRequest.FileNum

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
			for _, tx := range block.Transactions {
				// save transaction pos in db
				var buf bytes.Buffer
				transactionMeta := &TransactionMeta{FileNum: fileNum, BlockHash: blockHash, Pos: tx.StartPos}
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
					log.Printf("Received packet in Txid: %v\n", packet.Txid)
					log.Printf("\tData (%x, %d): %s\n", outputAddr, packet.Sequence, packet.Data)
					//log.Printf("\tChecksum: %x vs %x\n", packet.Checksum[:], packet.CalculateChecksum())
					//log.Printf("\tOutput Addr: %x (%d)\n", outputAddr, len(outputAddr))
					//log.Printf("\tOutput Script: %x (%d)\n", script, len(script))

					err = cc.CarbonDb.Batch(func(tx *bolt.Tx) error {
						bPackets := tx.Bucket([]byte("packets"))
						id, _ := bPackets.NextSequence()
						bId := make([]byte, 8)
						binary.BigEndian.PutUint64(bId, uint64(id))
						err = bPackets.Put(bId, outputAddr)
						if err != nil {
							return err
						}
						log.Printf("\tCreated bucket: %x\n", outputAddr)

						p, err := bPackets.CreateBucketIfNotExists(outputAddr)
						if err != nil {
							return err
						}

						var outBuf bytes.Buffer
						err = struc.Pack(&outBuf, packet)
						if err != nil {
							return err
						}
						id, _ = p.NextSequence()
						bId = make([]byte, 8)
						binary.BigEndian.PutUint64(bId, uint64(id))
						err = p.Put(bId, outBuf.Bytes())
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
}

func (cc *CarbonChain) processPacketQueue() error {
	packetQueue := make(map[string][]Packet)

	err := cc.CarbonDb.View(func(tx *bolt.Tx) error {
		bPackets := tx.Bucket([]byte("packets"))

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
				buf := bytes.NewBuffer(packetByte)
				packet := Packet{}
				err := struc.Unpack(buf, &packet)
				if err != nil {
					return err
				}
				//log.Printf("%d: %+v\n", binary.BigEndian.Uint64(id2), packet)

				//if bytes.Equal([]byte{0, 0, 0, 0, 0, 0, 0, 0}, packet.Checksum[:]) {
				//	continue
				//}

				// ignore packets added more than 3 days ago
				if time.Unix(packet.Timestamp, 0).Before(time.Now().AddDate(0, 0, -3)) {
					log.Printf("\tIgnoring %d: %+v (more than 3 days old!)\n", binary.BigEndian.Uint64(id2), packet)
					continue
				}

				if packetQueue[hex.EncodeToString(outputAddr)] == nil {
					packetQueue[hex.EncodeToString(outputAddr)] = make([]Packet, 0)
				}

				packetQueue[hex.EncodeToString(outputAddr)] = append(packetQueue[hex.EncodeToString(outputAddr)], packet)
			}
		}

		return nil
	})
	if err != nil {
		return err
	}

	//log.Printf("%+v\n", packetQueue)

	keys := []string{}
	for key, _ := range packetQueue {
		keys = append(keys, key)
	}

	datapacks := make([]Datapack, 0)

	for m := 0; m < len(keys); m++ {
		outputAddrStr := keys[m]
		outputAddr, _ := hex.DecodeString(outputAddrStr)
		packets := packetQueue[outputAddrStr]

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
			log.Printf("\tWARNING: Cannot find first packet outputAddr %x: %+v!\n", outputAddr, packets[0])
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
			log.Printf("\tWARNING: Missing packets for outputAddr %x. Last packet: %+v!\n", outputAddr, lastPacket)
			continue
		}

		//log.Printf("Merged Packet Data: %s\n", data)
		datapack := Datapack{Txids: txids, Data: data, Timestamp: lastTimestamp}
		copy(datapack.OutputAddr[:], outputAddr)
		datapacks = append(datapacks, datapack)
		err = cc.CarbonDb.Batch(func(tx *bolt.Tx) error {
			bDatas := tx.Bucket([]byte("datas"))

			var buf bytes.Buffer
			err := struc.Pack(&buf, datapack)
			if err != nil {
				return err
			}

			id, _ := bDatas.NextSequence()
			b := make([]byte, 8)
			binary.BigEndian.PutUint64(b, uint64(id))
			err = bDatas.Put(b, buf.Bytes())
			if err != nil {
				return err
			}

			return nil
		})
		if err != nil {
			return err
		}

		// delete used packets
		for i := 0; i < len(usedPackets); i++ {
			packets[i].Id = 0
		}
		for i := 0; i < len(packets); i++ {
			if packets[i].Id == 0 {
				packets = append(packets[:i], packets[i+1:]...)
				i--
			}
		}

		// delete if no more packets from this outputAddr
		if len(packets) == 0 {
			delete(packetQueue, outputAddrStr)
		} else {
			m--
		}
	}

	if len(datapacks) > 0 {
		log.Println("Datapacks:")
		for _, datapack := range datapacks {
			log.Printf("%s (%x - %+v)", datapack.Data, datapack.OutputAddr, datapack)
		}
	}
	if len(packetQueue) > 0 {
		log.Println("Unable to process:")
		for outputAddr, packets := range packetQueue {
			log.Printf("%x: %+v", outputAddr, packets)
		}
	}

	// Save the unable to process packetQueues back to db
	err = cc.CarbonDb.Batch(func(tx *bolt.Tx) error {
		tx.DeleteBucket([]byte("packets"))
		bPackets, err := tx.CreateBucketIfNotExists([]byte("packets"))
		if err != nil {
			return err
		}

		for outputAddrStr, packets := range packetQueue {
			outputAddr, _ := hex.DecodeString(outputAddrStr)

			id, _ := bPackets.NextSequence()
			bId := make([]byte, 8)
			binary.BigEndian.PutUint64(bId, uint64(id))
			err = bPackets.Put(bId, outputAddr)
			if err != nil {
				return err
			}

			p, err := bPackets.CreateBucketIfNotExists(outputAddr)
			if err != nil {
				return err
			}

			for _, packet := range packets {
				var buf bytes.Buffer
				err := struc.Pack(&buf, &packet)
				if err != nil {
					return err
				}

				id, _ := p.NextSequence()
				bId := make([]byte, 8)
				binary.BigEndian.PutUint64(bId, uint64(id))
				err = p.Put(bId, buf.Bytes())
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
