package main

// NOTE: THIS FILE IS DEPRECATED

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"github.com/boltdb/bolt"
	"github.com/lunixbochs/struc"
	"github.com/ruqqq/blockchainparser"
	"github.com/ruqqq/carbonchain"
	"runtime"
	"sync"
	"time"
)

var GENESIS_BLOCK string = "000000000933ea01ad0ee984209779baaec3ced90fa3f408719526f8d77f4943"

func rebuild_chain(db *bolt.DB, dbChain *bolt.DB) error {
	err := dbChain.Update(func(tx *bolt.Tx) error {
		// delete existing chain data
		err := tx.DeleteBucket([]byte("chain"))
		if err != nil {
			return err
		}
		err = tx.DeleteBucket([]byte("heights"))
		if err != nil {
			return err
		}

		// recreate the chain + heights buckets
		_, err = tx.CreateBucketIfNotExists([]byte("heights"))
		if err != nil {
			return err
		}
		_, err = tx.CreateBucketIfNotExists([]byte("chain"))
		if err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		return err
	}

	forks := make(map[string][][]byte)
	forkMutex := sync.Mutex{}
	iteratedBlocks := 0
	var totalBlocks int

	db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("blocks"))
		totalBlocks = b.Stats().KeyN
		return nil
	})

	fmt.Printf("Total Blocks: %d\n", totalBlocks)
	fmt.Printf("Rebuilding...\n")
	startTime := time.Now()

	numWorkers := runtime.GOMAXPROCS(-1) * 50
	successChan := make(chan bool, totalBlocks)
	hashValChan := make(chan map[string][]byte, totalBlocks)
	doneChan := make(chan bool, numWorkers)
	go func() {
		db.View(func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte("blocks"))
			// iterate through our blocks (not in any order)
			c := b.Cursor()
			for hash, v := c.First(); hash != nil; hash, v = c.Next() {
				hashValChan <- map[string][]byte{
					"h": hash,
					"v": v,
				}
			}

			return nil
		})
	}()

	worker := func() {
		for true {
			select {
			case <-doneChan:
				return
			case hashVal := <-hashValChan:
				hash := hashVal["h"]
				v := hashVal["v"]

				buf := bytes.NewBuffer(v)
				blockMeta := &carbonchain.BlockMeta{}
				struc.Unpack(buf, blockMeta)
				//fmt.Printf("key=%x, value=%v\n", genesisHash, blockMeta)

				err = dbChain.Batch(func(tx *bolt.Tx) error {
					bChain := tx.Bucket([]byte("chain"))

					v := bChain.Get(blockMeta.HashPrev)
					if v == nil {
						// if not a competing block (chain), do a normal insert
						err = bChain.Put(blockMeta.HashPrev, hash)
						if err != nil {
							return err
						}
					} else {
						// else ...
						hashNext := make([]byte, len(v))
						copy(hashNext, v)

						//fmt.Printf("\nDetected conflict: %x\n", blockchainparser.ReverseHex(hashNext))
						if len(hashNext) == 1 {
							forkMutex.Lock()
							forks[hex.EncodeToString(blockMeta.HashPrev)] = append(forks[hex.EncodeToString(blockMeta.HashPrev)], hash)
							forkMutex.Unlock()
						} else {
							forkMutex.Lock()
							forks[hex.EncodeToString(blockMeta.HashPrev)] = make([][]byte, 2)
							forks[hex.EncodeToString(blockMeta.HashPrev)][0] = hashNext
							forks[hex.EncodeToString(blockMeta.HashPrev)][1] = hash
							forkMutex.Unlock()

							err = bChain.Put(blockMeta.HashPrev, []byte{0})
							if err != nil {
								return err
							}
						}
					}

					return nil
				})
				if err != nil {
					panic(err)
					successChan <- false
				} else {
					successChan <- true
				}
			}
		}
	}

	for w := 0; w < numWorkers; w++ {
		go worker()
	}

	for i := 0; i < totalBlocks; i++ {
		<-successChan
		iteratedBlocks++
		fmt.Printf("\rProgress: %.2f%%", float64(iteratedBlocks)*100.0/float64(totalBlocks))
	}
	fmt.Println("")

	for w := 0; w < numWorkers; w++ {
		doneChan <- true
	}

	err = dbChain.Sync()
	if err != nil {
		return err
	}

	fmt.Printf("Chain trace (almost) completed. Took %d secs.\n", time.Now().Unix()-startTime.Unix())
	fmt.Printf("Forks detected: %d\n", len(forks))
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
				dbChain.View(func(tx *bolt.Tx) error {
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

			fmt.Printf("%x -> %x\n", blockchainparser.ReverseHex(hashPrev), blockchainparser.ReverseHex(nextHash))
		}

		winningHash := fork[0]
		winningHeight := 0
		for range fork {
			chainHeight := <-chainHeights
			if chainHeight.Height > winningHeight {
				winningHeight = chainHeight.Height
				winningHash = chainHeight.Hash
			}
		}

		err = dbChain.Batch(func(tx *bolt.Tx) error {
			bChain := tx.Bucket([]byte("chain"))
			err = bChain.Put(hashPrev, winningHash)
			return err
		})
		if err != nil {
			return err
		}

		fmt.Printf("Mainchain: %x -> %x\n", blockchainparser.ReverseHex(hashPrev), blockchainparser.ReverseHex(winningHash))
	}

	err = dbChain.Sync()
	if err != nil {
		return err
	}

	dbChain.View(func(tx *bolt.Tx) error {
		bChain := tx.Bucket([]byte("chain"))
		genesisHex, _ := hex.DecodeString(GENESIS_BLOCK)
		fmt.Printf("Genesis Chain: %x\n", bChain.Get(blockchainparser.ReverseHex(genesisHex)))
		return nil
	})

	startTime = time.Now()
	fmt.Printf("Calculating heights...\n")
	genesisHex, _ := hex.DecodeString(GENESIS_BLOCK)
	genesisHash := blockchainparser.ReverseHex(genesisHex)
	err = dbChain.Batch(func(tx *bolt.Tx) error {
		bChain := tx.Bucket([]byte("chain"))
		bHeights := tx.Bucket([]byte("heights"))

		height := 0
		nextHash := genesisHash
		for nextHash != nil {
			heightByte := make([]byte, 4)
			binary.LittleEndian.PutUint32(heightByte, uint32(height))
			err = bHeights.Put(nextHash, heightByte)
			if err != nil {
				return err
			}
			err = bHeights.Put(heightByte, nextHash)
			if err != nil {
				return err
			}

			nextHash = bChain.Get(nextHash)
			height++
			fmt.Printf("\rProgress: %.2f%%", float64(height)*100.0/float64(totalBlocks))
		}

		fmt.Println()

		heightByte := make([]byte, 4)
		binary.LittleEndian.PutUint32(heightByte, uint32(height))
		err = bHeights.Put([]byte("maxHeight"), heightByte)
		if err != nil {
			return err
		}

		fmt.Printf("maxHeight %d\n", height)

		return nil
	})
	if err != nil {
		return err
	}

	err = dbChain.Sync()
	if err != nil {
		return err
	}

	fmt.Printf("Height calculation completed. Took %d secs.\n", time.Now().Unix()-startTime.Unix())

	return nil
}
