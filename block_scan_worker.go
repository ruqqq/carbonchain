package carbonchain

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/boltdb/bolt"
	"github.com/lunixbochs/struc"
	"log"
	"time"
)

type BlockScanWorker struct {
	cc            *CarbonChain
	JobChannel    chan BlockScanRequest
	ResultChannel chan BlockScanResult
	quit          chan bool
}

func NewBlockScanWorker(cc *CarbonChain, jobChannel chan BlockScanRequest, resultChannel chan BlockScanResult) *BlockScanWorker {
	return &BlockScanWorker{
		cc:            cc,
		JobChannel:    jobChannel,
		ResultChannel: resultChannel,
		quit:          make(chan bool)}
}

func (w *BlockScanWorker) Stop() {
	go func() {
		w.quit <- true
	}()
}

func (w *BlockScanWorker) Start() {
	go func() {
		for {
			select {
			case <-w.quit:
				return
			case blockScanRequest := <-w.JobChannel:
				index := blockScanRequest.Index
				blockStartPos := blockScanRequest.BlockStartPos
				fileNum := blockScanRequest.FileNum

				//if blockStartPos == 0 {
				//	continue
				//}

				// check if block already scanned
				blockFileScannedBlockPosBucketName := fmt.Sprintf("blockFile%dScannedBlockPos", fileNum)
				isScanned := false

				w.cc.BlockDb.View(func(bTx *bolt.Tx) error {
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
					w.ResultChannel <- BlockScanResult{index}
					continue
				}

				// parse block data
				block, err := w.cc.readBlockFromBlockFile(fileNum, blockStartPos-4)
				if err != nil {
					panic(err)
					w.ResultChannel <- BlockScanResult{index}
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
						w.ResultChannel <- BlockScanResult{index}
						continue
					}

					err = w.cc.BlockDb.Batch(func(bTx *bolt.Tx) error {
						b := bTx.Bucket([]byte("transactions"))
						err = b.Put(tx.Txid(), buf.Bytes())
						return err
					})
					if err != nil {
						log.Fatalln(err)
						w.ResultChannel <- BlockScanResult{index}
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
						if payload[0] != w.cc.Options.PacketId {
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

						if w.cc.Options.LogLevel <= LOG_LEVEL_INFO {
							log.Printf("Received packet in Txid: %v\n", packet.Txid)
						}
						if w.cc.Options.LogLevel <= LOG_LEVEL_VERBOSE {
							log.Printf("\tData (%x, %d): %s\n", outputAddr, packet.Sequence, packet.Data)
							//log.Printf("\tChecksum: %x vs %x\n", packet.Checksum[:], packet.CalculateChecksum())
							//log.Printf("\tOutput Addr: %x (%d)\n", outputAddr, len(outputAddr))
							//log.Printf("\tOutput Script: %x (%d)\n", script, len(script))
						}

						err = w.cc.CarbonDb.Batch(func(tx *bolt.Tx) error {
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
								if w.cc.Options.LogLevel <= LOG_LEVEL_VERBOSE {
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
							w.ResultChannel <- BlockScanResult{index}
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
					w.ResultChannel <- BlockScanResult{index}
					continue
				}

				// update chain
				err = w.cc.ChainDb.Batch(func(tx *bolt.Tx) error {
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
				err = w.cc.BlockDb.Batch(func(bTx *bolt.Tx) error {
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
					w.ResultChannel <- BlockScanResult{index}
					continue
					//return err
				}

				w.ResultChannel <- BlockScanResult{index}
			}
		}
	}()
}
