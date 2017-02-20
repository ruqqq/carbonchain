package carbonchain

import (
	"encoding/binary"
	"encoding/hex"
	"github.com/ruqqq/blockchainparser"
	"hash/crc64"
)

type OutputAddr [20]byte

func (outputAddr OutputAddr) String() string {
	return hex.EncodeToString(outputAddr[:])
}

type Datapack struct {
	Txids      []blockchainparser.Hash256 `struc:"little"`
	OutputAddr OutputAddr                 `struc:"little,[20]byte"`
	Length     int                        `struc:"little,sizeof=Data"`
	Data       []byte                     `struc:"little"`
	Timestamp  int64                      `struc:"little"`
}

type Packet struct {
	Id           byte                     `struc:"little"`
	Txid         blockchainparser.Hash256 `struc:"little,[32]byte"`
	Flag         int                      `struc:"little"`
	Sequence     int16                    `struc:"little"`
	Checksum     [8]byte                  `struc:"little"`
	NextChecksum [8]byte                  `struc:"little"`
	Length       int                      `struc:"little,sizeof=Data"`
	Data         []byte                   `struc:"little"`
	OutputAddr   OutputAddr               `struc:"little,[20]byte"`
	Timestamp    int64                    `struc:"little"`
}

func NewPacketFromBytes(data []byte) *Packet {
	packet := &Packet{}
	packet.Id = data[0]
	packet.Flag = int(data[1])
	packet.Sequence = int16(binary.LittleEndian.Uint16(data[2:4]))
	copy(packet.Checksum[:], data[4:12])
	copy(packet.NextChecksum[:], data[12:20])
	packet.Data = data[20:]
	return packet
}

func (packet *Packet) CalculateChecksum() []byte {
	crc64Table := crc64.MakeTable(crc64.ECMA)
	crc64Hash := crc64.New(crc64Table)

	checksum := packet.Checksum
	packet.Checksum = [8]byte{0, 0, 0, 0, 0, 0, 0, 0}
	crc64Hash.Write(packet.Bytes())
	packet.Checksum = checksum

	return crc64Hash.Sum(nil)
}

func (packet *Packet) Bytes() []byte {
	// construct seq in bytes
	seq := make([]byte, 2)
	binary.LittleEndian.PutUint16(seq, uint16(packet.Sequence))

	// convert checksum to slice
	checksum := make([]byte, 8)
	copy(checksum, packet.Checksum[:])

	// convert nextChecksum to slice
	nextChecksum := make([]byte, 8)
	copy(nextChecksum, packet.NextChecksum[:])

	bin := make([]byte, 0)
	bin = append(bin, packet.Id)         // identifier (1 byte)
	bin = append(bin, byte(packet.Flag)) // flag (1 byte)
	bin = append(bin, seq...)            // sequence (2 byte)
	bin = append(bin, checksum...)       // checksum (8 byte)
	bin = append(bin, nextChecksum...)   // nextChecksum (8 byte)
	bin = append(bin, packet.Data...)    // data

	return bin
}
