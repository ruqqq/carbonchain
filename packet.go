package carbonchain

import (
	"encoding/binary"
	"encoding/hex"
	"github.com/ruqqq/blockchainparser"
	"hash/crc64"
)

const PACKET_HEADER_SIZE int = 20

type OutputAddr [20]byte

func (outputAddr OutputAddr) String() string {
	return hex.EncodeToString(outputAddr[:])
}

type PacketChecksum [8]byte

func (packetChecksum PacketChecksum) String() string {
	return hex.EncodeToString(packetChecksum[:])
}

type Packet struct {
	Id           byte
	Version      int8
	Sequence     int16
	Checksum     PacketChecksum
	NextChecksum PacketChecksum
	Data         []byte

	// fields below only exists in db, not on blockchain
	Txid       blockchainparser.Hash256 // txId this packet was extracted from
	OutputAddr OutputAddr               // outputAddr this packet belongs to
	Timestamp  int64                    // time this packet is inserted into db
}

func NewPacketFromBytes(data []byte) *Packet {
	packet := &Packet{}
	packet.Id = data[0]
	packet.Version = int8(data[1])
	packet.Sequence = int16(binary.LittleEndian.Uint16(data[2:4]))
	copy(packet.Checksum[:], data[4:12])
	copy(packet.NextChecksum[:], data[12:20])
	packetData := data[20:]
	packet.Data = make([]byte, len(packetData))
	copy(packet.Data, packetData)
	return packet
}

func NewPacketFromDbBytes(data []byte) *Packet {
	packet := NewPacketFromBytes(data)
	packet.Data = packet.Data[:len(packet.Data)-60]
	txIdByte := data[len(data)-60 : len(data)-60+32]
	packet.Txid = make([]byte, 32)
	copy(packet.Txid, txIdByte)
	outputAddrByte := data[len(data)-28 : len(data)-28+20]
	copy(packet.OutputAddr[:], outputAddrByte)
	timestampByte := data[len(data)-8:]
	packet.Timestamp = int64(binary.LittleEndian.Uint64(timestampByte))
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
	bin = append(bin, packet.Id)            // identifier (1 byte)
	bin = append(bin, byte(packet.Version)) // flag (1 byte)
	bin = append(bin, seq...)               // sequence (2 byte)
	bin = append(bin, checksum...)          // checksum (8 byte)
	bin = append(bin, nextChecksum...)      // nextChecksum (8 byte)
	bin = append(bin, packet.Data...)       // data

	return bin
}

func (packet *Packet) DbBytes() []byte {
	bin := packet.Bytes()

	txId := make([]byte, 32)
	copy(txId, packet.Txid[:])
	bin = append(bin, txId...)

	outputAddr := make([]byte, 20)
	copy(outputAddr, packet.OutputAddr[:])
	bin = append(bin, outputAddr...)

	timestamp := make([]byte, 8)
	binary.LittleEndian.PutUint64(timestamp, uint64(packet.Timestamp))
	bin = append(bin, timestamp...)

	return bin
}

type Datapack struct {
	TxIds      []blockchainparser.Hash256
	Version    int8
	OutputAddr OutputAddr
	Data       []byte
	Timestamp  int64
}

func NewDatapackFromBytes(data []byte) *Datapack {
	datapack := &Datapack{}
	txIdsLength := int(binary.LittleEndian.Uint32(data[0:4]))

	pos := 4
	for i := 0; i < txIdsLength; i++ {
		hash := make([]byte, 32)
		copy(hash[:], data[pos:pos+32])
		datapack.TxIds = append(datapack.TxIds, hash)
		pos += 32
	}

	datapack.Version = int8(data[pos])
	pos += 1

	copy(datapack.OutputAddr[:], data[pos:pos+20])
	pos += 20

	length := int(binary.LittleEndian.Uint32(data[pos : pos+4]))
	pos += 4

	datapack.Data = make([]byte, length)
	copy(datapack.Data, data[pos:pos+length])
	pos += length

	timestampByte := data[pos : pos+8]
	datapack.Timestamp = int64(binary.LittleEndian.Uint64(timestampByte))

	return datapack
}

func (datapack *Datapack) Bytes() []byte {
	// construct txIdsLength in bytes
	txIdsLength := make([]byte, 4)
	binary.LittleEndian.PutUint32(txIdsLength, uint32(len(datapack.TxIds)))

	// prepare txIds
	txIds := make([]byte, 32*len(datapack.TxIds))
	for i := 0; i < len(datapack.TxIds); i++ {
		copy(txIds[i*32:i*32+32], datapack.TxIds[i])
	}

	// convert outputAddr to slice
	outputAddr := make([]byte, 20)
	copy(outputAddr, datapack.OutputAddr[:])

	// construct length in bytes
	length := make([]byte, 4)
	binary.LittleEndian.PutUint32(length, uint32(len(datapack.Data)))

	data := make([]byte, len(datapack.Data))
	copy(data, datapack.Data)

	// construct timestamp in bytes
	timestamp := make([]byte, 8)
	binary.LittleEndian.PutUint64(timestamp, uint64(datapack.Timestamp))

	bin := make([]byte, 0)
	bin = append(bin, txIdsLength...)
	bin = append(bin, txIds...)
	bin = append(bin, byte(datapack.Version))
	bin = append(bin, outputAddr...)
	bin = append(bin, length...)
	bin = append(bin, data...)
	bin = append(bin, timestamp...)

	return bin
}
