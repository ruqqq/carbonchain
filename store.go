package carbonchain

import (
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/ruqqq/blockchainparser/rpc"
	"math"
	"sort"
)

const BITCOIN_BTC_DUST float32 = 0.00001
const OP_RETURN_MAX_BYTES int = 80
const OP_RETURN_MAX_BLOCKS int = 64

func Store(data []byte, packetId byte, btcFee float64, options *rpc.RpcOptions) ([]string, error) {
	// make sure bitcoind is available
	bitcoindAvail, _ := rpc.Check(options)
	if !bitcoindAvail {
		return nil, errors.New("Can't connect to bitcoind")
	}

	dataLen := len(data)

	// make sure there's data to store
	if dataLen == 0 {
		return nil, errors.New("Cannot store empty data")
	}

	// get change address to send the balance to minus the miner fee
	res, err := rpc.CmdAsSingleResult("getrawchangeaddress", options)
	if err != nil {
		return nil, err
	}
	changeAddress := res.(string)
	fmt.Printf("change address: %s\n", changeAddress)

	// get blockchain height
	res, err = rpc.CmdAsSingleResult("getblockcount", options)
	if err != nil {
		return nil, err
	}
	height := int64(res.(float64))
	fmt.Printf("blockchain height: %d\n", height)

	// get tx ids to avoid spending
	avoid_txids, err := rpc.GetRawMempool(options)
	if err != nil {
		return nil, err
	}
	for _, tx := range avoid_txids {
		fmt.Printf("avoid: %s\n", tx)
	}

	// get unspent inputs, sort by priority
	unspent, err := rpc.ListUnspent(options)
	if err != nil {
		return nil, err
	}
	for i, tx := range unspent {
		unspent[i].Priority = tx.Amount * float32(tx.Confirmations)
	}
	sort.Sort(unspent)
	for _, tx := range unspent {
		fmt.Printf("unspent: %+v\n", tx)
	}

	// split data & create our packets
	payload := make([]Packet, 0)
	totalPayload := int16(math.Ceil(float64(len(data)) / float64(OP_RETURN_MAX_BYTES-14))) // TODO: Error/Warn if reach limits
	fmt.Printf("total payloads: %d\n", totalPayload)
	for i := int16(0); i < totalPayload; i++ {
		length := OP_RETURN_MAX_BYTES - 14
		if len(data) < length {
			length = len(data)
		}

		packet := Packet{Id: packetId, Sequence: i, Data: data[0:length]}
		payload = append(payload, packet)
		data = data[length:]

		fmt.Printf("data %d: %s\n", i, packet.Data)
	}

	// generate our checksums
	copy(payload[totalPayload-1].Checksum[:], payload[totalPayload-1].CalculateChecksum()) // checksum for last item calculated first

	for i := totalPayload - 2; i >= 0; i-- {
		nextPacketChecksum := payload[i+1].CalculateChecksum()
		copy(payload[i].NextChecksum[:], nextPacketChecksum)

		packetChecksum := payload[i].CalculateChecksum()
		copy(payload[i].Checksum[:], packetChecksum)

		//fmt.Printf("next packet to checksum: %x\n", payload[i+1].Bytes())
		//fmt.Printf("next packet checksum: %x\n", nextPacketChecksum)
	}

	// debug payloads
	for i := int16(0); i < totalPayload; i++ {
		fmt.Printf("packet %d: %+v (len: %d)\n", i, payload[i], len(payload[i].Bytes()))
	}

	// generate inputs
	totalAmount := float32(btcFee) * float32(len(payload))
	inputAmount := float32(0)
	inputs := make(rpc.UnspentTxs, 0)

	for _, tx := range unspent {
		inputs = append(inputs, tx)
		inputAmount += tx.Amount

		if inputAmount >= totalAmount {
			break
		}
	}

	if inputAmount < totalAmount {
		return nil, errors.New("Not enough balance in wallet for fee")
	}

	// create our transactions
	prevTxns := make(rpc.UnspentTxs, 0)
	sendTxids := make([]string, 0)

	for _, item := range payload {
		fmt.Printf("unspent (current): %+v\n", inputs[0])
		// calculate change amount:
		// 	[total amount of inputs we are using] - [no. of transactions x fee]
		changeAmount := inputAmount - float32(btcFee)
		fmt.Printf("change amount: %f\n", changeAmount)

		// prepare to create tx
		outputs := make(map[string]float32)
		if changeAmount >= BITCOIN_BTC_DUST {
			outputs[changeAddress] = changeAmount
		}

		// create raw tx
		fmt.Printf("inputs: %+v\n", inputs)
		fmt.Printf("outputs: %+v\n", outputs)
		rawTxn, err := rpc.CreateRawTransaction(inputs, outputs, options)
		if err != nil {
			return nil, err
		}
		fmt.Printf("rawTxn: %+v\n", rawTxn)

		// insert OP_RETURN with payload to tx
		appendOPRETURNToTx(&rawTxn, item.Bytes())

		// sign the tx
		signedTx, err := rpc.SignRawTransaction(rawTxn, prevTxns, options)
		if err != nil {
			return nil, err
		}
		fmt.Printf("signedTx: %+v\n", signedTx)

		// send the tx to the network
		sendTxid, err := rpc.SendRawTransaction(signedTx.Hex, options)
		if err != nil {
			return nil, err
		}
		fmt.Printf("txid: %s\n", sendTxid)
		sendTxids = append(sendTxids, sendTxid)

		// prepare inputs for next
		prevTxns = make(rpc.UnspentTxs, 1)
		prevTxns[0] = rpc.UnspentTx{Txid: sendTxid, Vout: 0, ScriptPubKey: rawTxn.Vout[0].ScriptPubKey}
		inputs[0] = rpc.UnspentTx{Txid: sendTxid, Vout: 0}

		inputAmount = changeAmount
	}

	return sendTxids, nil
}

func appendOPRETURNToTx(txn *rpc.RawTxn, data []byte) {
	// insert our data
	payload := make([]byte, 1)
	payload[0] = byte(0x6a)
	dataLen := len(data)

	if dataLen <= 75 {
		// length byte + data (https://en.bitcoin.it/wiki/Script)
		payload = append(payload, byte(dataLen))
		payload = append(payload, data...)
	} else if dataLen <= 256 {
		// OP_PUSHDATA1 format
		payload = append(payload, 0x4c)
		payload = append(payload, byte(dataLen))
		payload = append(payload, data...)
	} else {
		// OP_PUSHDATA2 format
		payload = append(payload, 0x4d)
		payload = append(payload, byte(dataLen%256))
		payload = append(payload, byte(int32(math.Floor(float64(dataLen)/256))))
		payload = append(payload, data...)
	}

	payloadHex := hex.EncodeToString(payload)
	txn.Vout = append(txn.Vout, rpc.RawTxOut{0, payloadHex})
}
