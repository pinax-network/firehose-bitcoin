package fetch

import (
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/btcsuite/btcd/rpcclient"
	pbbitcoin "github.com/streamingfast/firehose-bitcoin/pb/sf/bitcoin/type/v1"
)

// The getblock RPC response, decoded directly rather than through btcjson: btcjson stops at
// verbosity 2 and predates the `address` field, so it can't carry `prevout`, `fee`, `mediantime` or
// `chainwork`.

type rpcBlock struct {
	Hash              string  `json:"hash"`
	Height            int64   `json:"height"`
	Version           int32   `json:"version"`
	VersionHex        string  `json:"versionHex"`
	MerkleRoot        string  `json:"merkleroot"`
	Time              int64   `json:"time"`
	MedianTime        int64   `json:"mediantime"`
	Nonce             uint32  `json:"nonce"`
	Bits              string  `json:"bits"`
	Difficulty        float64 `json:"difficulty"`
	Chainwork         string  `json:"chainwork"`
	PreviousBlockHash string  `json:"previousblockhash"`
	StrippedSize      int32   `json:"strippedsize"`
	Size              int32   `json:"size"`
	Weight            int32   `json:"weight"`
	Tx                []rpcTx `json:"tx"`
}

type rpcTx struct {
	Hex      string    `json:"hex"`
	Txid     string    `json:"txid"`
	Hash     string    `json:"hash"`
	Size     int32     `json:"size"`
	Vsize    int32     `json:"vsize"`
	Weight   int32     `json:"weight"`
	Version  uint32    `json:"version"`
	LockTime uint32    `json:"locktime"`
	Vin      []rpcVin  `json:"vin"`
	Vout     []rpcVout `json:"vout"`
	Fee      float64   `json:"fee"`
}

type rpcVin struct {
	Txid      string        `json:"txid"`
	Vout      uint32        `json:"vout"`
	ScriptSig *rpcScriptSig `json:"scriptSig"`
	Sequence  uint32        `json:"sequence"`
	Witness   []string      `json:"txinwitness"`
	Coinbase  string        `json:"coinbase"`
	Prevout   *rpcPrevout   `json:"prevout"`
}

type rpcPrevout struct {
	Generated    bool            `json:"generated"`
	Height       int64           `json:"height"`
	Value        float64         `json:"value"`
	ScriptPubKey rpcScriptPubKey `json:"scriptPubKey"`
}

type rpcVout struct {
	Value        float64         `json:"value"`
	N            uint32          `json:"n"`
	ScriptPubKey rpcScriptPubKey `json:"scriptPubKey"`
}

type rpcScriptSig struct {
	Asm string `json:"asm"`
	Hex string `json:"hex"`
}

type rpcScriptPubKey struct {
	Asm       string   `json:"asm"`
	Desc      string   `json:"desc"`
	Hex       string   `json:"hex"`
	ReqSigs   int32    `json:"reqSigs"`
	Type      string   `json:"type"`
	Address   string   `json:"address"`
	Addresses []string `json:"addresses"`
}

// getBlock fetches a block with getblock at the given verbosity (2, or 3 to include `prevout`).
func getBlock(client *rpcclient.Client, hash string, verbosity int) (*rpcBlock, error) {
	params := []json.RawMessage{
		json.RawMessage(strconv.Quote(hash)),
		json.RawMessage(strconv.Itoa(verbosity)),
	}

	raw, err := client.RawRequest("getblock", params)
	if err != nil {
		return nil, fmt.Errorf("unable to get block %s: %w", hash, err)
	}

	blk := &rpcBlock{}
	if err := json.Unmarshal(raw, blk); err != nil {
		return nil, fmt.Errorf("unable to decode block %s: %w", hash, err)
	}

	return blk, nil
}

func (b *rpcBlock) toProto() *pbbitcoin.Block {
	blk := &pbbitcoin.Block{
		Hash:         b.Hash,
		Size:         b.Size,
		StrippedSize: b.StrippedSize,
		Weight:       b.Weight,
		Height:       b.Height,
		Version:      b.Version,
		VersionHex:   b.VersionHex,
		MerkleRoot:   b.MerkleRoot,
		Time:         b.Time,
		Mediantime:   b.MedianTime,
		Nonce:        b.Nonce,
		Bits:         b.Bits,
		Difficulty:   b.Difficulty,
		Chainwork:    b.Chainwork,
		NTx:          uint32(len(b.Tx)),
		PreviousHash: b.PreviousBlockHash,
	}

	for _, tx := range b.Tx {
		trx := &pbbitcoin.Transaction{
			Hex:      tx.Hex,
			Txid:     tx.Txid,
			Hash:     tx.Hash,
			Size:     tx.Size,
			Vsize:    tx.Vsize,
			Weight:   tx.Weight,
			Version:  tx.Version,
			Locktime: tx.LockTime,
			// getblock leaves these out of its transactions, they are the block's
			Blockhash: b.Hash,
			Blocktime: b.Time,
			Fee:       tx.Fee,
		}

		for _, vin := range tx.Vin {
			pbvin := &pbbitcoin.Vin{
				Txid:        vin.Txid,
				Vout:        vin.Vout,
				Sequence:    vin.Sequence,
				Txinwitness: vin.Witness,
				Coinbase:    vin.Coinbase,
			}

			if vin.ScriptSig != nil {
				pbvin.ScriptSig = &pbbitcoin.ScriptSig{
					Asm: vin.ScriptSig.Asm,
					Hex: vin.ScriptSig.Hex,
				}
			}

			if vin.Prevout != nil {
				pbvin.Prevout = &pbbitcoin.Prevout{
					Generated:    vin.Prevout.Generated,
					Height:       vin.Prevout.Height,
					Value:        vin.Prevout.Value,
					ScriptPubKey: vin.Prevout.ScriptPubKey.toProto(),
				}
			}

			trx.Vin = append(trx.Vin, pbvin)
		}

		for _, vout := range tx.Vout {
			trx.Vout = append(trx.Vout, &pbbitcoin.Vout{
				Value:        vout.Value,
				N:            vout.N,
				ScriptPubKey: vout.ScriptPubKey.toProto(),
			})
		}
		blk.Tx = append(blk.Tx, trx)
	}

	return blk
}

func (s rpcScriptPubKey) toProto() *pbbitcoin.ScriptPubKey {
	return &pbbitcoin.ScriptPubKey{
		Asm:       s.Asm,
		Hex:       s.Hex,
		ReqSigs:   s.ReqSigs,
		Type:      s.Type,
		Address:   s.Address,
		Addresses: s.Addresses,
		Desc:      s.Desc,
	}
}
