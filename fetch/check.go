package fetch

import (
	"fmt"
	"math"

	"github.com/btcsuite/btcd/rpcclient"
	pbbitcoin "github.com/streamingfast/firehose-bitcoin/pb/sf/bitcoin/type/v1"
)

// FetchBlock fetches the block at height from the node and converts it as the poller does.
func FetchBlock(client *rpcclient.Client, height int64, getblockVerbosity int) (*pbbitcoin.Block, error) {
	hash, err := client.GetBlockHash(height)
	if err != nil {
		return nil, fmt.Errorf("unable to get block hash for block %d: %w", height, err)
	}

	blk, err := getBlock(client, hash.String(), getblockVerbosity)
	if err != nil {
		return nil, err
	}

	return blk.toProto(), nil
}

// CheckFees checks every non-coinbase transaction's prevouts: its inputs, less its outputs, must
// be the fee the node reported.
func CheckFees(blk *pbbitcoin.Block) error {
	for i, tx := range blk.Tx {
		if i == 0 {
			if len(tx.Vin) == 0 || tx.Vin[0].Prevout != nil {
				return fmt.Errorf("coinbase %s input has a prevout", tx.Txid)
			}
			continue
		}

		var in, out int64
		for _, vin := range tx.Vin {
			if vin.Prevout == nil {
				return fmt.Errorf("tx %s input %s:%d has no prevout", tx.Txid, vin.Txid, vin.Vout)
			}
			in += sats(vin.Prevout.Value)
		}
		for _, vout := range tx.Vout {
			out += sats(vout.Value)
		}

		if in-out != sats(tx.Fee) {
			return fmt.Errorf("tx %s: inputs %d - outputs %d = %d sats, fee is %d", tx.Txid, in, out, in-out, sats(tx.Fee))
		}
	}

	return nil
}

func sats(btc float64) int64 {
	return int64(math.Round(btc * 1e8))
}
