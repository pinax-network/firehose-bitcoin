package fetch

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/btcsuite/btcd/rpcclient"
	pbbitcoin "github.com/streamingfast/firehose-bitcoin/pb/sf/bitcoin/type/v1"
	"github.com/test-go/testify/require"
)

// Block 170 holds the first bitcoin transfer: a coinbase, and one spend of block 9's coinbase.
func TestGetBlockVerbosity3(t *testing.T) {
	fixture, err := os.ReadFile("testdata/getblock-170-verbosity-3.json")
	require.NoError(t, err)

	var params []json.RawMessage
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID     json.RawMessage   `json:"id"`
			Method string            `json:"method"`
			Params []json.RawMessage `json:"params"`
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
		require.Equal(t, "getblock", req.Method)
		params = req.Params
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"result":%s,"error":null,"id":%s}`, fixture, req.ID)
	}))
	defer srv.Close()

	client, err := rpcclient.New(&rpcclient.ConnConfig{
		Host:         strings.TrimPrefix(srv.URL, "http://"),
		DisableAuth:  true,
		HTTPPostMode: true,
		DisableTLS:   true,
	}, nil)
	require.NoError(t, err)

	hash := "00000000d1145790a8694403d4063f323d499e655c83426834d4ce2f8dd4a2ee"
	rpcBlk, err := getBlock(client, hash, 3)
	require.NoError(t, err)
	require.Equal(t, []json.RawMessage{json.RawMessage(`"` + hash + `"`), json.RawMessage(`3`)}, params)

	blk := rpcBlk.toProto()
	require.Equal(t, hash, blk.Hash)
	require.Equal(t, int64(170), blk.Height)
	require.Equal(t, "000000002a22cfee1f2c846adbd12b3e183d4f97683f85dad08a79780a84bd55", blk.PreviousHash)
	require.Equal(t, int64(1231716245), blk.Mediantime)
	require.Equal(t, "000000000000000000000000000000000000000000000000000000ab00ab00ab", blk.Chainwork)
	require.Equal(t, uint32(2), blk.NTx)
	require.Len(t, blk.Tx, 2)

	coinbase := blk.Tx[0]
	require.Equal(t, "04ffff001d0102", coinbase.Vin[0].Coinbase)
	require.Nil(t, coinbase.Vin[0].Prevout)

	spend := blk.Tx[1]
	require.Equal(t, "f4184fc596403b9d638783cf57adfe4c75c605f6356fbc91338530e9831e9e16", spend.Txid)
	require.Equal(t, hash, spend.Blockhash)
	require.Equal(t, blk.Time, spend.Blocktime)
	prevout := spend.Vin[0].Prevout
	require.NotNil(t, prevout)
	require.True(t, prevout.Generated)
	require.Equal(t, int64(9), prevout.Height)
	require.Equal(t, 50.0, prevout.Value)
	require.Equal(t, "pubkey", prevout.ScriptPubKey.Type)
	require.True(t, strings.HasPrefix(prevout.ScriptPubKey.Desc, "pk("))
	require.Equal(t, 10.0, spend.Vout[0].Value)
	require.Equal(t, 40.0, spend.Vout[1].Value)
	requireFeeMatchesPrevouts(t, blk)
}

// Checks the prevout mapping against a live node: every non-coinbase transaction's inputs, less its
// outputs, must equal the fee bitcoind reports.
func TestGetBlockVerbosity3Live(t *testing.T) {
	rpcEndpoint := os.Getenv("TEST_BTC_RPC_ENDPOINT")
	if rpcEndpoint == "" {
		t.Skip("TEST_BTC_RPC_ENDPOINT not set")
	}

	client, err := rpcclient.New(&rpcclient.ConnConfig{
		Host:         rpcEndpoint,
		DisableAuth:  true,
		HTTPPostMode: true,
		DisableTLS:   true,
	}, nil)
	require.NoError(t, err)

	// Pre-segwit, the first inscription, and the first runes block.
	for _, height := range []int64{200000, 767430, 840000} {
		t.Run(fmt.Sprint(height), func(t *testing.T) {
			hash, err := client.GetBlockHash(height)
			require.NoError(t, err)

			rpcBlk, err := getBlock(client, hash.String(), 3)
			require.NoError(t, err)

			blk := rpcBlk.toProto()
			require.Equal(t, height, blk.Height)
			require.NotEmpty(t, blk.Chainwork)
			require.NotZero(t, blk.Mediantime)
			requireFeeMatchesPrevouts(t, blk)
		})
	}
}

func requireFeeMatchesPrevouts(t *testing.T, blk *pbbitcoin.Block) {
	t.Helper()
	require.NoError(t, CheckFees(blk))
}
