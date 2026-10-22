package fetch

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/btcsuite/btcd/rpcclient"
	"github.com/streamingfast/bstream"
	"github.com/streamingfast/firehose-core/rpc"
	"github.com/test-go/testify/require"
)

func TestNew(t *testing.T) {
	rpcEndpoint := os.Getenv("TEST_BTC_RPC_ENDPOINT")
	if rpcEndpoint == "" {
		t.Skip("TEST_BTC_RPC_ENDPOINT not set")
	}
	connCfg := &rpcclient.ConnConfig{
		Host:         rpcEndpoint,
		DisableAuth:  true,
		HTTPPostMode: true,
		DisableTLS:   true,
	}

	client, err := rpcclient.New(connCfg, nil)
	require.NoError(t, err)

	r := &BlockFetcher{
		rpcClient: client,
		logger:    zap.NewNop(),
	}

	headBlock, err := r.GetHeadBlock()
	require.NoError(t, err)
	fmt.Println("Head Block", headBlock.String())

	libBlock, err := r.GetFinalizedBlock(headBlock)
	require.NoError(t, err)
	fmt.Println("Finalize Block", libBlock.String())

}

func TestWaitForBlockHeight(t *testing.T) {
	rpcEndpoint := os.Getenv("TEST_BTC_RPC_ENDPOINT")
	if rpcEndpoint == "" {
		t.Skip("TEST_BTC_RPC_ENDPOINT not set")
	}
	connCfg := &rpcclient.ConnConfig{
		Host:         rpcEndpoint,
		DisableAuth:  true,
		HTTPPostMode: true,
		DisableTLS:   true,
	}

	client, err := rpcclient.New(connCfg, nil)
	require.NoError(t, err)

	r := &BlockFetcher{
		rpcClient:            client,
		headBlockWaitTimeout: 5 * time.Second,
		logger:               zap.NewNop(),
	}

	headBlock, err := r.GetHeadBlock()
	require.NoError(t, err)

	// Already reached: returns the head without waiting.
	reached, err := r.waitForBlockHeight(headBlock.Num())
	require.NoError(t, err)
	require.Equal(t, headBlock.Num(), reached.Num())

	// Not reached: blocks for the timeout, then reports the head the node is still on.
	start := time.Now()
	pending, err := r.waitForBlockHeight(headBlock.Num() + 1)
	require.NoError(t, err)
	require.True(t, time.Since(start) >= r.headBlockWaitTimeout)
	require.True(t, pending.Num() <= headBlock.Num())
	fmt.Println("waited on head", pending.String())
}

// Any waitforblockheight failure must fall back to reading the head block, not fail the fetch:
// an unsupported method, and equally a proxy that cuts the long-lived request.
func TestAwaitBlockHeightFallsBack(t *testing.T) {
	tests := []struct {
		name  string
		reply func(w http.ResponseWriter, id json.RawMessage)
	}{
		{"method not found", func(w http.ResponseWriter, id json.RawMessage) {
			_, _ = fmt.Fprintf(w, `{"result":null,"error":{"code":-32601,"message":"Method not found"},"id":%s}`, id)
		}},
		{"proxy cut the request", func(w http.ResponseWriter, _ json.RawMessage) {
			w.WriteHeader(http.StatusGatewayTimeout)
		}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var methods []string
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var req struct {
					ID     json.RawMessage `json:"id"`
					Method string          `json:"method"`
				}
				require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
				methods = append(methods, req.Method)
				w.Header().Set("Content-Type", "application/json")
				if req.Method == "waitforblockheight" {
					tc.reply(w, req.ID)
					return
				}
				// The head-block read then fails distinguishably.
				_, _ = fmt.Fprintf(w, `{"result":null,"error":{"code":-1,"message":"nope"},"id":%s}`, req.ID)
			}))
			defer srv.Close()

			client, err := rpcclient.New(&rpcclient.ConnConfig{
				Host:         strings.TrimPrefix(srv.URL, "http://"),
				DisableAuth:  true,
				HTTPPostMode: true,
				DisableTLS:   true,
			}, nil)
			require.NoError(t, err)

			r := &BlockFetcher{
				rpcClient:            client,
				headBlockWaitTimeout: time.Millisecond,
				logger:               zap.NewNop(),
			}

			_, err = r.awaitBlockHeight(100)
			require.Error(t, err)
			require.Contains(t, err.Error(), "failed to get head block")
			require.Equal(t, []string{"waitforblockheight", "getbestblockhash"}, methods)
		})
	}
}

// Parallel fetches must reach the node concurrently: an rpcclient in HTTP POST mode sends one
// request at a time, so a batch of N needs N clients.
func TestNewRPCClientsFetchConcurrently(t *testing.T) {
	const batchSize = 4

	var inFlight, maxInFlight atomic.Int32
	var releaseOnce sync.Once
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID json.RawMessage `json:"id"`
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&req))

		n := inFlight.Add(1)
		for {
			m := maxInFlight.Load()
			if n <= m || maxInFlight.CompareAndSwap(m, n) {
				break
			}
		}
		if n == batchSize {
			releaseOnce.Do(func() { close(release) })
		}
		// Hold each request until the whole batch is in flight, or give up if it never is.
		select {
		case <-release:
		case <-time.After(2 * time.Second):
		}
		inFlight.Add(-1)

		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"result":"ok","error":null,"id":%s}`, req.ID)
	}))
	defer srv.Close()

	clients, rpcClients, err := newRPCClients(&rpcclient.ConnConfig{
		Host:         strings.TrimPrefix(srv.URL, "http://"),
		DisableAuth:  true,
		HTTPPostMode: true,
		DisableTLS:   true,
	}, batchSize, zap.NewNop())
	require.NoError(t, err)
	defer func() {
		for _, c := range rpcClients {
			c.Shutdown()
		}
	}()
	require.Len(t, rpcClients, batchSize)

	var wg sync.WaitGroup
	for i := 0; i < batchSize; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := rpc.WithClients(clients, func(ctx context.Context, client *rpcclient.Client) (json.RawMessage, error) {
				return client.RawRequest("getblockcount", nil)
			})
			require.NoError(t, err)
		}()
	}
	wg.Wait()

	require.Equal(t, int32(batchSize), maxInFlight.Load(), "fetches were serialized")
}

// A chain shorter than LibOffset has its genesis block final, instead of asking for a block below 0.
func TestFinalizedBlockOfShortChain(t *testing.T) {
	var heights []int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
			Params []int64         `json:"params"`
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
		require.Equal(t, "getblockhash", req.Method)
		heights = append(heights, req.Params[0])
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"result":"0f9188f13cb7b2c71f2a335e3a4fc328bf5beb436012afca590b1a11466e2206","error":null,"id":%s}`, req.ID)
	}))
	defer srv.Close()

	client, err := rpcclient.New(&rpcclient.ConnConfig{
		Host:         strings.TrimPrefix(srv.URL, "http://"),
		DisableAuth:  true,
		HTTPPostMode: true,
		DisableTLS:   true,
	}, nil)
	require.NoError(t, err)

	r := &BlockFetcher{rpcClient: client, logger: zap.NewNop()}
	for _, head := range []uint64{0, 3, 6, 10} {
		finalized, err := r.GetFinalizedBlock(bstream.NewBlockRef("head", head))
		require.NoError(t, err)
		expected := uint64(0)
		if head > 6 {
			expected = head - 6
		}
		require.Equal(t, expected, finalized.Num(), "head %d", head)
	}
	require.Equal(t, []int64{0, 0, 0, 4}, heights)
}
