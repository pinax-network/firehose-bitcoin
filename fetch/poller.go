package fetch

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/btcsuite/btcd/rpcclient"
	"github.com/streamingfast/bstream"
	pbbstream "github.com/streamingfast/bstream/pb/sf/bstream/v1"
	pbbitcoin "github.com/streamingfast/firehose-bitcoin/pb/sf/bitcoin/type/v1"
	"github.com/streamingfast/firehose-core/blockpoller"
	"github.com/streamingfast/firehose-core/rpc"
	"github.com/streamingfast/shutter"
	"go.uber.org/zap"
)

// maxBlockFetchDuration bounds a single Fetch call. Fetch blocks until the requested block
// exists on chain, as blockpoller expects, so this is an outer safety net rather than a
// per-RPC timeout: it must stay well above the longest plausible gap between two blocks.
const maxBlockFetchDuration = time.Hour

type BlockFetcher struct {
	*shutter.Shutter
	endpoint             string
	blockFetchRetryCount uint64
	blockFetchBatchSize  int
	stateStoragePath     string
	startBlockNum        uint64
	ignoreCursor         bool
	headBlockWaitTimeout time.Duration
	getblockVerbosity    int
	// headBlock is shared by the concurrent fetches when blockFetchBatchSize > 1
	headBlockLock sync.RWMutex
	headBlock     bstream.BlockRef
	rpcClient     *rpcclient.Client
	logger        *zap.Logger

	headers    map[string]string
	disableTLS bool
}

func New(
	endpoint string,
	https bool,
	blockFetchRetryCount uint64,
	blockFetchBatchSize int,
	stateStoragePath string,
	startBlockNum uint64,
	ignoreCursor bool,
	headBlockWaitTimeout time.Duration,
	getblockVerbosity int,
	headers map[string]string,
	logger *zap.Logger) *BlockFetcher {
	return &BlockFetcher{
		Shutter:              shutter.New(),
		endpoint:             endpoint,
		blockFetchRetryCount: blockFetchRetryCount,
		blockFetchBatchSize:  blockFetchBatchSize,
		stateStoragePath:     stateStoragePath,
		startBlockNum:        startBlockNum,
		ignoreCursor:         ignoreCursor,
		headBlockWaitTimeout: headBlockWaitTimeout,
		getblockVerbosity:    getblockVerbosity,
		disableTLS:           !https,
		headers:              headers,
		logger:               logger.Named("poller"),
	}
}

func (p *BlockFetcher) Run(ctx context.Context) error {
	contentType := getContentType()
	p.logger.Info("launching firebtc poller",
		zap.String("endpoint", p.endpoint),
		zap.String("content", contentType),
		zap.Uint64("start_block_num", p.startBlockNum),
		zap.Int("getblock_verbosity", p.getblockVerbosity),
		zap.Int("block_fetch_batch_size", p.blockFetchBatchSize),
	)

	connCfg := &rpcclient.ConnConfig{
		Host:         p.endpoint,
		DisableAuth:  true,
		HTTPPostMode: true,
		DisableTLS:   p.disableTLS,
		ExtraHeaders: p.headers,
	}

	clients, rpcClients, err := newRPCClients(connCfg, p.blockFetchBatchSize, p.logger)
	if err != nil {
		return err
	}
	defer func() {
		for _, client := range rpcClients {
			client.Shutdown()
		}
	}()
	p.rpcClient = rpcClients[0]

	opts := []blockpoller.Option[*rpcclient.Client]{
		blockpoller.WithLogger[*rpcclient.Client](p.logger),
		blockpoller.WithBlockFetchRetryCount[*rpcclient.Client](p.blockFetchRetryCount),
		blockpoller.WithStoringState[*rpcclient.Client](p.stateStoragePath),
	}

	if p.ignoreCursor {
		opts = append(opts, blockpoller.IgnoreCursor[*rpcclient.Client]())
	}

	bp := blockpoller.New[*rpcclient.Client](p, blockpoller.NewFireBlockHandler(contentType), clients, opts...)
	p.OnTerminating(func(err error) {
		p.logger.Info("shutting down firebtc reader", zap.Error(err))
		bp.Shutdown(nil)
	})

	headBlock, err := p.GetHeadBlock()
	if err != nil {
		return fmt.Errorf("failed to get head block: %w", err)
	}
	p.setKnownHeadBlock(headBlock)

	finalizedBlk, err := p.GetFinalizedBlock(headBlock)
	if err != nil {
		return fmt.Errorf("failed to get finalized block: %w", err)
	}
	p.logger.Info("retrieved finalized and head block",
		zap.Stringer("finalized_block", finalizedBlk),
		zap.Stringer("chain_head_block", headBlock),
	)

	return bp.Run(p.startBlockNum, nil, p.blockFetchBatchSize)
}

func (p *BlockFetcher) GetFinalizedBlock(headBlock bstream.BlockRef) (bstream.BlockRef, error) {
	return p.getFinalizedBlock(p.rpcClient, headBlock)
}

func (p *BlockFetcher) getFinalizedBlock(client *rpcclient.Client, headBlock bstream.BlockRef) (bstream.BlockRef, error) {
	// a chain shorter than LibOffset (a new regtest chain) has only its genesis block final
	finalizedBlockNum := int64(0)
	if headBlock.Num() > pbbitcoin.LibOffset {
		finalizedBlockNum = int64(headBlock.Num() - pbbitcoin.LibOffset)
	}
	finalizedBlockHash, err := client.GetBlockHash(finalizedBlockNum)
	if err != nil {
		return nil, fmt.Errorf("failed to get finalized block hash: %w", err)
	}

	return bstream.NewBlockRef(finalizedBlockHash.String(), uint64(finalizedBlockNum)), nil
}

func (p *BlockFetcher) GetHeadBlock() (bstream.BlockRef, error) {
	return p.getHeadBlock(p.rpcClient)
}

func (p *BlockFetcher) getHeadBlock(client *rpcclient.Client) (bstream.BlockRef, error) {
	bestBlockHash, err := client.GetBestBlockHash()
	if err != nil {
		return nil, fmt.Errorf("unable to get best block hash: %w", err)
	}
	p.logger.Info("found best block hash", zap.Stringer("blockhash", bestBlockHash))

	bestBlock, err := client.GetBlockVerbose(bestBlockHash)
	if err != nil {
		return nil, fmt.Errorf("unable to get best block %s: %w", bestBlockHash.String(), err)
	}

	return bstream.NewBlockRef(bestBlockHash.String(), uint64(bestBlock.Height)), nil
}

// awaitBlockHeight waits for the node to reach height and returns its head block. Any failure —
// a node without waitforblockheight (Bitcoin Core before 0.12), a proxy cutting the long-lived
// request — falls back to sleep-polling.
func (p *BlockFetcher) awaitBlockHeight(height uint64) (bstream.BlockRef, error) {
	headBlock, err := p.waitForBlockHeight(height)
	if err == nil {
		return headBlock, nil
	}
	p.logger.Debug("unable to wait on the node, polling the head block instead", zap.Error(err))

	headBlock, err = p.GetHeadBlock()
	if err != nil {
		return nil, fmt.Errorf("failed to get head block: %w", err)
	}
	if headBlock.Num() < height {
		time.Sleep(p.headBlockWaitTimeout)
	}

	return headBlock, nil
}

// waitForBlockHeight blocks on the node until it reaches height, and returns its head block as soon
// as it does. On timeout it returns the head the node is still on, leaving the caller to wait again.
func (p *BlockFetcher) waitForBlockHeight(height uint64) (bstream.BlockRef, error) {
	params := []json.RawMessage{
		json.RawMessage(strconv.FormatUint(height, 10)),
		json.RawMessage(strconv.FormatInt(p.headBlockWaitTimeout.Milliseconds(), 10)),
	}

	raw, err := p.rpcClient.RawRequest("waitforblockheight", params)
	if err != nil {
		return nil, fmt.Errorf("unable to wait for block height %d: %w", height, err)
	}

	var result struct {
		Hash   string `json:"hash"`
		Height uint64 `json:"height"`
	}
	if err := json.Unmarshal(raw, &result); err != nil {
		return nil, fmt.Errorf("unable to decode waitforblockheight response: %w", err)
	}
	if result.Hash == "" {
		return nil, fmt.Errorf("node returned no block hash waiting for height %d", height)
	}

	return bstream.NewBlockRef(result.Hash, result.Height), nil
}

func (p *BlockFetcher) knownHeadBlock() bstream.BlockRef {
	p.headBlockLock.RLock()
	defer p.headBlockLock.RUnlock()
	return p.headBlock
}

func (p *BlockFetcher) setKnownHeadBlock(headBlock bstream.BlockRef) {
	p.headBlockLock.Lock()
	defer p.headBlockLock.Unlock()
	p.headBlock = headBlock
}

func (p *BlockFetcher) IsBlockAvailable(requestedSlot uint64) bool {
	return requestedSlot <= p.knownHeadBlock().Num()
}

func (p *BlockFetcher) Fetch(ctx context.Context, client *rpcclient.Client, blkNum uint64) (*pbbstream.Block, bool, error) {
	for p.knownHeadBlock().Num() < blkNum {
		if err := ctx.Err(); err != nil {
			return nil, false, err
		}

		p.logger.Debug("head block is behind, waiting for it to catch up",
			zap.Stringer("head_block", p.knownHeadBlock()),
			zap.Uint64("block_num", blkNum),
			zap.Duration("wait_timeout", p.headBlockWaitTimeout),
		)

		headBlock, err := p.awaitBlockHeight(blkNum)
		if err != nil {
			return nil, false, err
		}
		p.setKnownHeadBlock(headBlock)
	}

	p.logger.Debug("fetching block", zap.Uint64("block_num", blkNum))
	t0 := time.Now()
	blkHash, err := client.GetBlockHash(int64(blkNum))
	if err != nil {
		return nil, false, fmt.Errorf("unable to get block hash for block %d: %w", blkNum, err)
	}
	duration := time.Since(t0)

	t1 := time.Now()
	rpcBlk, err := getBlock(client, blkHash.String(), p.getblockVerbosity)
	if err != nil {
		return nil, false, fmt.Errorf("unable to get block %d (%s): %w", blkNum, blkHash.String(), err)
	}

	p.logger.Debug("found block",
		zap.Int64("block_num", rpcBlk.Height),
		zap.String("block_hash", rpcBlk.Hash),
		zap.String("prev_hash", rpcBlk.PreviousBlockHash),
		zap.Duration("blockhash_duration", duration),
		zap.Duration("block_duration", time.Since(t1)),
		zap.Duration("total_duration", time.Since(t0)),
	)

	blk := rpcBlk.toProto()

	return blk.MustToBstreamBlock(), false, nil
}

func getContentType() string {
	blk := &pbbitcoin.Block{}
	return "type.googleapis.com/" + string(blk.ProtoReflect().Descriptor().FullName())
}

// newRPCClients returns one client per parallel fetch: an rpcclient in HTTP POST mode sends one
// request at a time, so a single client would serialize them. The spread start hands each
// concurrent fetch a different client.
func newRPCClients(connCfg *rpcclient.ConnConfig, blockFetchBatchSize int, logger *zap.Logger) (*rpc.Clients[*rpcclient.Client], []*rpcclient.Client, error) {
	var rollingStrategy rpc.RollingStrategy[*rpcclient.Client] = rpc.NewStickyRollingStrategy[*rpcclient.Client]()
	if blockFetchBatchSize > 1 {
		rollingStrategy = rpc.NewRollingStrategySequential[*rpcclient.Client](rpc.WithSpreadStart())
	}
	clients := rpc.NewClients(maxBlockFetchDuration, rollingStrategy, logger)

	var rpcClients []*rpcclient.Client
	for i := 0; i < max(blockFetchBatchSize, 1); i++ {
		client, err := rpcclient.New(connCfg, nil)
		if err != nil {
			for _, c := range rpcClients {
				c.Shutdown()
			}
			return nil, nil, fmt.Errorf("failed to setup rpc client: %w", err)
		}
		rpcClients = append(rpcClients, client)
		clients.AddNamed(client, fmt.Sprintf("rpc-%d", i))
	}

	return clients, rpcClients, nil
}
