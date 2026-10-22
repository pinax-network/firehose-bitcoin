package main

import (
	"fmt"
	"github.com/streamingfast/firehose-bitcoin/fetch"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/streamingfast/cli"
	"github.com/streamingfast/cli/sflags"
	"go.uber.org/zap"
)

var pollerCmd = &cobra.Command{
	Use:          "poller <start-block>",
	Short:        "launches the RPC reader",
	SilenceUsage: true,
	RunE:         readerRunE,
	Args:         cobra.ExactArgs(1),
}

func init() {
	rootCmd.AddCommand(pollerCmd)
	pollerCmd.Flags().String("rpc-endpoint", "http://localhost:8333", "The bitcoin RPC node")
	pollerCmd.Flags().Bool("ignore-cursor", false, "When enable it will ignore the cursor and start from the start block num, the cursor will still be saved as the poller progresses")
	pollerCmd.Flags().Uint64("block-fetch-retry-count", 3, "The number of times to retry fetching a block before ending in error")
	pollerCmd.Flags().Int("block-fetch-batch-size", 1, "The number of blocks to fetch in parallel while catching up; blocks are still emitted in order")
	pollerCmd.Flags().Duration("head-block-wait-timeout", 30*time.Second, "How long a single waitforblockheight call may block on the node before it is retried")
	pollerCmd.Flags().Int("getblock-verbosity", 2, "The getblock verbosity: 2, or 3 to include each input's prevout (requires Bitcoin Core 23.0+ and an unpruned node)")
	pollerCmd.Flags().String("reader-state-storage-path", "/localdata/", "The local path where the reader state will be stored, if blank no state will be stored")
	pollerCmd.Flags().StringSliceP("headers", "H", nil, "List of HTTP headers to pass with the request (ex: 'Authorization: Basic ...')")
	pollerCmd.Flags().Duration("graceful-shutdown-delay", 0*time.Millisecond, "delay before shutting down, after the health endpoint returns unhealthy")
	pollerCmd.Flags().Duration("unready-period-delay", 0*time.Millisecond, "the delay starting the shutdown sequence after the health endpoint returns unhealthy")
}

func readerRunE(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()

	startBlockNumStr := args[0]
	rpcEndpoint := sflags.MustGetString(cmd, "rpc-endpoint")
	blockFetchRetryCount := sflags.MustGetUint64(cmd, "block-fetch-retry-count")
	blockFetchBatchSize := sflags.MustGetInt(cmd, "block-fetch-batch-size")
	readerStateStoragePath := sflags.MustGetString(cmd, "reader-state-storage-path")
	headerStrings := sflags.MustGetStringSlice(cmd, "headers")
	gracefulShutdownDelay := sflags.MustGetDuration(cmd, "graceful-shutdown-delay")
	unreadyPeriodDelay := sflags.MustGetDuration(cmd, "unready-period-delay")
	ignoreCursor := sflags.MustGetBool(cmd, "ignore-cursor")
	headBlockWaitTimeout := sflags.MustGetDuration(cmd, "head-block-wait-timeout")
	getblockVerbosity := sflags.MustGetInt(cmd, "getblock-verbosity")

	zlog.Info("launching firebtc reader",
		zap.String("start_block_num", startBlockNumStr),
		zap.String("rpc_endpoint", rpcEndpoint),
		zap.Uint64("block_fetch_retry_count", blockFetchRetryCount),
		zap.Int("block_fetch_batch_size", blockFetchBatchSize),
		zap.String("reader_state_storage_path", readerStateStoragePath),
		zap.Strings("reader_state_storage_path", headerStrings),
		zap.Duration("graceful_shutdown_delay", gracefulShutdownDelay),
		zap.Duration("unready_period_delay", unreadyPeriodDelay),
		zap.Bool("ignore_cursor", ignoreCursor),
		zap.Duration("head_block_wait_timeout", headBlockWaitTimeout),
		zap.Int("getblock_verbosity", getblockVerbosity),
	)

	startBlockNum, err := strconv.ParseUint(startBlockNumStr, 10, 64)
	if err != nil {
		return fmt.Errorf("unable to parse start block number %s: %w", startBlockNumStr, err)
	}

	if blockFetchBatchSize < 1 {
		return fmt.Errorf("invalid block fetch batch size %d: must be at least 1", blockFetchBatchSize)
	}

	if getblockVerbosity != 2 && getblockVerbosity != 3 {
		return fmt.Errorf("invalid getblock verbosity %d: must be 2 or 3", getblockVerbosity)
	}

	headers, err := parseHeaders(headerStrings)
	if err != nil {
		return err
	}

	var https bool
	if strings.HasPrefix(rpcEndpoint, "https://") {
		https = true
		rpcEndpoint = strings.TrimPrefix(rpcEndpoint, "https://")
	} else if strings.HasPrefix(rpcEndpoint, "http://") {
		rpcEndpoint = strings.TrimPrefix(rpcEndpoint, "http://")
	}

	p := fetch.New(rpcEndpoint, https, blockFetchRetryCount, blockFetchBatchSize, readerStateStoragePath, startBlockNum, ignoreCursor, headBlockWaitTimeout, getblockVerbosity, headers, zlog)
	app := cli.NewApplication(ctx)
	app.SuperviseAndStart(p)

	return app.WaitForTermination(zlog, unreadyPeriodDelay, gracefulShutdownDelay)
}

func parseHeaders(headers []string) (map[string]string, error) {
	if headers == nil {
		return nil, nil
	}
	result := make(map[string]string)
	for _, header := range headers {
		parts := strings.Split(header, ":")
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid header format: %s", header)
		}
		result[strings.TrimSpace(parts[0])] = strings.TrimSpace(parts[1])
	}
	return result, nil
}
