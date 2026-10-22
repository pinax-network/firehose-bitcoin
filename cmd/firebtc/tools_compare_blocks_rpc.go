package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/btcsuite/btcd/rpcclient"
	"github.com/spf13/cobra"
	"github.com/streamingfast/cli/sflags"
	"github.com/streamingfast/firehose-bitcoin/fetch"
	pbbitcoin "github.com/streamingfast/firehose-bitcoin/pb/sf/bitcoin/type/v1"
	"github.com/streamingfast/firehose-core/firehose/client"
	pbfirehose "github.com/streamingfast/pbgo/sf/firehose/v2"
	"go.uber.org/zap"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

var toolsCmd = &cobra.Command{
	Use:   "tools",
	Short: "Developer tools",
}

var compareBlocksRPCCmd = &cobra.Command{
	Use:   "compare-blocks-rpc <firehose-endpoint> <rpc-endpoint> <start-block> <stop-block>",
	Short: "Checks a Firehose range against the node's RPC (getblock), block by block",
	Long: `Streams final blocks from Firehose and compares each one with the block the node returns
for the same height, converted as the poller does. Also checks that every transaction's prevouts,
less its outputs, equal the fee the node reported.`,
	Example: `  firebtc tools compare-blocks-rpc -p localhost:10015 http://localhost:8332 840000 840010`,
	Args:    cobra.ExactArgs(4),
	RunE:    compareBlocksRPCE,
}

func init() {
	rootCmd.AddCommand(toolsCmd)
	toolsCmd.AddCommand(compareBlocksRPCCmd)

	compareBlocksRPCCmd.Flags().BoolP("plaintext", "p", false, "Use plaintext connection to Firehose")
	compareBlocksRPCCmd.Flags().BoolP("insecure", "k", false, "Use TLS to Firehose but skip certificate validation")
	compareBlocksRPCCmd.Flags().String("api-key-env-var", "FIREHOSE_API_KEY", "Environment variable holding the Firehose API key, if any")
	compareBlocksRPCCmd.Flags().StringSliceP("headers", "H", nil, "HTTP headers for the RPC endpoint (ex: 'Authorization: Basic ...')")
	compareBlocksRPCCmd.Flags().Int("getblock-verbosity", 3, "The getblock verbosity the Firehose blocks were produced with")
	compareBlocksRPCCmd.Flags().String("save-files", "", "Directory where the Firehose and RPC versions of differing blocks are saved as fh_<num>.json and rpc_<num>.json")
}

func compareBlocksRPCE(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()

	startBlock, err := strconv.ParseUint(args[2], 10, 64)
	if err != nil {
		return fmt.Errorf("invalid start block %q: %w", args[2], err)
	}
	stopBlock, err := strconv.ParseUint(args[3], 10, 64)
	if err != nil {
		return fmt.Errorf("invalid stop block %q: %w", args[3], err)
	}

	headers, err := parseHeaders(sflags.MustGetStringSlice(cmd, "headers"))
	if err != nil {
		return err
	}

	rpcEndpoint := args[1]
	https := strings.HasPrefix(rpcEndpoint, "https://")
	rpcEndpoint = strings.TrimPrefix(strings.TrimPrefix(rpcEndpoint, "https://"), "http://")
	rpcClient, err := rpcclient.New(&rpcclient.ConnConfig{
		Host:         rpcEndpoint,
		DisableAuth:  true,
		HTTPPostMode: true,
		DisableTLS:   !https,
		ExtraHeaders: headers,
	}, nil)
	if err != nil {
		return fmt.Errorf("unable to setup rpc client: %w", err)
	}
	defer rpcClient.Shutdown()

	firehoseClient, closeFunc, callOpts, err := client.NewFirehoseClient(
		args[0],
		"",
		os.Getenv(sflags.MustGetString(cmd, "api-key-env-var")),
		sflags.MustGetBool(cmd, "insecure"),
		sflags.MustGetBool(cmd, "plaintext"),
	)
	if err != nil {
		return fmt.Errorf("unable to setup firehose client: %w", err)
	}
	defer closeFunc()

	stream, err := firehoseClient.Blocks(ctx, &pbfirehose.Request{
		StartBlockNum:   int64(startBlock),
		StopBlockNum:    stopBlock,
		FinalBlocksOnly: true,
	}, callOpts...)
	if err != nil {
		return fmt.Errorf("unable to start firehose stream: %w", err)
	}

	verbosity := sflags.MustGetInt(cmd, "getblock-verbosity")
	saveDir := sflags.MustGetString(cmd, "save-files")

	var blocks, txs, differences int
	for {
		response, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("firehose stream: %w", err)
		}

		fhBlock := &pbbitcoin.Block{}
		if err := response.Block.UnmarshalTo(fhBlock); err != nil {
			return fmt.Errorf("unable to decode firehose block: %w", err)
		}

		rpcBlock, err := fetch.FetchBlock(rpcClient, fhBlock.Height, verbosity)
		if err != nil {
			return err
		}

		blocks++
		txs += len(fhBlock.Tx)

		if !proto.Equal(fhBlock, rpcBlock) {
			differences++
			fmt.Printf("block %d: differs from RPC\n%s\n", fhBlock.Height, firstDifferences(fhBlock, rpcBlock, 10))
			if saveDir != "" {
				if err := saveBlocks(saveDir, fhBlock, rpcBlock); err != nil {
					return err
				}
			}
		}

		if verbosity >= 3 {
			if err := fetch.CheckFees(fhBlock); err != nil {
				differences++
				fmt.Printf("block %d: %s\n", fhBlock.Height, err)
			}
		}

		if blocks%100 == 0 {
			zlog.Info("compared blocks", zap.Int("blocks", blocks), zap.Int64("last_block", fhBlock.Height), zap.Int("differences", differences))
		}
	}

	fmt.Printf("compared %d blocks, %d transactions: %d differences\n", blocks, txs, differences)
	if differences > 0 {
		return fmt.Errorf("%d differences", differences)
	}
	if expected := int(stopBlock - startBlock + 1); blocks != expected {
		return fmt.Errorf("compared %d blocks, expected %d", blocks, expected)
	}
	return nil
}

var jsonOptions = protojson.MarshalOptions{Multiline: true, Indent: " "}

// firstDifferences returns the first lines where the two blocks' JSON differ.
func firstDifferences(fh, rpc *pbbitcoin.Block, max int) string {
	fhLines := strings.Split(jsonOptions.Format(fh), "\n")
	rpcLines := strings.Split(jsonOptions.Format(rpc), "\n")

	var out []string
	for i := 0; i < len(fhLines) || i < len(rpcLines); i++ {
		var fhLine, rpcLine string
		if i < len(fhLines) {
			fhLine = fhLines[i]
		}
		if i < len(rpcLines) {
			rpcLine = rpcLines[i]
		}
		if fhLine == rpcLine {
			continue
		}
		out = append(out, fmt.Sprintf("  line %d:\n    firehose: %s\n    rpc:      %s", i+1, strings.TrimSpace(fhLine), strings.TrimSpace(rpcLine)))
		if len(out) == max {
			out = append(out, "  ...")
			break
		}
	}
	return strings.Join(out, "\n")
}

func saveBlocks(dir string, fh, rpc *pbbitcoin.Block) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	for name, blk := range map[string]*pbbitcoin.Block{"fh": fh, "rpc": rpc} {
		path := filepath.Join(dir, fmt.Sprintf("%s_%d.json", name, blk.Height))
		if err := os.WriteFile(path, []byte(jsonOptions.Format(blk)), 0o644); err != nil {
			return fmt.Errorf("unable to save %s: %w", path, err)
		}
	}
	return nil
}
