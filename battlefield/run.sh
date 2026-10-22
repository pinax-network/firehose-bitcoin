#!/usr/bin/env bash
# firehose-bitcoin battlefield.
#
# Builds a regtest chain whose blocks hold every common output and spend type (scenario.sh),
# serves it through firebtc (--getblock-verbosity=3) under firecore, and checks every block
# against bitcoind with `firebtc tools compare-blocks-rpc`, which also checks each transaction's
# prevouts against its fee.
#
# Needs on PATH: bitcoind bitcoin-cli firebtc firecore jq
set -euo pipefail

ROOT=$(cd "$(dirname "$0")" && pwd)
WORK=${BATTLEFIELD_WORK:-$(mktemp -d -t bitcoin-battlefield.XXXXXX)}

RPC_PORT=${BATTLEFIELD_RPC_PORT:-38443}
P2P_PORT=$((RPC_PORT + 1))
FIREHOSE_PORT=${BATTLEFIELD_FIREHOSE_PORT:-39015}
# blocks firebtc considers final trail the head by this many
LIB_OFFSET=6

for bin in bitcoind bitcoin-cli firebtc firecore jq; do
  command -v "$bin" >/dev/null || { echo "missing $bin on PATH" >&2; exit 1; }
done

for port in "$RPC_PORT" "$P2P_PORT" "$FIREHOSE_PORT" $((FIREHOSE_PORT + 1)) $((FIREHOSE_PORT + 2)); do
  if (echo >"/dev/tcp/127.0.0.1/$port") 2>/dev/null; then
    echo "port $port is already in use, is another battlefield running?" >&2
    exit 1
  fi
done

echo "battlefield work dir: $WORK"
mkdir -p "$WORK"/{bitcoind,firehose}

pids=()
cleanup() {
  for pid in "${pids[@]}"; do kill "$pid" 2>/dev/null || true; done
  cli stop >/dev/null 2>&1 || true
  # firecore takes a few seconds to shut down; the next run needs its ports
  for pid in "${pids[@]}"; do
    for _ in $(seq 1 100); do kill -0 "$pid" 2>/dev/null || break; sleep 0.2; done
  done
}
trap cleanup EXIT

cli() {
  bitcoin-cli -regtest -datadir="$WORK/bitcoind" -rpcport="$RPC_PORT" -rpcuser=bf -rpcpassword=bf "$@"
}

w() {
  cli -rpcwallet=bf "$@"
}

mine() {
  w -generate "${1:-1}" >/dev/null
}

echo "== bitcoind"
bitcoind -regtest -daemonwait -datadir="$WORK/bitcoind" -rpcport="$RPC_PORT" -port="$P2P_PORT" \
  -rpcuser=bf -rpcpassword=bf -fallbackfee=0.0001 -listen=0 -server >/dev/null
cli -named createwallet wallet_name=bf >/dev/null
mine 110

echo "== scenario"
FIRST=$(($(cli getblockcount) + 1))
# shellcheck source=scenario.sh
source "$ROOT/scenario.sh"
LAST=$(cli getblockcount)

# every case must be on chain: the script types spent, and created, by the scenario blocks
spent=$(for height in $(seq "$FIRST" "$LAST"); do
  cli getblock "$(cli getblockhash "$height")" 3 | jq -r '.tx[].vin[].prevout.scriptPubKey.type // empty'
done | sort -u)
created=$(for height in $(seq "$FIRST" "$LAST"); do
  cli getblock "$(cli getblockhash "$height")" 3 | jq -r '.tx[].vout[].scriptPubKey.type'
done | sort -u)
# (P2PK spends come from early mainnet, e.g. compare-blocks-rpc over blocks 170-270)
for type in pubkeyhash scripthash witness_v0_keyhash witness_v0_scripthash witness_v1_taproot; do
  grep -qx "$type" <<<"$spent" || { echo "scenario spent no $type output" >&2; exit 1; }
done
grep -qx nulldata <<<"$created" || { echo "scenario created no OP_RETURN output" >&2; exit 1; }
echo "spent: $(echo $spent)"

# Firehose serves the range from merged blocks: mine until the 100-block bundle holding the last
# scenario block is merged (the merger needs the next bundle started, past LIB)
mine $(( (LAST / 100 + 1) * 100 + LIB_OFFSET + 10 - LAST ))
echo "scenario in blocks $FIRST-$LAST, chain at $(cli getblockcount)"

echo "== firehose-bitcoin"
cat >"$WORK/firehose/config.yml" <<EOF
start:
  args:
    - reader-node
    - merger
    - firehose
  flags:
    log-to-file: false
    common-live-blocks-addr: localhost:$((FIREHOSE_PORT + 1))
    reader-node-grpc-listen-addr: :$((FIREHOSE_PORT + 1))
    reader-node-path: $(command -v firebtc)
    reader-node-arguments: >-
      poller 0
      --getblock-verbosity=3
      --block-fetch-batch-size=4
      --reader-state-storage-path=$WORK/firehose/poller
      --rpc-endpoint=http://127.0.0.1:$RPC_PORT
      -H "Authorization: Basic $(printf bf:bf | base64)"
    merger-grpc-listen-addr: :$((FIREHOSE_PORT + 2))
    firehose-grpc-listen-addr: :$FIREHOSE_PORT
    # regtest's genesis block isn't one firecore can infer the chain from
    advertise-chain-name: bitcoin-regtest
EOF
firecore -d "$WORK/firehose/data" -c "$WORK/firehose/config.yml" start >"$WORK/firehose.log" 2>&1 &
pids+=($!)
for _ in $(seq 1 150); do (echo >"/dev/tcp/127.0.0.1/$FIREHOSE_PORT") 2>/dev/null && break; sleep 0.2; done

echo "== compare with bitcoind"
# every block since genesis, the scenario and the coinbase-only blocks around it
firebtc tools compare-blocks-rpc -p "127.0.0.1:$FIREHOSE_PORT" "http://127.0.0.1:$RPC_PORT" \
  -H "Authorization: Basic $(printf bf:bf | base64)" 1 "$LAST" 2>"$WORK/compare.log"

echo "battlefield passed"
