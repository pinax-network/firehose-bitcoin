# Battlefield

End-to-end check of firebtc on a regtest chain, after StreamingFast's battlefield repositories
(battlefield-ethereum, battlefield-eosio).

`run.sh` starts `bitcoind -regtest`, runs `scenario.sh`, which puts every common output and spend
type on chain with Bitcoin Core's wallet:

- spends of P2PKH, P2SH-P2WPKH, P2WPKH and P2TR (key path) outputs, and of matured coinbases
- 1-of-2 multisig in P2SH and P2WSH
- an OP_RETURN data output
- a chain of spends inside one block (outputs spent in the block that creates them)
- a 50-output transaction, then a 50-input one

It checks every type made it on chain, then serves the chain through firebtc
(`--getblock-verbosity=3`) under firecore and compares every block, from genesis to the scenario's
last, with bitcoind (`firebtc tools compare-blocks-rpc`), which also checks each transaction's
prevouts against its fee.

P2PK spends only exist in early mainnet blocks: check them with
`firebtc tools compare-blocks-rpc` over blocks 170-270. Taproot script-path spends, and the
OP_RETURN runestones, are on the firehose-bitcoin-ordinals battlefield's chain.

## Running

Needs on `PATH`: `bitcoind`, `bitcoin-cli` (Bitcoin Core 23+), `firebtc` (this branch),
`firecore`, `jq`.

```
./battlefield/run.sh
```

`BATTLEFIELD_WORK` sets the work directory (temporary by default), with `firehose.log` and
`compare.log`. `BATTLEFIELD_RPC_PORT` and `BATTLEFIELD_FIREHOSE_PORT` move the ports (38443,
39015 and the two after it).
