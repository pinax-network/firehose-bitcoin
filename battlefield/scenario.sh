# Battlefield scenario, sourced by run.sh: puts every common output and spend type on chain.
# Uses run.sh's cli, w (the funded `bf` wallet) and mine.

cli -named createwallet wallet_name=dst >/dev/null
DST=$(cli -rpcwallet=dst getnewaddress "" bech32)

# spends exactly one output to DST, the rest back as change
spend() {
  local txid=$1 vout=$2
  w -named send outputs="[{\"$DST\": 0.1}]" \
    options="{\"inputs\": [{\"txid\": \"$txid\", \"vout\": $vout}], \"add_inputs\": false}" |
    jq -r .txid
}

vout_to() {
  w gettransaction "$1" | jq -r --arg address "$2" '.details[] | select(.address == $address and .category == "send") | .vout' | head -1
}

echo "-- fund every single-key address type (coinbase spends)"
declare -A FUNDED
for type in legacy p2sh-segwit bech32 bech32m; do
  address=$(w getnewaddress "" "$type")
  txid=$(w sendtoaddress "$address" 1)
  FUNDED[$type]="$txid $(vout_to "$txid" "$address")"
done
mine 1

echo "-- spend every single-key type: P2PKH, P2SH-P2WPKH, P2WPKH, P2TR key path"
for type in legacy p2sh-segwit bech32 bech32m; do
  spend ${FUNDED[$type]} >/dev/null
done
mine 1

echo "-- 1-of-2 multisig: P2SH and P2WSH"
# descriptor wallets only: build the multisig descriptors from bf's own private keys
TPRV=$(w listdescriptors true | jq -r '[.descriptors[].desc | select(startswith("wpkh("))][0]' |
  sed -E 's/^wpkh\(([^/]+).*/\1/')
cli -named createwallet wallet_name=ms blank=true >/dev/null
for wrap in sh wsh; do
  desc="$wrap(multi(1,$TPRV/87/0/*,$TPRV/87/1/*))"
  checksum=$(cli getdescriptorinfo "$desc" | jq -r .checksum)
  cli -rpcwallet=ms importdescriptors \
    "[{\"desc\": \"$desc#$checksum\", \"timestamp\": \"now\", \"active\": false, \"range\": [0, 10]}]" >/dev/null
done
MS_ADDRESSES=()
for wrap in sh wsh; do
  desc="$wrap(multi(1,$TPRV/87/0/*,$TPRV/87/1/*))"
  public=$(cli getdescriptorinfo "$desc" | jq -r .descriptor)
  MS_ADDRESSES+=("$(cli deriveaddresses "$public" "[0,0]" | jq -r '.[0]')")
done
for address in "${MS_ADDRESSES[@]}"; do
  w sendtoaddress "$address" 1 >/dev/null
done
mine 1
cli -rpcwallet=ms -named send outputs="[{\"$DST\": 1.5}]" \
  options="{\"change_address\": \"$DST\"}" >/dev/null
mine 1

echo "-- OP_RETURN data"
w -named send outputs='[{"data": "626174746c656669656c64"}]' >/dev/null
mine 1

echo "-- chain of spends inside one block"
first=$(w sendtoaddress "$(w getnewaddress "" bech32m)" 2)
second=$(w -named send outputs="[{\"$(w getnewaddress "" bech32)\": 1}]" \
  options="{\"inputs\": [{\"txid\": \"$first\", \"vout\": $(vout_to "$first" "$(w gettransaction "$first" | jq -r '[.details[] | select(.category == "send")][0].address')")}], \"add_inputs\": true}" |
  jq -r .txid)
w -named send outputs="[{\"$DST\": 0.5}]" \
  options="{\"inputs\": [{\"txid\": \"$second\", \"vout\": 0}, {\"txid\": \"$second\", \"vout\": 1}], \"add_inputs\": false}" >/dev/null
mine 1

echo "-- many outputs, then many inputs"
outputs=$(for i in $(seq 1 50); do printf '"%s": 0.01,' "$(cli -rpcwallet=dst getnewaddress "" bech32)"; done)
w sendmany "" "{${outputs%,}}" >/dev/null
mine 1
cli -rpcwallet=dst -named sendall recipients="[\"$(w getnewaddress "" bech32m)\"]" >/dev/null
mine 1
