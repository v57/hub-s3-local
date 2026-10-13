#!/usr/bin/env bash
set -euo pipefail
umask 077

# HN-502 Linux acceptance: a caller and provider use real kernel WireGuard
# interfaces. The provider hosts S3 on its overlay address; the caller uploads
# and downloads through canonical presigned hub.invalid URLs.

if [[ $(uname -s) != Linux ]]; then
  echo "SKIP: Linux network namespaces required"
  exit 0
fi
if [[ ${EUID} -ne 0 ]]; then
  echo "SKIP: root required"
  exit 0
fi
for command in curl ip nft python3 wg; do
  command -v "$command" >/dev/null || {
    echo "SKIP: missing required command: $command"
    exit 0
  }
done
bun_bin=${BUN_BIN:-$(command -v bun || true)}
if [[ -z "$bun_bin" || ! -x "$bun_bin" ]]; then
  echo "SKIP: missing required command: bun (pass BUN_BIN through sudo)"
  exit 0
fi

root_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
run_id="hn502-$$"
caller="${run_id}-caller"
provider="${run_id}-provider"
link_id="h$$"
tmp_dir=$(mktemp -d /run/hn502.XXXXXX)
mkdir -p /etc/wireguard
key_dir=$(mktemp -d /etc/wireguard/hn502.XXXXXX)
authority="s3-testowner.v1-testhub.hub.invalid"
s3_pid=
tcp_pid=
udp_pid=

cleanup() {
  set +e
  [[ -n "$s3_pid" ]] && kill "$s3_pid" 2>/dev/null
  [[ -n "$tcp_pid" ]] && kill "$tcp_pid" 2>/dev/null
  [[ -n "$udp_pid" ]] && kill "$udp_pid" 2>/dev/null
  wait "$s3_pid" 2>/dev/null
  wait "$tcp_pid" 2>/dev/null
  wait "$udp_pid" 2>/dev/null
  ip netns delete "$caller" 2>/dev/null
  ip netns delete "$provider" 2>/dev/null
  rm -rf "/etc/netns/$caller" "/etc/netns/$provider"
  rm -rf "$tmp_dir" "$key_dir"
}
trap cleanup EXIT

for namespace in "$caller" "$provider"; do
  ip netns add "$namespace"
  ip -n "$namespace" link set lo up
  mkdir -p "/etc/netns/$namespace"
done

ip link add "${link_id}c" type veth peer name "${link_id}p"
ip link set "${link_id}c" netns "$caller"
ip link set "${link_id}p" netns "$provider"
ip -n "$caller" link set "${link_id}c" name eth0
ip -n "$provider" link set "${link_id}p" name eth0
ip -n "$caller" addr add 192.0.2.10/24 dev eth0
ip -n "$provider" addr add 192.0.2.20/24 dev eth0
ip -n "$caller" link set eth0 up
ip -n "$provider" link set eth0 up

wg genkey >"$key_dir/caller.key"
wg genkey >"$key_dir/provider.key"
wg pubkey <"$key_dir/caller.key" >"$key_dir/caller.pub"
wg pubkey <"$key_dir/provider.key" >"$key_dir/provider.pub"
for namespace in "$caller" "$provider"; do
  ip -n "$namespace" link add wg0 type wireguard
  ip -n "$namespace" link set wg0 up
done
ip -n "$caller" addr add 100.96.0.10/32 dev wg0
ip -n "$provider" addr add 100.96.0.20/32 dev wg0
ip -n "$caller" route add 100.96.0.20/32 dev wg0
ip -n "$provider" route add 100.96.0.10/32 dev wg0
ip netns exec "$caller" wg set wg0 private-key "$key_dir/caller.key" listen-port 51820 \
  peer "$(cat "$key_dir/provider.pub")" allowed-ips 100.96.0.20/32 endpoint 192.0.2.20:51820
ip netns exec "$provider" wg set wg0 private-key "$key_dir/provider.key" listen-port 51820 \
  peer "$(cat "$key_dir/caller.pub")" allowed-ips 100.96.0.10/32 endpoint 192.0.2.10:51820

printf '100.96.0.20 %s\n' "$authority" >"/etc/netns/$caller/hosts"
mkdir -p "$tmp_dir/objects"
ip netns exec "$provider" "$bun_bin" "$root_dir/integration/s3_fixture.ts" serve "$tmp_dir/objects" \
  >"$tmp_dir/s3.log" 2>&1 &
s3_pid=$!
for _ in $(seq 1 50); do
  ip netns exec "$caller" curl --noproxy '*' -sS --connect-timeout 1 \
    "http://100.96.0.20:9001/" >/dev/null 2>&1 && break
  sleep 0.1
done
kill -0 "$s3_pid" 2>/dev/null || {
  echo "FAIL: S3 fixture exited"
  sed -n '1,80p' "$tmp_dir/s3.log"
  exit 1
}

mapfile -t urls < <(
  ip netns exec "$caller" "$bun_bin" "$root_dir/integration/s3_fixture.ts" presign "$authority" "acceptance/object.txt"
)
[[ ${#urls[@]} -eq 2 ]]
[[ ${urls[0]} == http://${authority}:9001/* ]]
[[ ${urls[1]} == http://${authority}:9001/* ]]

payload="HN-502 overlay upload/download $run_id"
ip netns exec "$caller" curl --noproxy '*' -fsS -X PUT --data-binary "$payload" "${urls[0]}"
downloaded=$(ip netns exec "$caller" curl --noproxy '*' -fsS "${urls[1]}")
[[ "$downloaded" == "$payload" ]] || {
  echo "FAIL: downloaded object differs"
  exit 1
}

handshake=$(ip netns exec "$caller" wg show wg0 latest-handshakes | awk '{print $2}')
[[ ${handshake:-0} -ne 0 ]] || {
  echo "FAIL: WireGuard handshake missing"
  exit 1
}

tcp_port=25565
udp_port=19132
ip netns exec "$provider" python3 "$root_dir/integration/game_fixture.py" \
  serve tcp 100.96.0.20 "$tcp_port" >"$tmp_dir/tcp.log" 2>&1 &
tcp_pid=$!
ip netns exec "$provider" python3 "$root_dir/integration/game_fixture.py" \
  serve udp 100.96.0.20 "$udp_port" >"$tmp_dir/udp.log" 2>&1 &
udp_pid=$!
sleep 0.1
kill -0 "$tcp_pid" 2>/dev/null || {
  echo "FAIL: TCP fixture exited"
  sed -n '1,80p' "$tmp_dir/tcp.log"
  exit 1
}
kill -0 "$udp_pid" 2>/dev/null || {
  echo "FAIL: UDP fixture exited"
  sed -n '1,80p' "$tmp_dir/udp.log"
  exit 1
}

ip netns exec "$caller" python3 "$root_dir/integration/game_fixture.py" \
  call tcp 100.96.0.20 "$tcp_port" "tcp-game-$run_id"
ip netns exec "$caller" python3 "$root_dir/integration/game_fixture.py" \
  call udp 100.96.0.20 "$udp_port" "udp-game-$run_id"

# HN-706 release orchestration may restart a Launcher-supervised production
# agent while this real S3/TCP/UDP data plane remains live. Pass an executable
# hook as argv, never a shell command string, so argument boundaries survive.
if (( $# > 0 )); then
  "$@"
  downloaded=$(ip netns exec "$caller" curl --noproxy '*' -fsS "${urls[1]}")
  [[ "$downloaded" == "$payload" ]] || {
    echo "FAIL: downloaded object differs after recovery hook"
    exit 1
  }
  ip netns exec "$caller" python3 "$root_dir/integration/game_fixture.py" \
    call tcp 100.96.0.20 "$tcp_port" "tcp-game-recovered-$run_id"
  ip netns exec "$caller" python3 "$root_dir/integration/game_fixture.py" \
    call udp 100.96.0.20 "$udp_port" "udp-game-recovered-$run_id"
fi

# Model immediate Hub permission revocation at provider enforcement boundary.
# Production hub-net uses same exact overlay source/destination/protocol/port
# tuple and deletes matching conntrack state when applying this generation bump.
ip netns exec "$provider" nft -f - <<NFT
table inet hn706_revoked {
  chain input {
    type filter hook input priority -10; policy accept;
    ip saddr 100.96.0.10 ip daddr 100.96.0.20 tcp dport $tcp_port drop
    ip saddr 100.96.0.10 ip daddr 100.96.0.20 udp dport $udp_port drop
  }
}
NFT
if ip netns exec "$caller" python3 "$root_dir/integration/game_fixture.py" \
  call tcp 100.96.0.20 "$tcp_port" "revoked-tcp" 2>/dev/null; then
  echo "FAIL: TCP game share remained reachable after revocation"
  exit 1
fi
if ip netns exec "$caller" python3 "$root_dir/integration/game_fixture.py" \
  call udp 100.96.0.20 "$udp_port" "revoked-udp" 2>/dev/null; then
  echo "FAIL: UDP game share remained reachable after revocation"
  exit 1
fi

echo "PASS: S3, TCP, UDP cross WireGuard; TCP/UDP blocked after revocation"
