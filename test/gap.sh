#!/bin/sh
# A copy that was away. What it missed is not in the batches that come after, so
# appending those leaves a hole that looks like a copy, and a node holding one
# can be elected. It has to end up the same as the leader's, byte for byte.
#
#     sh test/gap.sh [first-port]
set -u
. "$(dirname "$0")/lib.sh"
A="${1:-9951}"
B=$((A + 1))
C=$((A + 2))
P="127.0.0.1:$A,127.0.0.1:$B,127.0.0.1:$C"
DIR="$(mktemp -d)"
PASS=0
FAIL=0

say() {
  if [ "$2" = "$3" ]; then
    printf 'ok   %s\n' "$1"; PASS=$((PASS + 1))
  else
    printf 'FAIL %s: wanted `%s`, got `%s`\n' "$1" "$3" "$2"; FAIL=$((FAIL + 1))
  fi
}

ask() {
  python3 -c "
import socket
s = socket.create_connection(('127.0.0.1', $1)); s.settimeout(6)
s.sendall(b'$2')
try: print(s.recv(400).decode().strip())
except Exception: print('(timeout)')
"
}

node() {
  ./rillmq "$1" "dir=$DIR/$2" "node=$2" "peers=$P" beat=300 >/dev/null 2>&1 &
  BROKER=$!
}

node "$A" 0; N0=$BROKER
node "$B" 1; N1=$BROKER
node "$C" 2; N2=$BROKER
up "$C" "$N2" || exit 1
sleep 1

# `alpha` hashes to the third node, whatever the ports are.
ask "$A" 'PUB alpha 3\r\none' >/dev/null
kill -9 "$N1"
wait "$N1" 2>/dev/null
ask "$A" 'PUB alpha 3\r\ntwo' >/dev/null
ask "$A" 'PUB alpha 5\r\nthree' >/dev/null

node "$B" 1; N1=$BROKER
up "$B" "$N1" || exit 1
sleep 1.5
# The first batch it sees is the one that finds the hole, and fills it.
ask "$A" 'PUB alpha 4\r\nfour' >/dev/null
sleep 0.5

say "a copy that was away ends up the same as the leader's" "$(cmp "$DIR/1/alpha.log" "$DIR/2/alpha.log" && echo same)" "same"

kill -9 "$N0" "$N1" "$N2" 2>/dev/null
wait 2>/dev/null
rm -rf "$DIR"
printf '\n%d of %d passed\n' "$PASS" "$((PASS + FAIL))"
[ "$FAIL" -eq 0 ]
