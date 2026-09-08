#!/bin/sh
# Three nodes, and the one holding a queue is killed with nobody watching.
#
#     sh test/failover.sh [first-port]
set -u
. "$(dirname "$0")/lib.sh"
A="${1:-9901}"
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
s = socket.create_connection(('127.0.0.1', $1)); s.settimeout(12)
s.sendall(b'$2')
try: print(s.recv(400).decode().strip())
except Exception: print('(timeout)')
"
}

./rillmq "$A" "dir=$DIR/1" node=0 "peers=$P" > "$DIR/1.log" 2>&1 &
N1=$!
up "$A" "$N1" || exit 1
./rillmq "$B" "dir=$DIR/2" node=1 "peers=$P" > "$DIR/2.log" 2>&1 &
N2=$!
./rillmq "$C" "dir=$DIR/3" node=2 "peers=$P" > "$DIR/3.log" 2>&1 &
N3=$!
sleep 1.5

# `alpha` hashes to the third node, whatever the ports are.
ask "$A" 'PUB alpha 3\r\none' >/dev/null
say "a queue on another node takes messages" "$(ask "$A" 'PUB alpha 3\r\ntwo' | cut -d' ' -f1)" "+OK"
say "and nobody has been elected to anything" "$(ask "$A" 'WHO alpha\r\n')" "+WHO -1"

kill -9 "$N3" 2>/dev/null
wait "$N3" 2>/dev/null
sleep 2

# The first request after the node has gone is the one that holds the election;
# it is refused, and whoever asked asks again.
ask "$A" 'PUB alpha 5\r\nthree' >/dev/null
sleep 1
say "a queue comes back without anyone deciding it should" "$(ask "$A" 'PUB alpha 4\r\nfour' | cut -d' ' -f1)" "+OK"
say "and the node that took it says so" "$(ask "$A" 'WHO alpha\r\n')" "+WHO 0"
say "as does the one that voted for it" "$(ask "$B" 'WHO alpha\r\n')" "+WHO 0"
say "with what was published before still in it" "$(ask "$A" 'QSTAT alpha\r\n' | cut -d' ' -f2)" "3"

# A client on the other node finds the new leader rather than the old one.
say "and the other node finds it too" "$(ask "$B" 'PUB alpha 4\r\nfive' | cut -d' ' -f1)" "+OK"

kill -9 "$N1" "$N2" 2>/dev/null
wait "$N1" "$N2" 2>/dev/null
rm -rf "$DIR"
printf '\n%s of %s passed\n' "$PASS" "$((PASS + FAIL))"
[ "$FAIL" -eq 0 ]
