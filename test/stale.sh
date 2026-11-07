#!/bin/sh
# A leader that was replaced while it could not answer, and has woken up. It is
# frozen rather than killed, because a killed one comes back with nothing in
# memory, and this one comes back believing what it believed.
#
#     sh test/stale.sh [first-port]
set -u
. "$(dirname "$0")/lib.sh"
A="${1:-9921}"
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
s = socket.create_connection(('127.0.0.1', $1)); s.settimeout(8)
s.sendall(b'$2')
try: print(s.recv(400).decode().strip())
except Exception: print('(timeout)')
"
}

./rillmq "$A" "dir=$DIR/1" node=0 "peers=$P" beat=300 >/dev/null 2>&1 &
N1=$!
up "$A" "$N1" || exit 1
./rillmq "$B" "dir=$DIR/2" node=1 "peers=$P" beat=300 >/dev/null 2>&1 &
N2=$!
./rillmq "$C" "dir=$DIR/3" node=2 "peers=$P" beat=300 >/dev/null 2>&1 &
N3=$!
sleep 1.2

# `alpha` hashes to the third node, whatever the ports are.
ask "$A" 'PUB alpha 3\r\none' >/dev/null
ask "$A" 'PUB alpha 3\r\ntwo' >/dev/null

kill -STOP "$N3"
sleep 1.5
ask "$A" 'PUB alpha 5\r\nthree' >/dev/null
sleep 0.5
say "the others have moved on" "$(ask "$A" 'PUB alpha 4\r\nfour' | cut -d' ' -f1)" "+OK"

kill -CONT "$N3"
sleep 0.4

# Told no by everybody it asked, so it has not got a majority, and a message it
# calls safe is one the cluster carrying on does not have.
say "a leader that was replaced cannot confirm anything" "$(ask "$C" 'PUB alpha 4\r\nfive' | cut -c1-4)" "-ERR"

kill -9 "$N1" "$N2" "$N3" 2>/dev/null
wait 2>/dev/null
printf '\n%d of %d passed\n' "$PASS" "$((PASS + FAIL))"
[ "$FAIL" -eq 0 ]
