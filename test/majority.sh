#!/bin/sh
# Three nodes, and what a leader will and will not confirm.
#
#     sh test/majority.sh [first-port]
set -u
. "$(dirname "$0")/lib.sh"
A="${1:-9801}"
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

# `gamma` is node 0's, whatever the ports are: the owner is the hash of the
# name over the number of nodes, and neither depends on where anybody is.
pub() {
  python3 -c "
import socket
s = socket.create_connection(('127.0.0.1', $A)); s.settimeout(10)
s.sendall(b'PUB gamma 2\r\nhi')
try: print(s.recv(300).decode().strip())
except Exception: print('(timeout)')
"
}

./rillmq "$A" "dir=$DIR/1" node=0 "peers=$P" >/dev/null 2>&1 &
N1=$!
up "$A" "$N1" || exit 1
say "a leader alone will not confirm" "$(pub)" "-ERR not enough of the cluster has it"

./rillmq "$B" "dir=$DIR/2" node=1 "peers=$P" >/dev/null 2>&1 &
N2=$!
up "$B" "$N2" || exit 1
sleep 2
say "with two of three it will" "$(pub | cut -d' ' -f1)" "+OK"

./rillmq "$C" "dir=$DIR/3" node=2 "peers=$P" >/dev/null 2>&1 &
N3=$!
up "$C" "$N3" || exit 1
sleep 2
say "and with all three" "$(pub | cut -d' ' -f1)" "+OK"

kill -9 "$N2" 2>/dev/null
wait "$N2" 2>/dev/null
sleep 2
say "one of three going changes nothing" "$(pub | cut -d' ' -f1)" "+OK"

kill -9 "$N3" 2>/dev/null
wait "$N3" 2>/dev/null
sleep 2
say "two of three going stops it" "$(pub)" "-ERR not enough of the cluster has it"

kill -TERM "$N1" 2>/dev/null
wait "$N1" 2>/dev/null
rm -rf "$DIR"
printf '\n%s of %s passed\n' "$PASS" "$((PASS + FAIL))"
[ "$FAIL" -eq 0 ]
