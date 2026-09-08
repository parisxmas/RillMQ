#!/bin/sh
# A node that has fallen behind, and an election it can still win.
#
# A leader waits for one copy rather than for both, so a node that was away
# while messages were published has less of the queue than the node that
# acknowledged them. It used to be refused the leadership and stay refused —
# nothing copied it what it was missing — so the queue waited for the node
# that was ahead to be asked for it. Now it fetches the part of the journal it
# does not have and then holds the election it was going to hold anyway.
#
#     sh test/catchup.sh [first-port]
set -u
. "$(dirname "$0")/lib.sh"
A="${1:-9820}"
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

size() { wc -c < "$DIR/$1/alpha.log" 2>/dev/null | tr -d ' '; }

# A minute between heartbeats, so that every election in this file is one the
# test asked for. What is being measured is the catching up, not the racing.
node() { ./rillmq "$2" "dir=$DIR/$1" "node=$3" "peers=$P" beat=60000 >> "$DIR/$1.log" 2>&1 & }

node 1 "$A" 0
N1=$!
up "$A" "$N1" || exit 1
node 2 "$B" 1
N2=$!
up "$B" "$N2" || exit 1
node 3 "$C" 2
N3=$!
up "$C" "$N3" || exit 1

# `alpha` hashes to the third node, whatever the ports are.
ask "$A" 'PUB alpha 3\r\none' >/dev/null
ask "$A" 'PUB alpha 3\r\ntwo' >/dev/null
sleep 0.5
say "both copies have what was published" "$([ "$(size 1)" = "$(size 2)" ] && echo same || echo "$(size 1) vs $(size 2)")" "same"

kill -9 "$N1" 2>/dev/null
wait "$N1" 2>/dev/null
BEHIND=$(size 1)

ask "$B" 'PUB alpha 5\r\nthree' >/dev/null
ask "$B" 'PUB alpha 4\r\nfour' >/dev/null
sleep 0.5
say "a node that is away misses what arrives while it is" \
  "$([ "$BEHIND" -lt "$(size 2)" ] && echo behind || echo "$BEHIND vs $(size 2)")" "behind"

node 1 "$A" 0
N1=$!
up "$A" "$N1" || exit 1
say "and is still behind when it comes back" \
  "$([ "$(size 1)" -lt "$(size 2)" ] && echo behind || echo "$(size 1) vs $(size 2)")" "behind"

# The owner goes, and the node that is behind is the lowest-numbered one left,
# so it is the one that stands.
kill -9 "$N3" 2>/dev/null
wait "$N3" 2>/dev/null
# A link that has just gone is given a moment before it is tried again, so the
# very first request after a kill is refused whatever else is true. The
# failover suite waits here for the same reason.
sleep 2

say "the request that holds the election is answered" "$(ask "$A" 'PUB alpha 4\r\nfive' | cut -d' ' -f1)" "+OK"
sleep 0.5
say "the node that was behind is the one that leads" "$(ask "$A" 'WHO alpha\r\n')" "+WHO 0"
say "and the other survivor agrees" "$(ask "$B" 'WHO alpha\r\n')" "+WHO 0"
say "with everything in it, including what it had missed" "$(ask "$A" 'QSTAT alpha\r\n' | cut -d' ' -f2)" "5"

kill -9 "$N1" "$N2" 2>/dev/null
wait "$N1" "$N2" 2>/dev/null
rm -rf "$DIR"
printf '\n%s of %s passed\n' "$PASS" "$((PASS + FAIL))"
[ "$FAIL" -eq 0 ]
