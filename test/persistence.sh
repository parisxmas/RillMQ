#!/bin/sh
# What a journal is for: the broker stops and the messages do not.
#
# The client tests are one connection asking questions; these need the broker
# itself to end, so they are a shell script.
#
#     sh test/persistence.sh [port]
set -u
PORT="${1:-7999}"
DIR="$(mktemp -d)/rillmq"
PASS=0
FAIL=0

say() {
  if [ "$2" = "$3" ]; then
    printf 'ok   %s\n' "$1"
    PASS=$((PASS + 1))
  else
    printf 'FAIL %s: wanted `%s`, got `%s`\n' "$1" "$3" "$2"
    FAIL=$((FAIL + 1))
  fi
}

start() {
  ./rillmq "$PORT" "dir=$DIR" "ack=${1:-5000}" >/dev/null 2>&1 &
  BROKER=$!
  sleep 1
}

stop() { kill -TERM "$BROKER" 2>/dev/null; wait "$BROKER" 2>/dev/null; }
crash() { kill -9 "$BROKER" 2>/dev/null; wait "$BROKER" 2>/dev/null; }
ask() { printf "$1" | nc -w 2 127.0.0.1 "$PORT"; }
stats() { ask 'STATS\r\nQUIT\r\n' | head -1 | tr -d '\r'; }

start
ask 'PUB orders 5\r\nalphaPUB orders 4\r\nbetaPUB orders 5\r\ngammaQUIT\r\n' >/dev/null
say "three published" "$(stats)" "+STATS 1 3 0"
stop

start
say "three still there after a clean stop" "$(stats)" "+STATS 1 3 0"
# The subscription and the acknowledgement have to be the same connection:
# a consumer that hangs up gives back whatever it was holding, so an `ACK`
# arriving on a fresh connection would find nothing to acknowledge.
(printf 'SUB orders\r\n'; sleep 1; printf 'ACK orders 1\r\n'; sleep 1) | nc -w 4 127.0.0.1 "$PORT" >/dev/null
say "one acknowledged" "$(stats)" "+STATS 1 2 0"
stop

start
say "the acknowledged one did not come back" "$(stats)" "+STATS 1 2 0"
BODIES=$( (printf 'SUB orders\r\n'; sleep 1) | nc -w 2 127.0.0.1 "$PORT" | sed 's/MSG [0-9]* orders [0-9]*//g' | tr -d '\r\n' | sed 's/^+OK//' )
say "and the other two still have their bodies" "$BODIES" "betagamma"
crash

start
say "a hard kill loses nothing that was answered" "$(stats)" "+STATS 1 2 0"
stop

printf 'P\011\000\000' >> "$DIR/orders.log"      # half a record, as a power cut leaves
start
say "half a record at the end is not read" "$(stats)" "+STATS 1 2 0"
BYTES=$(wc -c < "$DIR/orders.log" | tr -d ' ')
stop
start
say "and the file was cut back to where it made sense" "$(wc -c < "$DIR/orders.log" | tr -d ' ')" "$BYTES"
stop

ask 'PING\r\n' >/dev/null 2>&1                   # nothing listening now
start
ask 'PUB other 2\r\nhiQUIT\r\n' >/dev/null
say "a second queue gets its own journal" "$(ls "$DIR" | sort | tr '\n' ' ')" "orders.log other.log "
stop
start
say "and both come back" "$(stats)" "+STATS 2 3 0"
stop

# -- rewriting a journal that is mostly acknowledgements ---------------------------

start 60000
./rillmq-compact "$PORT" bulk 6000 5000 >/dev/null
sleep 2
BULK=$(wc -c < "$DIR/bulk.log" | tr -d ' ')
# Ready and in flight together: a hard kill gives the in-flight ones back as
# ready, so counting only what is ready would be counting a different thing on
# either side of the crash.
LEFT=$(stats | awk '{print $3 + $4}')
say "the journal was rewritten to about what is left" "$([ "$BULK" -lt 40000 ] && echo small || echo "$BULK")" "small"
crash

start 60000
say "and a hard kill after a rewrite loses nothing" "$(stats | awk '{print $3 + $4}')" "$LEFT"
stop

# -- who is allowed to stay, and how many ------------------------------------------

./rillmq "$PORT" idle=2 conns=4 >/dev/null 2>&1 &
BROKER=$!
sleep 1
QUIET=$( (sleep 5) | nc 127.0.0.1 "$PORT" | tr -d '\r\n' )
say "a connection that says nothing is let go" "$QUIET" "-ERR said nothing for 2 seconds"
SUBBED=$( (printf 'SUB q\r\n'; sleep 4) | nc -w 6 127.0.0.1 "$PORT" | tr -d '\r\n' )
say "a subscriber may say nothing for as long as it likes" "$SUBBED" "+OK"
NTH=$(python3 -c "
import socket
socks=[]
last=''
for i in range(5):
    s=socket.create_connection(('127.0.0.1', $PORT)); s.settimeout(2)
    s.sendall(b'SUB k\r\n')
    try: last=s.recv(200).decode().strip()
    except Exception: last='(nothing)'
    socks.append(s)
print(last)
")
say "the one past the limit is turned away" "$NTH" "-ERR too many connections"
stop

rm -rf "$DIR"
printf '\n%s of %s passed\n' "$PASS" "$((PASS + FAIL))"
[ "$FAIL" -eq 0 ]
