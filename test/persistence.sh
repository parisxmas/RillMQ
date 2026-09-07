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
  ./rillmq "$PORT" "$DIR" "${1:-5000}" >/dev/null 2>&1 &
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

rm -rf "$DIR"
printf '\n%s of %s passed\n' "$PASS" "$((PASS + FAIL))"
[ "$FAIL" -eq 0 ]
