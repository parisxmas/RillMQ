#!/bin/sh
# A client that does not keep up. The broker must go on reading it, must not
# grow without limit on its account, and must not keep anything once it leaves.
#
#     sh test/slow.sh [native-port] [amqp-port]
set -u
. "$(dirname "$0")/lib.sh"
A="${1:-9893}"
AMQP="${2:-5893}"
PASS=0
FAIL=0
PY="python3 -I test/slow.py"

say() {
  if [ "$2" = "$3" ]; then
    printf 'ok   %s\n' "$1"; PASS=$((PASS + 1))
  else
    printf 'FAIL %s: wanted %s, got %s\n' "$1" "$3" "$2"; FAIL=$((FAIL + 1))
  fi
}

start() {
  ./rillmq "$A" "amqp=$AMQP" "$@" >/dev/null 2>&1 &
  BROKER=$!
  up "$A" "$BROKER" || exit 1
  up "$AMQP" "$BROKER" || exit 1
}

stop() {
  kill -TERM "$BROKER" 2>/dev/null
  wait "$BROKER" 2>/dev/null
}

kb() { ps -o rss= -p "$BROKER" | tr -d ' '; }
fds() { lsof -nP -p "$BROKER" 2>/dev/null | wc -l | tr -d ' '; }

# Unanswered messages come back after `ack=`, and this is about the deadlock
# and not about that, so it is set well past the length of the test. The
# deadlock came and went with timing, which is why the first check is repeated.
start ack=600000
round=0
while [ "$round" -lt 5 ]; do
  say "a window of zero is read to the end, run $round" "$($PY drain "$AMQP" 50000 0)" "drained"
  stop
  start ack=600000
  round=$((round + 1))
done
say "a window larger than the socket is too" "$($PY drain "$AMQP" 50000 32768)" "drained"
say "a client that acknowledges in batches is fed until it has one" "$($PY batches "$AMQP" 50000 5000)" "drained"
stop

# One connection first, because the strands that look after connections are
# shared and each has a timer of its own, which on Linux is a descriptor.
start
$PY closes "$AMQP" 1 >/dev/null
sleep 1
before=$(fds)
$PY closes "$AMQP" 200 >/dev/null
sleep 1
say "a connection that closes gives back its descriptor" "$(fds)" "$before"

# Redelivery goes on behind a client that has stopped, and what is owed to it
# must not grow with every round of that. Forty-odd seconds is eight rounds.
$PY ignore "$AMQP" 50000 >/dev/null &
IGNORING=$!
sleep 10
early=$(kb)
sleep 35
late=$(kb)
say "memory does not grow behind a client that has stopped" "$((late - early < 20000))" "1"
say "the broker answers around it" "$(printf 'PING\r\nQUIT\r\n' | nc -w 2 127.0.0.1 "$A" | head -1 | tr -d '\r')" "+PONG"

# With that client still connected and nothing able to wake its writer but the
# broker itself.
t0=$(date +%s)
stop
say "and it does not stop the broker stopping" "$(($(date +%s) - t0 < 10))" "1"
kill "$IGNORING" 2>/dev/null
wait "$IGNORING" 2>/dev/null

start
say "a client that only asks is cut off in the end" "$($PY flood "$AMQP" 120)" "cut off"
say "and costs what the limit says" "$(($(kb) < 400000))" "1"
stop

# A client that stops reading and stays connected, and a client that is only
# slow, which is the one that must not be mistaken for it.
start stall=2000
say "a write that goes nowhere ends the connection" "$($PY gone "$AMQP" 50000)" "cut off"
stop
start stall=2000 ack=600000
say "a client that is only slow keeps it" "$($PY dawdle "$AMQP" 200000 8)" "still connected"
stop

# The native protocol has no way to ask for a window, so the broker is told.
start prefetch=0 ack=600000
say "the native protocol is read to the end with no window" "$($PY native "$A" 50000)" "drained"
stop

start
say "a native client that only asks is cut off in the end" "$($PY pings "$A" 120)" "cut off"
say "and costs what the limit says" "$(($(kb) < 400000))" "1"
stop

printf '\n%d passed, %d failed\n' "$PASS" "$FAIL"
[ "$FAIL" -eq 0 ]
