#!/bin/sh
# A publisher told to stop rather than told no.
#
# A broker with no room left answered individual publishes with an error, so a
# publisher found out one message at a time and had no way to be asked to
# pause. AMQP has one, and RabbitMQ.Client raises it as an event.
#
#     sh test/blocked.sh [native-port] [amqp-port]
set -u
. "$(dirname "$0")/lib.sh"
A="${1:-9740}"
AMQP="${2:-5740}"
D="$(mktemp -d)"
PASS=0
FAIL=0

say() {
  if [ "$2" = "$3" ]; then
    printf 'ok   %s\n' "$1"; PASS=$((PASS + 1))
  else
    printf 'FAIL %s: wanted `%s`, got `%s`\n' "$1" "$3" "$2"; FAIL=$((FAIL + 1))
  fi
}

# Small enough to reach by publishing, large enough that the broker is not
# already over it when it starts.
./rillmq "$A" "dir=$D/d" "amqp=$AMQP" mem=64 >/dev/null 2>&1 &
BROKER=$!
up "$A" "$BROKER" || exit 1
up "$AMQP" "$BROKER" || exit 1

client() { (cd test/dotnet && dotnet run --no-build -- "$AMQP" "$@" 2>&1 | tail -1); }

say "a publisher filling the broker is asked to stop" "$(client blocked fill)" "blocked: low on memory"

kill -TERM "$BROKER" 2>/dev/null
sleep 0.3
printf '\n%d of %d passed\n' "$PASS" "$((PASS + FAIL))"
[ "$FAIL" -eq 0 ]
