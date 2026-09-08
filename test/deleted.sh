#!/bin/sh
# What survives a restart and what does not, which is one question with several
# faces: a deleted exchange must stay deleted, a churned routing table must not
# grow without bound, and a queue a client asked not to keep must not be kept.
set -u
. "$(dirname "$0")/lib.sh"
A="${1:-9890}"
AMQP="${2:-5890}"
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

start() {
  ./rillmq "$A" "dir=$D/d" "amqp=$AMQP" >/dev/null 2>&1 &
  BROKER=$!
  up "$A" "$BROKER" || exit 1
  up "$AMQP" "$BROKER" || exit 1
}

client() { (cd test/dotnet && dotnet run --no-build -- "$AMQP" "$@" 2>&1 | tail -1); }
ask() { printf "$1" | nc -w 2 127.0.0.1 "$A" >/dev/null 2>&1; }

start
say "an exchange is declared, bound and deleted" "$(client xdel make orders-x)" "deleted"
say "and publishing to it is refused at once" "$(client xdel gone orders-x)" "404"

kill -TERM "$BROKER" 2>/dev/null
wait "$BROKER" 2>/dev/null
start
say "it is still gone after a restart" "$(client xdel gone orders-x)" "404"

# The routing journal used to grow a record per binding made and per binding
# taken away, with nothing ever shortening it. A table that changes as often as
# this one does is a file that only ever got bigger.
before=$(wc -c < "$D/d/_routes.log" 2>/dev/null || echo 0)
client churn 3000 >/dev/null
after=$(wc -c < "$D/d/_routes.log" 2>/dev/null || echo 0)
say "churning bindings does not grow the journal without bound" "$([ "$after" -lt 200000 ] && echo small || echo "$after")" "small"

kill -TERM "$BROKER" 2>/dev/null
wait "$BROKER" 2>/dev/null
start
say "and what is left after a restart is the table as it stood" "$(client unrouted churn-x k)" "came back"

# `durable` was read off the wire and ignored, so a client that asked for a
# temporary queue got a permanent one — the safe direction to be wrong in and
# still the wrong answer.
client transient make >/dev/null
say "only the durable one is on the disk" "$(ls "$D/d" | grep -cE '^(lasting|passing)-q\.log$')" "1"

kill -TERM "$BROKER" 2>/dev/null
wait "$BROKER" 2>/dev/null
start
say "and only it comes back" "$(client transient check)" "kept=1 passing=404"

# An exchange only other exchanges may publish to. The flag is in the routing
# journal's record for the exchange, in a third field older files do not have,
# so this asks whether it is still there after the file has been read again.
ask 'XDECL walled fanout internal\r\nQUIT\r\n' >/dev/null
say "a client may not publish to an internal exchange" "$(client innerpub walled)" "403"

kill -TERM "$BROKER" 2>/dev/null
wait "$BROKER" 2>/dev/null
start
say "and still may not after a restart" "$(client innerpub walled)" "403"

kill -TERM "$BROKER" 2>/dev/null
sleep 0.3
printf '\n%d of %d passed\n' "$PASS" "$((PASS + FAIL))"
[ "$FAIL" -eq 0 ]
