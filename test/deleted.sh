#!/bin/sh
# Does an exchange somebody deleted stay deleted across a restart?
#
# The routing journal replays what was declared and what was bound, in order,
# so it has to replay what was deleted or the broker helpfully brings it back.
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

kill -TERM "$BROKER" 2>/dev/null
sleep 0.3
printf '\n%d of %d passed\n' "$PASS" "$((PASS + FAIL))"
[ "$FAIL" -eq 0 ]
