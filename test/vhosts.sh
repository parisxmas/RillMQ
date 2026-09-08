#!/bin/sh
# Virtual hosts, which are a namespace and a permission and nothing else.
#
#     sh test/vhosts.sh [first-port]
set -u
. "$(dirname "$0")/lib.sh"
A="${1:-9840}"
AMQP=$((A + 1))
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

./rillmq passwd "$D/users" alice hunter2 >/dev/null
./rillmq passwd "$D/users" carol c4rol >/dev/null
# Carol may open the one host she is given and no other. The field is on the
# end of her line, so every line written before there was more than one host
# goes on meaning everywhere.
# Carol may open the one host she is given and no other, and there she may
# only read. Both fields are on the end of her line, so every line written
# before there was more than one host or more than one right goes on meaning
# everywhere and everything.
awk -F: 'BEGIN { OFS = ":" } $1 == "carol" { print $0 ":/:r"; next } { print }' "$D/users" > "$D/users.new"
mv "$D/users.new" "$D/users"

./rillmq "$A" "dir=$D/d" "users=$D/users" "amqp=$AMQP" "vhosts=/,prod" >/dev/null 2>&1 &
N=$!
up "$A" "$N" || exit 1
up "$AMQP" "$N" || exit 1

client() { (cd test/dotnet && dotnet run --no-build -- "$AMQP" "$@" 2>&1 | tail -1); }

say "the host a broker was told about opens" "$(RILLMQ_USER=alice RILLMQ_PASS=hunter2 client vhost open prod)" "in"
say "and one it was not is refused" "$(RILLMQ_USER=alice RILLMQ_PASS=hunter2 client vhost open nowhere)" "out"
say "somebody who may open it, does" "$(RILLMQ_USER=carol RILLMQ_PASS=c4rol client vhost open /)" "in"
say "and somebody who may not, does not" "$(RILLMQ_USER=carol RILLMQ_PASS=c4rol client vhost open prod)" "out"

say "one queue name in two hosts is two queues" \
  "$(RILLMQ_USER=alice RILLMQ_PASS=hunter2 client vhost split prod)" "there=(nothing) home=home"
say "and the name a client gets back is the one it gave" \
  "$(RILLMQ_USER=alice RILLMQ_PASS=hunter2 client vhost name prod)" "vh-named"
# Alice declares it; Carol may read it and nothing else.
RILLMQ_USER=alice RILLMQ_PASS=hunter2 client vhost may / >/dev/null
say "somebody with every right has every right" \
  "$(RILLMQ_USER=alice RILLMQ_PASS=hunter2 client vhost may /)" "declare=yes publish=yes consume=yes"
say "and somebody who may only read, only reads" \
  "$(RILLMQ_USER=carol RILLMQ_PASS=c4rol client vhost may /)" "declare=403 publish=403 consume=yes"

say "the queue in the other host has a file of its own" "$(ls "$D/d" | grep -c '^prod')" "2"
say "and the one at home is named as it always was" "$(ls "$D/d" | grep -c '^vh-shared.log$')" "1"

kill "$N" 2>/dev/null
wait "$N" 2>/dev/null
rm -rf "$D"
printf '\n%d of %d passed\n' "$PASS" "$((PASS + FAIL))"
[ "$FAIL" -eq 0 ]
