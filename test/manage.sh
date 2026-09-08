#!/bin/sh
# The management pages, over HTTP.
#
#     sh test/manage.sh [first-port]
set -u
. "$(dirname "$0")/lib.sh"
A="${1:-9900}"
W=$((A + 1))
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
./rillmq "$A" "dir=$D/d" "users=$D/users" "manage=$W" >/dev/null 2>&1 &
N=$!
up "$A" "$N" || exit 1
up "$W" "$N" || exit 1

U="http://127.0.0.1:$W"
get() { curl -s -u alice:hunter2 "$U$1"; }
code() { curl -s -o /dev/null -w '%{http_code}' "$@"; }
post() { curl -s -o /dev/null -w '%{http_code}' -u alice:hunter2 -X POST -d "$2" "$U$1"; }

say "a page without a password is refused" "$(code "$U/")" "401"
say "and with one is not" "$(code -u alice:hunter2 "$U/")" "200"
say "a word that is wrong is refused" "$(code -u alice:nope "$U/")" "401"
say "the overview says nothing is there yet" "$(get /api/queues)" "[]"

printf 'AUTH alice hunter2\r\nPUB orders 5\r\nhelloPUB orders 5\r\nworld' | nc -w 2 127.0.0.1 "$A" >/dev/null
sleep 0.5
say "and then says what is" "$(get /api/queues)" '[{"name":"orders","ready":2,"inflight":0,"consumers":0,"published":2}]'
say "the page names the queue" "$(get / | grep -c '<code>orders</code>')" "1"

say "publishing from a form works" "$(post /publish 'queue=orders&body=from+the+page')" "303"
sleep 0.4
say "and the count went up" "$(get /api/queues | grep -o '"ready":3')" '"ready":3'

say "emptying a queue works" "$(post /drain 'queue=orders')" "303"
sleep 0.3
say "and it is empty" "$(get /api/queues | grep -o '"ready":0')" '"ready":0'

say "adding somebody works" "$(post /remember 'name=carol&word=w0rd&tag=monitoring')" "303"
say "and they can get in at once" "$(code -u carol:w0rd "$U/")" "200"
say "removing them works" "$(post /forget 'name=carol')" "303"
say "and they cannot get in after" "$(code -u carol:w0rd "$U/")" "401"

say "a page nobody wrote is a 404" "$(code -u alice:hunter2 "$U/nowhere")" "404"

kill "$N" 2>/dev/null
sleep 0.3
printf '\n%d of %d passed\n' "$PASS" "$((PASS + FAIL))"
[ "$FAIL" -eq 0 ]
