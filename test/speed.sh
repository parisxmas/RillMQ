#!/bin/sh
# Every other check here says whether the broker is right. 0.2.4 was right, and
# twenty-five times too slow, and all of them passed. The floor is far below what
# this does on the slowest runner, so that it only ever means something broke.
#
#     sh test/speed.sh [port] [floor]
set -u
. "$(dirname "$0")/lib.sh"
PORT="${1:-9981}"
FLOOR="${2:-40000}"

./rillmq "$PORT" >/dev/null 2>&1 &
BROKER=$!
up "$PORT" "$BROKER" || exit 1

OUT=$(./rillmq-bench "$PORT" 100000 64)
kill -TERM "$BROKER" 2>/dev/null
wait "$BROKER" 2>/dev/null
printf '%s\n' "$OUT"

FAIL=0
for rate in $(printf '%s\n' "$OUT" | sed -E 's/.* ([0-9]+)\/sec/\1/'); do
  if [ "$rate" -lt "$FLOOR" ]; then
    printf 'FAIL %s/sec is under the floor of %s\n' "$rate" "$FLOOR"
    FAIL=1
  fi
done
[ "$FAIL" -eq 0 ] && printf 'ok   faster than the floor\n'
exit "$FAIL"
