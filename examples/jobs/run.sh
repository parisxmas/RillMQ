#!/bin/sh
# A broker, two workers and a producer, from the repository's root:
#
#     sh examples/jobs/run.sh
#
# Wants `pika` (`pip install pika`) and a built `./rillmq`.
set -u
. "$(dirname "$0")/../../test/lib.sh"
HERE="$(dirname "$0")"
PORT="${PORT:-9871}"
AMQP_PORT="${AMQP_PORT:-5682}"
export AMQP_PORT
PY="${PYTHON:-python3}"
D=$(mktemp -d)
OUT="$D/out"

"$PY" -c 'import pika' 2>/dev/null || { echo 'wants pika: pip install pika' >&2; exit 1; }

./rillmq "$PORT" "dir=$D/data" "amqp=$AMQP_PORT" >/dev/null 2>&1 &
BROKER=$!
up "$PORT" "$BROKER" || exit 1

"$PY" "$HERE/worker.py" one >>"$OUT" 2>&1 &
ONE=$!
"$PY" "$HERE/worker.py" two >>"$OUT" 2>&1 &
TWO=$!
"$PY" "$HERE/producer.py" 10 >>"$OUT" 2>&1

# Ten jobs, the crash twice, the producer's line and the dying worker's.
i=0
while [ "$(grep -c '' "$OUT")" -lt 14 ] && [ "$i" -lt 100 ]; do
  sleep 0.1
  i=$((i + 1))
done
kill "$ONE" "$TWO" 2>/dev/null
kill -TERM "$BROKER" 2>/dev/null
wait 2>/dev/null
cat "$OUT"
rm -rf "$D"
