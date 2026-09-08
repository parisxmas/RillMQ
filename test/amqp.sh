#!/bin/sh
# The acceptance test: RabbitMQ's own .NET client, unmodified, pointed at
# RillMQ. Nothing in it knows it is not talking to RabbitMQ.
#
#     sh test/amqp.sh [amqp-port] [native-port]
set -u
. "$(dirname "$0")/lib.sh"
AMQP="${1:-5672}"
NATIVE="${2:-7998}"
DIR="$(mktemp -d)/rillmq"

./rillmq "$NATIVE" "dir=$DIR" "amqp=$AMQP" >/dev/null 2>&1 &
BROKER=$!
up "$NATIVE" "$BROKER" || exit 1
up "$AMQP" "$BROKER" || exit 1

# One exchange the .NET client cannot declare: RabbitMQ.Client 6 has no way to
# say `internal`, so the broker's own protocol says it instead. What is being
# tested is what the broker does with such an exchange, not who declared it.
printf 'XDECL shut-x fanout internal\r\nQUIT\r\n' | nc -w 2 127.0.0.1 "$NATIVE" >/dev/null

# Builds if it has to. The first run fetches RabbitMQ.Client from nuget.
( cd test/dotnet && dotnet run -- "$AMQP" )
RC=$?

kill -TERM "$BROKER" 2>/dev/null
wait "$BROKER" 2>/dev/null
rm -rf "$DIR"
exit $RC
