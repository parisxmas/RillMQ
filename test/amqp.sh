#!/bin/sh
# The acceptance test: RabbitMQ's own .NET client, unmodified, pointed at
# RillMQ. Nothing in it knows it is not talking to RabbitMQ.
#
#     sh test/amqp.sh [amqp-port] [native-port]
set -u
AMQP="${1:-5672}"
NATIVE="${2:-7998}"
DIR="$(mktemp -d)/rillmq"

./rillmq "$NATIVE" "dir=$DIR" "amqp=$AMQP" >/dev/null 2>&1 &
BROKER=$!
sleep 1

# Builds if it has to. The first run fetches RabbitMQ.Client from nuget.
( cd test/dotnet && dotnet run -- "$AMQP" )
RC=$?

kill -TERM "$BROKER" 2>/dev/null
wait "$BROKER" 2>/dev/null
rm -rf "$DIR"
exit $RC
