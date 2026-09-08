#!/bin/sh
# TLS and passwords, over both protocols.
#
#     sh test/secure.sh [first-port]
set -u
. "$(dirname "$0")/lib.sh"
A="${1:-9880}"
TLSP=$((A + 1))
AMQP=$((A + 2))
AMQPS=$((A + 3))
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

openssl req -x509 -newkey rsa:2048 -keyout "$D/key.pem" -out "$D/cert.pem" \
  -days 30 -nodes -subj "/CN=localhost" \
  -addext "subjectAltName=DNS:localhost,IP:127.0.0.1" >/dev/null 2>&1

./rillmq passwd "$D/users" alice hunter2 >/dev/null
./rillmq passwd "$D/users" bob s3cret monitoring >/dev/null

./rillmq "$A" "dir=$D/d" "users=$D/users" "tls=$TLSP" "amqp=$AMQP" "amqps=$AMQPS" \
  "cert=$D/cert.pem" "key=$D/key.pem" >/dev/null 2>&1 &
N=$!
up "$A" "$N" || exit 1
up "$TLSP" "$N" || exit 1
up "$AMQP" "$N" || exit 1
up "$AMQPS" "$N" || exit 1

plain() { printf "$1" | nc -w 3 127.0.0.1 "$A" | tr -d '\r' | tr '\n' '|'; }
wrapped() { printf "$1" | openssl s_client -quiet -connect "127.0.0.1:$TLSP" 2>/dev/null | tr -d '\r' | tr '\n' '|'; }
dotnet_check() { (cd test/dotnet && dotnet run --no-build -- "$@" 2>&1 | tail -1); }

say "a word that is right gets in" "$(plain 'AUTH alice hunter2\r\nPING\r\n')" "+OK|+PONG|"
say "a word that is wrong does not" "$(plain 'AUTH alice nope\r\nPING\r\n')" "-ERR no|+PONG|"
say "and nothing works before signing in" "$(plain 'STATS\r\n')" "-ERR say AUTH first|"
say "a name nobody has is refused" "$(plain 'AUTH nobody x\r\nSTATS\r\n')" "-ERR no|-ERR say AUTH first|"
say "the same over tls" "$(wrapped 'AUTH bob s3cret\r\nPING\r\nQUIT\r\n')" "+OK|+PONG|+OK|"
say "and tls refuses the wrong word too" "$(wrapped 'AUTH bob nope\r\nSTATS\r\nQUIT\r\n')" "-ERR no|-ERR say AUTH first|+OK|"

say "amqp lets the right pair in" "$(dotnet_check "$AMQP" auth alice hunter2)" "in"
say "and keeps the wrong pair out" "$(dotnet_check "$AMQP" auth alice nope)" "out"
say "amqps carries a message" "$(RILLMQ_USER=alice RILLMQ_PASS=hunter2 dotnet_check "$AMQPS" tls)" "over tls"

kill "$N" 2>/dev/null
sleep 0.3
printf '\n%d of %d passed\n' "$PASS" "$((PASS + FAIL))"
[ "$FAIL" -eq 0 ]
