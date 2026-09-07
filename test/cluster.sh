#!/bin/sh
# Two nodes, and a client that cannot tell.
#
#     sh test/cluster.sh [port-a] [port-b]
set -u
A="${1:-9101}"
B="${2:-9102}"
PEERS="127.0.0.1:$A,127.0.0.1:$B"
DIR="$(mktemp -d)"
PASS=0
FAIL=0

say() {
  if [ "$2" = "$3" ]; then
    printf 'ok   %s\n' "$1"; PASS=$((PASS + 1))
  else
    printf 'FAIL %s: wanted `%s`, got `%s`\n' "$1" "$3" "$2"; FAIL=$((FAIL + 1))
  fi
}

./rillmq "$A" "dir=$DIR/a" node=0 "peers=$PEERS" >/dev/null 2>&1 &
NA=$!
./rillmq "$B" "dir=$DIR/b" node=1 "peers=$PEERS" >/dev/null 2>&1 &
NB=$!
sleep 1

# Four names, hashed over two nodes: neither node should hold all of them.
for q in alpha beta gamma delta; do
  printf 'PUB %s 4\r\ntestQUIT\r\n' "$q" | nc -w 2 127.0.0.1 "$A" >/dev/null
done
sleep 1
HERE=$(printf 'QUEUES\r\nQUIT\r\n' | nc -w 2 127.0.0.1 "$B" | grep -c '^Q ')
say "the names are split between the nodes" "$([ "$HERE" -gt 0 ] && [ "$HERE" -lt 4 ] && echo split || echo "$HERE")" "split"

# Publishing to one node and consuming from the other, for a queue that lives
# on whichever of them the hash chose.
SEEN=$(python3 -c "
import socket, time
sub = socket.create_connection(('127.0.0.1', $A)); sub.settimeout(5)
sub.sendall(b'SUB alpha\r\n'); time.sleep(0.5); sub.recv(65536)
pub = socket.create_connection(('127.0.0.1', $B)); pub.settimeout(5)
for i in range(3):
    b = ('across %d' % i).encode()
    pub.sendall(b'PUB alpha ' + str(len(b)).encode() + b'\r\n' + b)
time.sleep(1.0)
got = sub.recv(65536).decode()
print(got.count('MSG '))
sub.close(); pub.close()
")
say "a message crosses from one node to the other" "$SEEN" "3"

# And an acknowledgement goes back the same way.
ACKED=$(python3 -c "
import socket, time
sub = socket.create_connection(('127.0.0.1', $A)); sub.settimeout(5)
sub.sendall(b'SUB beta\r\n'); time.sleep(0.5)
got = sub.recv(65536).decode()
tag = [l for l in got.split(chr(13) + chr(10)) if l.startswith('MSG ')]
if not tag: print('(nothing to acknowledge)')
else:
    sub.sendall(('ACK beta %s\r\n' % tag[0].split(' ')[1]).encode())
    time.sleep(0.5)
    print(sub.recv(200).decode().strip())
sub.close()
")
say "and an acknowledgement goes back the same way" "$ACKED" "+OK"

# -- the copy, and what happens when a node goes ------------------------------------

# Whatever node holds `alpha`, the other one keeps a copy of its journal, and
# the copy is the same bytes: it is the records as they were written, sent on.
sleep 1
SAME=$(cmp -s "$DIR/a/alpha.log" "$DIR/b/alpha.log" && echo same || echo different)
say "both nodes hold the same journal for a queue" "$SAME" "same"

# A publisher whose queue is on a node that has gone is told so rather than
# left waiting, and the queue works again when the node comes back.
kill -9 "$NB" 2>/dev/null
wait "$NB" 2>/dev/null
sleep 2
OUT=$(python3 -c "
import socket
s = socket.create_connection(('127.0.0.1', $A)); s.settimeout(6)
s.sendall(b'PUB alpha 4\r\ndown')
try: print(s.recv(200).decode().strip())
except Exception: print('(no answer)')
")
say "a queue whose node has gone says so" "$OUT" "-ERR the node holding that queue cannot be reached"

./rillmq "$B" "dir=$DIR/b" node=1 "peers=$PEERS" >/dev/null 2>&1 &
NB=$!
sleep 2
BACK=$(python3 -c "
import socket
s = socket.create_connection(('127.0.0.1', $A)); s.settimeout(6)
s.sendall(b'PUB alpha 4\r\nback')
try: print(s.recv(200).decode().strip()[:3])
except Exception: print('(no answer)')
")
say "and works again when it comes back" "$BACK" "+OK"

kill -TERM "$NA" "$NB" 2>/dev/null
wait "$NA" "$NB" 2>/dev/null
rm -rf "$DIR"
printf '\n%s of %s passed\n' "$PASS" "$((PASS + FAIL))"
[ "$FAIL" -eq 0 ]
