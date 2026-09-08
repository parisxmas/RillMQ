#!/bin/sh
# A failover as a client sees it, which is not how the other cluster checks
# look at one. They kill a node and then ask a question. This keeps a
# publisher and a consumer working across the moment, because both of them
# used to lose something.
#
#     sh test/handover.sh [first-port]
set -u
. "$(dirname "$0")/lib.sh"
A="${1:-9950}"
B=$((A + 1))
C=$((A + 2))
P="127.0.0.1:$A,127.0.0.1:$B,127.0.0.1:$C"
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

./rillmq "$A" "dir=$D/1" node=0 "peers=$P" >/dev/null 2>&1 & N1=$!; up "$A" "$N1" || exit 1
./rillmq "$B" "dir=$D/2" node=1 "peers=$P" >/dev/null 2>&1 & N2=$!; up "$B" "$N2" || exit 1
./rillmq "$C" "dir=$D/3" node=2 "peers=$P" >/dev/null 2>&1 & N3=$!; up "$C" "$N3" || exit 1
sleep 2

# `gamma` is node 0's, whatever the ports are: the owner is the hash of the
# name over the number of nodes. So everything below goes through node 1,
# which holds nothing but a stand-in for it.
OUT=$(python3 -c "
import socket, time, threading, subprocess
B = $B
Q = b'gamma'
got = []
def consume():
    c = socket.create_connection(('127.0.0.1', B)); c.settimeout(0.5)
    c.sendall(b'SUB ' + Q + b'\r\n'); buf = b''
    while True:
        try: buf += c.recv(65536)
        except Exception: continue
        while b'\r\n' in buf:
            line, rest = buf.split(b'\r\n', 1)
            if line.startswith(b'MSG'):
                parts = line.split(); n = int(parts[3])
                if len(rest) < n: break
                got.append(rest[:n].decode()); buf = rest[n:]
                c.sendall(b'ACK ' + Q + b' ' + parts[1] + b'\r\n')
            else: buf = rest
threading.Thread(target=consume, daemon=True).start()
said = []
def pub(body):
    s = socket.create_connection(('127.0.0.1', B)); s.settimeout(6)
    s.sendall(b'PUB ' + Q + b' %d\r\n%s' % (len(body), body.encode()))
    try: said.append(s.recv(200).decode().strip().split()[0])
    except Exception: said.append('(timeout)')
    finally: s.close()
time.sleep(1.2)
pub('one'); time.sleep(1.0)
subprocess.run(['kill', '-9', '$N1']); time.sleep(1.2)
pub('two'); time.sleep(1.2)
pub('three'); time.sleep(1.5)
print(','.join(said) + '|' + ','.join(got))
")

say "the publish that provokes the takeover is not the one it loses" "${OUT%%|*}" "+OK,+OK,+OK"
say "and the consumer reads through it without noticing" "${OUT##*|}" "one,two,three"

kill -9 "$N2" "$N3" 2>/dev/null
sleep 0.3
printf '\n%d of %d passed\n' "$PASS" "$((PASS + FAIL))"
[ "$FAIL" -eq 0 ]
