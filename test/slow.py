#!/usr/bin/env python3
"""A client too simple to hide a broker's mistakes.

    slow.py drain <port> <messages> <window>    read one, ack one, on one thread
    slow.py batches <port> <messages> <size>    read that many, then ack them all
    slow.py beside <port> <messages>            drain while others come and go
    slow.py closes <port> <count>               open and politely close
    slow.py ignore <port> <messages>            consume, then never read again
    slow.py flood <port> <seconds>              ask, and never read an answer
    slow.py gone <port> <messages>              ignore, then say whether it was cut off
    slow.py dawdle <port> <messages> <seconds>  read, but slowly
    slow.py behind <port> <messages>            publish beside a client that stopped
    slow.py native <port> <messages>            drain, on RillMQ's own protocol
    slow.py pings <port> <seconds>              flood, on RillMQ's own protocol

RabbitMQ's .NET client cannot be used for this, because it reads on a thread of
its own: the socket keeps emptying whatever the rest of the program is doing. A
client that reads and writes on one thread is the one that parks in a write
while the broker parks in a write to it. Every mode prints one line. With
WRAPPED set in the environment the AMQP modes speak TLS.
"""
import os, socket, ssl, struct, sys, threading, time

STALL = 20
BODY = b"x" * 64


def short(s):
    b = s.encode()
    return bytes([len(b)]) + b


def frame(kind, chan, payload):
    return struct.pack(">BHI", kind, chan, len(payload)) + payload + b"\xce"


def method(chan, cls, mth, args=b""):
    return frame(1, chan, struct.pack(">HH", cls, mth) + args)


def declare(name, passive=False):
    return method(1, 50, 10, b"\0\0" + short(name) + bytes([1 if passive else 0]) + b"\0\0\0\0")


def publish(key):
    return (method(1, 60, 40, b"\0\0" + short("") + short(key) + b"\0")
            + frame(2, 1, struct.pack(">HHQH", 60, 0, len(BODY), 0))
            + frame(3, 1, BODY))


class Conn:
    def __init__(self, port):
        self.s = socket.create_connection(("127.0.0.1", port))
        if os.environ.get("WRAPPED"):
            wrap = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            wrap.check_hostname = False
            wrap.verify_mode = ssl.CERT_NONE
            self.s = wrap.wrap_socket(self.s)
        self.s.settimeout(STALL)
        self.buf = b""
        self.s.sendall(b"AMQP\0\0\x09\x01")
        self.expect(10, 10)
        auth = b"\0guest\0guest"
        self.s.sendall(method(0, 10, 11, b"\0\0\0\0" + short("PLAIN")
                              + struct.pack(">I", len(auth)) + auth + short("en_US")))
        tune = self.expect(10, 30)
        self.s.sendall(method(0, 10, 31, tune[:8]))
        self.s.sendall(method(0, 10, 40, short("/") + short("") + b"\0"))
        self.expect(10, 41)
        self.s.sendall(method(1, 20, 10, short("")))
        self.expect(20, 11)

    def frame(self):
        while True:
            if len(self.buf) >= 7:
                kind, _, size = struct.unpack(">BHI", self.buf[:7])
                if len(self.buf) >= size + 8:
                    payload = self.buf[7:7 + size]
                    self.buf = self.buf[size + 8:]
                    return kind, payload
            more = self.s.recv(1 << 16)
            if not more:
                raise ConnectionError("closed")
            self.buf += more

    def expect(self, cls, mth):
        while True:
            kind, p = self.frame()
            if kind == 1 and struct.unpack(">HH", p[:4]) == (cls, mth):
                return p[4:]

    def ready(self, queue):
        self.s.sendall(declare(queue, True))
        ok = self.expect(50, 11)
        return struct.unpack(">I", ok[1 + ok[0]:5 + ok[0]])[0]

    def close(self):
        self.s.sendall(method(0, 10, 50, struct.pack(">HH", 200, 0) + short("bye") + struct.pack(">HH", 0, 0)))
        self.expect(10, 51)
        self.s.close()

    def consume(self, queue, window):
        self.s.sendall(method(1, 60, 10, struct.pack(">IHB", 0, window, 0)))
        self.expect(60, 11)
        self.s.sendall(method(1, 60, 20, b"\0\0" + short(queue) + short("c") + b"\0\0\0\0\0"))
        self.expect(60, 21)


def tell(port, queue):
    """What a stuck run looked like, on stderr, because it may only happen on a
    machine nobody can log in to."""
    os.system(f"(ss -tn 2>/dev/null || netstat -an -p tcp) | grep -E '[.:]{port}\\b' >&2")
    try:
        c = Conn(port)
        c.s.settimeout(5)
        print(f"the queue says {c.ready(queue)} left", file=sys.stderr)
    except Exception as e:
        print(f"the broker did not answer: {e!r}", file=sys.stderr)


def fill(port, queue, n):
    c = Conn(port)
    c.s.sendall(declare(queue))
    c.expect(50, 11)
    for start in range(0, n, 1000):
        c.s.sendall(b"".join(publish(queue) for _ in range(min(1000, n - start))))
    while c.ready(queue) < n:
        time.sleep(0.05)
    c.close()


def drain(port, n, window):
    queue = f"drain-{window}"
    fill(port, queue, n)
    c = Conn(port)
    c.consume(queue, window)
    done = 0
    try:
        while done < n:
            kind, p = c.frame()
            if kind == 1 and p[:4] == struct.pack(">HH", 60, 60):
                at = 5 + p[4]
                c.s.sendall(method(1, 60, 80, p[at:at + 8] + b"\0"))
                done += 1
    except (socket.timeout, TimeoutError):
        print(f"stuck at {done}")
        tell(port, queue)
        return
    c.s.sendall(method(1, 60, 30, short("c") + b"\0"))
    c.expect(60, 31)
    left = Conn(port).ready(queue)
    print("drained" if left == 0 else f"{left} left")


def batches(port, n, size):
    queue = "batches"
    fill(port, queue, n)
    c = Conn(port)
    c.consume(queue, 0)
    done = 0
    held = []
    try:
        while done < n:
            while len(held) < min(size, n - done):
                kind, p = c.frame()
                if kind == 1 and p[:4] == struct.pack(">HH", 60, 60):
                    held.append(p[5 + p[4]:13 + p[4]])
            c.s.sendall(b"".join(method(1, 60, 80, t + b"\0") for t in held))
            done += len(held)
            held = []
    except (socket.timeout, TimeoutError):
        print(f"stuck at {done + len(held)}")
        return
    print("drained")


def closes(port, count):
    for _ in range(count):
        Conn(port).close()
    print("closed")


def beside(port, n):
    """Drain while other clients come and go, on the same broker."""
    def come_and_go():
        while not over.is_set():
            try:
                Conn(port).close()
            except OSError:
                pass
    over = threading.Event()
    threading.Thread(target=come_and_go, daemon=True).start()
    try:
        drain(port, n, 0)
    except ConnectionError:
        print("dropped")
    finally:
        over.set()


def ignore(port, n):
    queue = "ignored"
    fill(port, queue, n)
    c = Conn(port)
    c.consume(queue, 0)
    print("ignoring", flush=True)
    time.sleep(3600)


def gone(port, n):
    queue = "gone"
    fill(port, queue, n)
    c = Conn(port)
    c.consume(queue, 0)
    time.sleep(6)
    # Everything the broker sent before it gave up is still here to be read.
    c.s.settimeout(5)
    try:
        while c.s.recv(1 << 20):
            pass
    except ConnectionError:
        pass
    except (socket.timeout, TimeoutError):
        print("still connected")
        return
    print("cut off")


def dawdle(port, n, seconds):
    queue = "dawdle"
    fill(port, queue, n)
    c = Conn(port)
    c.consume(queue, 0)
    until = time.monotonic() + seconds
    while time.monotonic() < until:
        try:
            if not c.s.recv(16384):
                print("cut off")
                return
        except ConnectionError:
            print("cut off")
            return
        time.sleep(0.1)
    print("still connected")


def behind(port, n):
    source = "behind"
    shared = "behind-shared"
    fill(port, source, n)
    late = Conn(port)
    late.s.sendall(declare(shared))
    late.expect(50, 11)
    late.consume(source, 0)
    time.sleep(1)
    # A client that has stopped reading goes on publishing, so what it is owed
    # for that comes back to a connection that is not taking anything.
    late.s.sendall(b"".join(publish(shared) for _ in range(5000)))
    time.sleep(1)
    well = Conn(port)
    well.s.sendall(method(1, 85, 10, b"\0"))
    well.expect(85, 11)
    well.s.sendall(publish(shared))
    well.s.settimeout(5)
    try:
        well.expect(60, 80)
    except (socket.timeout, TimeoutError):
        print("held up")
        return
    print("answered")


def flood(port, seconds):
    c = Conn(port)
    name = "f" * 255
    c.s.sendall(declare(name))
    ask = declare(name, True) * 1000
    c.s.settimeout(seconds)
    until = time.monotonic() + seconds
    try:
        while time.monotonic() < until:
            c.s.sendall(ask)
    except (ConnectionError, OSError):
        print("cut off")
        return
    print("still connected")


def native(port, n):
    p = socket.create_connection(("127.0.0.1", port))
    p.settimeout(STALL)
    for _ in range(0, n, 1000):
        p.sendall((b"PUB nq 64\r\n" + BODY) * 1000)
        heard = b""
        while heard.count(b"\r\n") < 1000:
            heard += p.recv(1 << 16)
    c = socket.create_connection(("127.0.0.1", port))
    c.settimeout(STALL)
    c.sendall(b"SUB nq\r\n")
    buf = b""
    done = 0
    first = True
    try:
        while done < n:
            at = buf.find(b"\r\n")
            if at < 0:
                more = c.recv(1 << 16)
                if not more:
                    print(f"closed at {done}")
                    return
                buf += more
                continue
            line = buf[:at]
            if first:
                # What `SUB` promised comes before anything it brings.
                if line != b"+OK":
                    print(f"{line!r} came first")
                    return
                first = False
            if line.startswith(b"MSG "):
                _, mid, _, size = line.split()
                need = at + 2 + int(size)
                while len(buf) < need:
                    buf += c.recv(1 << 16)
                buf = buf[need:]
                c.sendall(b"ACK nq " + mid + b"\r\n")
                done += 1
            else:
                buf = buf[at + 2:]
    except (socket.timeout, TimeoutError):
        print(f"stuck at {done}")
        return
    print("drained")


def pings(port, seconds):
    c = socket.create_connection(("127.0.0.1", port))
    c.settimeout(seconds)
    ask = b"PING\r\n" * 100000
    until = time.monotonic() + seconds
    try:
        while time.monotonic() < until:
            c.sendall(ask)
    except (ConnectionError, OSError):
        print("cut off")
        return
    print("still connected")


mode = sys.argv[1]
args = [int(a) for a in sys.argv[2:]]
{"drain": drain, "batches": batches, "closes": closes, "beside": beside, "ignore": ignore, "flood": flood,
 "native": native, "pings": pings, "gone": gone, "dawdle": dawdle, "behind": behind}[mode](*args)
