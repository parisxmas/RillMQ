#!/usr/bin/env python3
"""A client too simple to hide a broker's mistakes.

    slow.py drain <port> <messages> <window>    read one, ack one, on one thread
    slow.py closes <port> <count>               open and politely close
    slow.py ignore <port> <messages>            consume, then never read again
    slow.py flood <port> <seconds>              ask, and never read an answer

RabbitMQ's .NET client cannot be used for this, because it reads on a thread of
its own: the socket keeps emptying whatever the rest of the program is doing. A
client that reads and writes on one thread is the one that parks in a write
while the broker parks in a write to it. Every mode prints one line.
"""
import socket, struct, sys, time

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
        return
    c.s.sendall(method(1, 60, 30, short("c") + b"\0"))
    c.expect(60, 31)
    left = Conn(port).ready(queue)
    print("drained" if left == 0 else f"{left} left")


def closes(port, count):
    for _ in range(count):
        Conn(port).close()
    print("closed")


def ignore(port, n):
    queue = "ignored"
    fill(port, queue, n)
    c = Conn(port)
    c.consume(queue, 0)
    print("ignoring", flush=True)
    time.sleep(3600)


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


mode = sys.argv[1]
args = [int(a) for a in sys.argv[2:]]
{"drain": drain, "closes": closes, "ignore": ignore, "flood": flood}[mode](*args)
