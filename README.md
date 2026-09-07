# RillMQ

A message broker written in [Rill](../funclang). Messages are kept in memory
and written to a journal, so a broker that is killed comes back with what it
had answered for.

```sh
rill build src/main.rill -o rillmq
./rillmq 6789 data     # a directory to keep messages in
./rillmq 6789 -        # or `-` for a broker that keeps nothing
```

```
$ nc localhost 6789
PUB jobs 5
hello
+OK 1
SUB jobs
+OK
MSG 1 jobs 5
hello
ACK jobs 1
+OK
```

## What it is

Queues that fan out to competing consumers, with acknowledgements, redelivery
of anything that goes unanswered, and a prefetch window so a fast queue cannot
bury a slow consumer. Messages are written down before a publisher is told they
are safe, and a broker that is killed comes back with everything it had
answered for. It stops cleanly on `SIGINT` or `SIGTERM`.

## The design is the concurrency model

There is no lock anywhere in RillMQ, and nothing is shared between strands.
Rill's maps are storage rather than values and are explicitly one strand's at a
time on a multi-worker build, so a queue that several connections could reach
would have to be guarded. Making the queue *be* a strand removes the question.

| strand | how many | what it owns |
|---|---|---|
| registry | one | which queues exist, and the channel to each |
| queue | one per queue | its messages, its subscribers, what is in flight |
| connection reader | one per connection | the bytes arriving, and nothing else |
| connection writer | one per connection | the socket's write side |

Everything else is a channel. A connection that wants to publish sends a
request down the queue's channel with a reply channel inside it, and parks
until the answer comes back. Two strands per connection rather than one,
because a subscriber is being pushed messages at the same time as it is
sending commands, and one strand cannot be parked on a socket read and a
channel receive at once. Only the writer writes, which is what makes the
socket safe without a lock: it is one strand's, exactly as a queue's messages
are.

Stopping is one `close(ch)`. Every queue, the registry and every connection
writer is parked on a `select` that watches the same `done` channel, so
closing it reaches all of them at once and nothing has to be counted or
joined.

## The journal

One file per queue, appended to and never written over. Three kinds of record
go in it — the queue's name once at the front, a message published, a message
acknowledged — and a queue is rebuilt by reading them in order and keeping
whatever was published and not acknowledged.

```
kind  1 byte    'Q' the name, 'P' published, 'A' acknowledged
id    8 bytes   little-endian
len   4 bytes   little-endian, the body that follows
body  len bytes
sum   4 bytes   little-endian, FNV-1a over everything above in this record
```

The checksum is not there for corruption, which is the disk's business. It is
there for the *last* record: a machine that loses power in the middle of an
append leaves a record half written, and the reader has to be able to say so
rather than believing whatever the length field happened to contain. What
follows a record that does not add up is not read, and the file is cut back to
where it did.

**A publisher is answered after the sync, and not before.** That is the only
promise a broker can make about the disk, and it is what the `+OK` means. An
acknowledgement is written down but nothing waits for it: losing one costs a
redelivery, which at-least-once already allows, and it can never overtake the
message it acknowledges because the file is written in order.

**Syncs are batched, and the batching is not a timer.** A sync takes about a
millisecond, and Rill's `file_sync` parks the strand rather than holding the
worker — so while one batch is going to the disk, everything arriving queues up
behind it and goes out together on the next turn. A disk that costs a
millisecond a sync therefore costs a millisecond a *batch*, and the batch is as
big as the publisher is fast.

That only works if nothing waits in between, and getting it wrong is
instructive: the connection strand originally waited for each publish to be
answered before reading the next command, which meant one message per sync
however many the publisher had sent at once — four hundred a second, with a
batch size of one. The queue is handed the connection's writer instead, and the
`+OK` is written by whoever makes the message durable.

## The wire

Text lines with a length-prefixed body, so a person can read a session with
`nc` and a parser never has to look inside a payload for where it stops. A
body is bytes; a Rill string is a byte string, so nothing cares what is in one.

| sent | answered |
|---|---|
| `PUB <queue> <len>\r\n<body>` | `+OK <id>` |
| `SUB <queue>` | `+OK`, then `MSG` frames |
| `UNSUB <queue>` | `+OK` |
| `ACK <queue> <id>` | `+OK` |
| `NACK <queue> <id>` | `+OK`, and the message comes straight back |
| `STATS` | `+STATS <queues> <ready> <inflight>` |
| `PING` | `+PONG` |
| `QUIT` | `+OK`, then the socket closes |

Anything refused answers `-ERR <what>`. A subscriber is pushed
`MSG <id> <queue> <len>\r\n<body>` whenever the broker has something for it.
An acknowledgement names the queue as well as the message because ids are each
queue's own count, and the client has the name already: it came with the
message.

Subscribing is answered before anything the subscription brings. That is the
one piece of ordering promised, and it is worth knowing what is *not*: a
pushed message and the answer to a command are put into the connection's one
channel by two different strands, so nothing decides which lands first. A
client reads for the kind of frame it is waiting for and lets the other kind
past, which is what a client for any protocol that pushes has to do.

## Delivery

At least once. A message handed to a consumer is in flight until it is
acknowledged; if the deadline passes first it goes back to the *front* of the
queue, because a consumer that died holding message one should not put it
behind the thousand that arrived while it was dying. A consumer that
disconnects gives back what it was holding at once rather than waiting out the
deadline. `NACK` is the same thing said on purpose.

The queue wakes for a deadline through a `select` arm rather than a ticker
strand:

```
step = select
  r = recv(inbox) -> Some(q_pump(q_handle(q, r)))
  recv(done) -> None
  timeout(q_wait_ms(q)) -> Some(q_pump(q_sweep(q)))
```

`q_wait_ms` is the time the soonest deadline has left, or half a minute when
nothing is in flight. An idle queue costs nothing at all.

**Prefetch is not optional**, and that is the thing this exercise taught. With
no limit on what one consumer may hold, a queue with fifty thousand messages
in it hands every one of them to the first subscriber that appears. The socket
fills, so the strand writing it parks; the channel behind that strand fills,
so the strand *reading* the connection parks trying to put a reply in it; and
the reply it was trying to put there was the answer to an acknowledgement. The
consumer waits for the broker to read an acknowledgement the broker cannot
read until the consumer reads what it has already been sent. Neither side is
at fault and neither can move. A window of sixty-four messages per consumer is
what stands between a broker and that deadlock.

## Numbers

Apple M4, one worker, 200,000 messages of 64 bytes on one connection.
Publishes are pipelined a thousand at a time; deliveries are acknowledged one
at a time. `file_sync` on macOS is `F_FULLFSYNC`, which waits for the drive
rather than for the cache.

| | with a journal | keeping nothing |
|---|---:|---:|
| publish | 88,000/sec | 209,000/sec |
| deliver and acknowledge | 166,000/sec | 165,000/sec |

| | |
|---|---:|
| binary | 220 KB |
| idle | 1.4 MB |

A pipelining client has to bound its depth. A client that writes a hundred
thousand requests and reads none of the answers deadlocks against any broker
with a finite amount of room in it: the replies fill the socket, the broker
stops being able to write and therefore stops reading, and the client is still
writing. `test/bench.rill` writes a thousand and reads a thousand, which is
what every pipelining client does and for this reason.

Memory with messages actually queued is the honest weak spot: about 600 to 900
bytes per 64-byte message while a large queue is draining. A message is a boxed
record holding a counted string, in a hash table that grows by doubling and
keeps a tombstone for every entry taken out, and the frame handed to each
subscriber is built fresh. A ring buffer over one allocation would be a
different order of magnitude, and Rill does not have a growable array of
arbitrary values to build one from yet.

## Running the tests

```sh
rill build src/main.rill  -o rillmq
rill build test/client.rill -o rillmq-test
rill build test/bench.rill  -o rillmq-bench

./rillmq 7700 - 300 &        # port, no journal, a 300 ms ack deadline
./rillmq-test 7700           # 9 checks
./rillmq-bench 7700 200000 64

sh test/persistence.sh       # 10 checks, each of which stops the broker
```

`rillmq <port> [dir|-] [ack_ms] [prefetch]`. The test client has its own parser
rather than the broker's, because a wire format that only ever reads itself has
not been tested. The persistence checks are a shell script because they need
the broker itself to end, including once by `kill -9` and once with half a
record appended by hand.

All nineteen pass against a `--parallel` build on four workers, and with or
without a journal.

## What it deliberately is not, yet

- **A journal is never compacted.** It grows by one record per publish and one
  per acknowledgement, for ever. A queue that has moved a billion messages has
  a journal of a billion records and takes as long to start as it takes to read
  them. Rewriting it to hold only what is live — write a new file, sync it,
  rename it over, `dir_sync` the directory — is the obvious next thing and is
  not here.
- **Replay is one pass and no more.** Starting up reads every journal from the
  front. There is no snapshot and no index.
- **One node.** No clustering, no replication, and no way to name a peer:
  Rill's sockets are IPv4 addresses with no name resolution.
- **No TLS, and no authentication.** Do not put this on a network you do not
  own.
- **No exchanges, routing keys or topics.** One queue, by name, fanning out to
  competing consumers.
- **No flow control back to publishers.** A publisher can fill memory faster
  than consumers drain it, and nothing stops it.
