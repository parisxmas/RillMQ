# RillMQ

A message broker written in [Rill](../funclang), keeping everything in memory.

```sh
rill build src/main.rill -o rillmq
./rillmq 6789
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
bury a slow consumer. It stops cleanly on `SIGINT` or `SIGTERM`. It keeps
nothing on disk, so a restart starts empty.

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

Apple M4, one worker, 200,000 messages of 64 bytes on one connection, three
runs. Publishes are pipelined; deliveries are acknowledged one at a time.

| | |
|---|---:|
| publish | 215,000/sec |
| deliver and acknowledge | 155,000/sec |
| binary | 215 KB |
| idle | 1.4 MB |

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

./rillmq 7700 300 &          # port, and a 300 ms ack deadline for the tests
./rillmq-test 7700           # 9 checks
./rillmq-bench 7700 200000 64
```

`rillmq <port> [ack_ms] [prefetch]`. The test client has its own parser rather
than the broker's, because a wire format that only ever reads itself has not
been tested.

The same nine checks pass against a `--parallel` build on four workers.

## What it deliberately is not, yet

- **Nothing is written down.** A restart starts empty. The language has the
  file calls for a journal — appends, `file_sync` off the worker, `dir_sync`
  for the name — and this does not use them.
- **One node.** No clustering, no replication, and no way to name a peer:
  Rill's sockets are IPv4 addresses with no name resolution.
- **No TLS, and no authentication.** Do not put this on a network you do not
  own.
- **No exchanges, routing keys or topics.** One queue, by name, fanning out to
  competing consumers.
- **No flow control back to publishers.** A publisher can fill memory faster
  than consumers drain it, and nothing stops it.
