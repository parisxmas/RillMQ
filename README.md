# RillMQ

A message broker written in [Rill](../funclang). Messages are kept in memory
and written to a journal, so a broker that is killed comes back with what it
had answered for. It speaks AMQP 0-9-1, so RabbitMQ's own client libraries
work against it unchanged.

```sh
rill build src/main.rill -o rillmq
./rillmq 6789                          # keeps nothing
./rillmq 6789 dir=data                 # keeps messages in ./data
./rillmq 6789 dir=data amqp=5672       # and speaks AMQP as well
```

Everything else is optional and named: `amqp=<port>` a second port speaking
AMQP 0-9-1, `ack=<ms>` how long a message may be out unanswered, `prefetch=<n>` how much one consumer may hold, `mem=<MB>` how
big the broker may get before it refuses publishes, `idle=<s>` how long a
connection that has asked for nothing may say nothing, `conns=<n>` how many
connections at once.

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

## Speaking AMQP

`amqp=5672` opens a second port speaking AMQP 0-9-1, the protocol RabbitMQ
speaks. The point is not the protocol: it is that a client library which has
never heard of RillMQ works against it. The acceptance test is RabbitMQ's own
`RabbitMQ.Client` for .NET, unmodified, doing what anyone would do with it.

```csharp
var factory = new ConnectionFactory { HostName = "127.0.0.1", Port = 5672 };
using var conn = factory.CreateConnection();
using var ch = conn.CreateModel();
ch.QueueDeclare("orders", durable: true, exclusive: false, autoDelete: false);
ch.BasicPublish("", "orders", null, Encoding.UTF8.GetBytes("hello"));
```

The two protocols are two doors into the same queues. A message published by
the .NET client over AMQP is delivered to a subscriber on RillMQ's own port,
is written to the same journal, and is there after a restart.

What is implemented is what a client actually uses: the connection handshake,
channels, `Queue.Declare` (which is also how a client asks how many are
waiting), `Exchange.Declare` and `Queue.Bind` accepted as no-ops, `Basic.Qos`,
`Basic.Publish`, `Basic.Consume` and `Basic.Deliver`, `Basic.Ack`,
`Basic.Nack` and `Basic.Reject`, `Basic.Get`, `Basic.Cancel`, and closing a
channel or a connection.

Two things are worth knowing. There is only the default exchange: publishing
routes by the routing key, taken as a queue name, and a declared exchange is
answered politely and forgotten. And **a publish over AMQP is not waiting for
the disk** — `Basic.Publish` has no reply unless publisher confirms are on,
and they are not implemented, so the client is told nothing and the journal
catches up behind it. RillMQ's own protocol answers `+OK` after the sync,
which is the difference between the two publish numbers below.

The offsets are the part to get right and the part that fails quietly.
Arguments begin four bytes into a method payload, past the class and the
method, and most methods then open with a `reserved-1` short nobody has used
since 0-9 — so the first field a client filled in is at six. Reading from four
gives every string as empty and sends every message to the queue named `""`,
which works perfectly until something asks a queue its name.

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

## What stops it filling the machine

A queue with nobody reading it grows until the machine has no more to give and
the process is killed. Nothing is lost when that happens — everything answered
for is in a journal — but the broker is down and has to read all of it back,
which is the worst moment to be doing that.

So a publish is refused once the process is holding more than it was told it
could. The measurement is the broker's own resident size rather than a count of
messages, because a count is not what runs out and because it is global by
construction: one queue or a thousand, the number is the number. It is taken
every five hundred and twelve requests rather than every one, since asking the
operating system how big you are costs more than accepting a message does — so
the limit is soft by about that many messages, which at a hundred and sixty-four
bytes each is under a hundred kilobytes of overshoot.

```
$ ./rillmq 6789 data 5000 64 512      # ... and 512 MB
PUB busy 5
hello
-ERR broker is holding all it was told it could
```

The refusal goes down the journal's channel with everything else rather than
straight back to the connection. A client that pipelines three publishes and
has the middle one refused must be answered in the order it asked, and that
ordering is what the one channel gives.

Three floods of two hundred thousand messages at a thirty megabyte limit leave
the broker at thirty-one megabytes with a hundred and ninety-seven thousand
messages held. It does not grow.

## Who is allowed to stay

A client that opens a socket and never speaks holds a strand, a descriptor and
a buffer for as long as it likes, and opening several thousand of those costs
the opener nothing. So a connection that has asked for nothing and said nothing
for a minute is let go.

A subscriber is a different matter: saying nothing is exactly what it is
supposed to do between messages, so the clock only runs for a connection with
no subscriptions. That is also why a subscriber's reads carry no deadline at
all — a consumer acknowledging one message at a time makes many small reads,
and a look at the socket before each of them costs about a tenth of the
delivery rate for a limit it was never going to reach.

The accept loop counts what is up and hears from each connection as it ends,
so the thousand-and-first is turned away with a sentence rather than by a
descriptor that could not be opened. Counting there rather than in a strand of
its own is what keeps it off the path a message takes: nothing asks it
anything.

## Rewriting a journal

A journal grows by one record per publish and one per acknowledgement, so a
queue that has moved a million messages and drained has a million records and
nothing in it. The journal counts what it has written: with `n` published and
`a` acknowledged the file holds `n + a` records and the queue holds `n - a`
messages, so "four times as many records as messages" is `5a > 3n`. Past a few
thousand records, that is when it asks the queue for the queue.

Only the queue knows what is still live, so the queue answers: everything in
flight and everything ready, as records. Then the journal writes a new file,
syncs it, renames it over the old one, and syncs the directory — in that order,
which is the whole of what makes it safe. A machine that stops partway has the
old journal untouched and a half-written temporary nobody will read. The rename
is atomic, so a reader sees one file or the other. And the directory is synced
last, because until it is, the fact that this name now means the new file is
only in a cache: the bytes being durable does not make the *name* durable.

**The request goes down the same channel as the records**, and that is not an
implementation detail. On a channel of its own, an acknowledgement written
after the queue took its snapshot would be appended to the file that is about
to be replaced, and the message it acknowledged would come back from the dead.
In the record stream, everything before the request is in the old file and
accounted for in the snapshot, and everything after it lands in the new one.

Replay tolerates a message appearing twice, because it can: a publish already
on its way when a rewrite starts is written into the new file by the snapshot
and again by the append behind it. Reading the second one changes nothing so
long as it is not counted as a second message.

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
| `QUEUES` | `+QUEUES <n>`, then `n` rows of `Q <name> <ready> <inflight> <subs> <published>` |
| `PING` | `+PONG` |
| `QUIT` | `+OK`, then the socket closes |

`STATS` says how much there is and `QUEUES` says whose, which is the question
worth asking when something is wrong:

```
QUEUES
+QUEUES 2
Q orders 0 2 1 2
Q audit 1 0 0 1
```

The count comes first so a client knows how many rows to read. Asking every
queue is the one slow thing the registry does, and the registry is on the path
of every publish, so each ask has a fifth of a second to answer: a queue too
busy to reply is reported as `Q <name> ? ? ? ?` rather than waited for. There
is room for one late answer, so a queue that replies afterwards is not left
stuck sending it.

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

| RillMQ's own protocol | with a journal | keeping nothing |
|---|---:|---:|
| publish | 85,000/sec | 182,000/sec |
| deliver and acknowledge | 131,000/sec | 131,000/sec |

Over AMQP, through `RabbitMQ.Client`, 50,000 messages of 64 bytes:

| AMQP | with a journal | keeping nothing |
|---|---:|---:|
| publish | 373,000/sec | 334,000/sec |
| deliver and acknowledge | 134,000/sec | 146,000/sec |

The AMQP publish figure is higher than the native one and it is not a better
number: a publish there has no reply to wait for, so it measures the rate at
which the broker takes messages rather than the rate at which it makes them
durable. That is also why having a journal does not slow it down.

Delivery is slower with a journal than without because draining a queue is
what makes its journal worth rewriting, so the rewrite happens during the
measurement. Afterwards the two hundred thousand records are eight kilobytes
on disk.

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

Two hundred thousand 64-byte messages sitting in a queue cost 31 MB, which is
**164 bytes a message**: the sixty-four of body, a hash table entry, a boxed
record and a counted string, each rounded up by the allocator.

That number was six hundred to nine hundred until this broker was measured
carefully, and the difference was not in the broker. A `select` arm that bound
a value and only read it never released it, so every message left seventeen
allocations behind. It is a Rill bug and it is fixed there; twenty thousand
messages published, delivered and acknowledged now leave two allocations at
exit rather than three hundred and forty thousand. Writing a broker turns out
to be a good way to find one.

An acknowledged message is freed the moment the acknowledgement is handled —
out of the ready table on delivery, out of the in-flight table on the `ACK`,
and that is the last reference, so reference counting takes it there and then.
Publishing two hundred thousand, acknowledging all of them and publishing two
hundred thousand more leaves the broker exactly where the first two hundred
thousand had it.

## Running the tests

```sh
rill build src/main.rill  -o rillmq
rill build test/client.rill -o rillmq-test
rill build test/bench.rill  -o rillmq-bench

./rillmq 7700 ack=300 &      # no journal, a 300 ms ack deadline
./rillmq-test 7700           # 10 checks
./rillmq-bench 7700 200000 64

sh test/persistence.sh       # 15 checks, most of which stop the broker
sh test/amqp.sh              # 8 checks through RabbitMQ's own .NET client
```

The AMQP checks need the .NET SDK; `test/dotnet` is a plain console program
with a `PackageReference` to `RabbitMQ.Client`, and nothing else.

The test client has its own parser rather than the broker's, because a wire
format that only ever reads itself has not been tested. The persistence checks are a shell script because they need
the broker itself to end, including once by `kill -9` and once with half a
record appended by hand.

All twenty-five pass against a `--parallel` build on four workers, and with or
without a journal.

## What it deliberately is not, yet

- **Replay is one pass and no more.** Starting up reads every journal from the
  front. A rewrite keeps that bounded by the size of the queue rather than by
  its history, but there is no snapshot and no index, so a broker holding ten
  million messages reads ten million records to start.
- **A rewrite holds the whole live queue as records at once.** The queue builds
  them and hands them over as a list, which for a large queue is a second copy
  of it in memory for as long as the write takes.
- **One node.** No clustering, no replication, and no way to name a peer:
  Rill's sockets are IPv4 addresses with no name resolution.
- **No TLS, and no authentication.** Do not put this on a network you do not
  own.
- **No exchanges, routing keys or topics.** One queue, by name, fanning out to
  competing consumers. Over AMQP that is the default exchange and nothing else:
  a declared exchange is answered and forgotten, and a binding with it.
- **No publisher confirms.** A publish over AMQP is answered by nothing, so a
  client cannot learn when its message became durable. RillMQ's own protocol
  can, and does.
- **One virtual host, and any password.** `Connection.Open` takes whatever
  virtual host it is given and `PLAIN` takes whatever credentials it is given.
- **No flow control back to publishers, only a wall.** A publisher that
  outruns its consumers is refused rather than slowed, so it finds out by
  being told no rather than by being made to wait.
- **Nothing is measured over time.** `QUEUES` says what is true now; there is
  no rate, no age of the oldest message, and no way to see how big a queue's
  journal has grown.
