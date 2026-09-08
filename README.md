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

Everything else is optional and named. Ports: `amqp=<port>` AMQP 0-9-1,
`tls=<port>` and `amqps=<port>` the same two wrapped in TLS, `manage=<port>`
the management pages. Files: `cert=` and `key=` a certificate and its key in
PEM, `users=` who may connect. Cluster: `peers=<host:port,...>`, `node=<i>`,
and `peer=<name:word>` for what one node signs in to another as. Limits:
`ack=<ms>` how long a message may be out unanswered, `prefetch=<n>` how much
one consumer may hold, `mem=<MB>` how big the broker may get before it refuses
publishes, `idle=<s>` how long a connection that has asked for nothing may say
nothing, `conns=<n>` how many connections at once.

```sh
./rillmq passwd data/users alice hunter2
./rillmq 6789 dir=data amqp=5672 amqps=5671 users=data/users \
         cert=server.pem key=server.key manage=15672
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
channels — several to a connection, each with its own consumers and its own
prefetch — `Queue.Declare` (which is also how a client asks how many are
waiting), `Exchange.Declare` and `Queue.Bind` — both of which now refuse a name nobody
declared — `Basic.Qos`, `Basic.Publish`,
`Basic.Consume` with `no-ack`, `Basic.Deliver`, `Basic.Ack`, `Basic.Nack` and
`Basic.Reject`, `Basic.Get`, `Basic.Cancel`, `Confirm.Select`, and closing a
channel or a connection — from either side, since the broker closes a channel
itself when it has to refuse something. A message's properties — `reply-to`,
`correlation-id`, `content-type`, headers — are kept as they came and handed
back untouched, which is what the request-and-reply pattern is made of.

**Publisher confirms are the interesting one.** A `Basic.Publish` has no reply,
so an AMQP client cannot otherwise learn when its message reached the disk —
which is exactly what RillMQ's own protocol tells a publisher as a matter of
course. `ConfirmSelect()` turns that on, and then the journal's answer becomes
the `Basic.Ack` the client is waiting for:

```csharp
ch.ConfirmSelect();
for (int i = 0; i < 200; i++) ch.BasicPublish("", q, null, body);
ch.WaitForConfirms(TimeSpan.FromSeconds(20));   // every one is on the disk
```

Confirms are numbered in publish order and the journal answers in the order it
was asked, so counting is enough and nothing has to be remembered per message.

`Exchange.Declare` makes a real exchange and `Queue.Bind` a real binding; see
**Routing** above for what the three kinds do.

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

## More than one node

```sh
./rillmq 6789 dir=a node=0 peers=10.0.0.1:6789,10.0.0.2:6789
./rillmq 6789 dir=b node=1 peers=10.0.0.1:6789,10.0.0.2:6789
```

A queue lives on one node, and the rule for which is the whole of the design:
its owner is the hash of its name over however many nodes there are. No node
has to be asked and no node has to be told. Every node is given the same list
in the same order and works out the same answer, which is what lets a client
connect to any of them.

A node asked for a queue it does not hold does not redirect the client. It
stands in for the queue: a strand that looks exactly like a queue from the
inside, and is on the outside a client connection to the node that has the
real one. That is why the link between nodes is RillMQ's own protocol rather
than something new — the thing at the far end is already a broker, and already
speaks it. Publishing, subscribing, acknowledging and asking for numbers all
cross that way, and the `.NET` client cannot tell: all thirteen of its checks
pass against a node where three of the queues are somewhere else.

The far end answers in the order it was asked, so the proxy keeps its
outstanding questions in a queue and hands each answer to whoever asked first.
Nothing is numbered and nothing is correlated.

A node whose peer cannot be reached still answers, because a publisher that
names a queue on a dead node must be told something rather than wait for ever.
It says `-ERR the node holding that queue cannot be reached`.

### The copy

Every other node keeps a copy of a queue's journal, and **a publish is not
confirmed until a majority has it**. With three nodes that is the leader and
either of the other two, so one node going takes nothing with it; with two it
is both of them, which is the truth about two-node clusters rather than a
shortcoming of this one.

What travels is the journal batch itself: the same bytes the leader just
wrote, with the same checksums, appended by the other node to the file that
queue would have if it held it. So the two files are identical, and promoting
a copy is nothing more than starting a queue from a journal that is already
there. One batch is one message on the wire and one wait, however many records
are in it, which is what makes this cost a round trip a batch rather than a
round trip a publish.

A leader that cannot reach a majority refuses:

```
PUB gamma 2
hi
-ERR not enough of the cluster has it
```

That is not politeness, it is the whole safety argument. Whatever an isolated
leader is still holding, nobody was told it was safe, so somebody else may
take the queue over without losing anything anyone was promised. A node that
was down when a queue started is tried again about once a second, so a peer
coming back is picked up without anyone doing anything.

A refused publish has still been written to the leader's own journal, and will
be delivered if that leader is the one that carries on. A publisher told no and
a message delivered anyway is what at-least-once means when the answer went
missing, and it is why a publisher that cares retries with an idea of its own
about duplicates.

A queue whose *owner* cannot be reached is a different matter, and is answered
rather than left waiting:

```
PUB alpha 4
down
-ERR the node holding that queue cannot be reached
```

The node keeps trying, about once a second, and picks up again the moment the
owner is back — with the subscriptions asked for again, because the far end
has no memory of them.

### Failing over

A node whose stand-in for a queue cannot reach the node holding it does not
give up and does not guess. It asks everybody else whether they know of a
leader — a node that gave its vote does — and only if nobody does, it stands
for the leadership itself:

```
VOTE orders <turn> <how much of the journal I have> <which node I am>
+VOTE
```

A vote is given when the turn asked for is later than any this node knows of
**and** the asker's journal for that queue is at least as long as its own. The
second half is the one that matters. A majority of the cluster has every record
anybody was told was safe, so a candidate a majority will vote for has all of
them too — and a candidate that has fallen behind is refused by everybody who
is ahead of it, which is enough of them to matter.

With a majority the candidate writes the turn into the queue's journal and
starts serving it. Everything it sends from then on says which turn it is for,
and a node holding an older one is refused:

```
REPL orders <turn> <len>
-STALE
```

which is how a leader that was replaced while it was away finds out. It can no
longer reach a majority, so it can no longer confirm anything, so nothing it
takes can be lost by the cluster carrying on without it.

The request that starts an election is refused, and whoever made it asks again
— by then there is a leader. Nobody decides anything on a clock: an election
happens when somebody wants a queue and cannot have it.

**Three nodes are the smallest cluster this works in.** A majority of two is
two, so a two-node cluster cannot lose a node and still confirm, and cannot
elect anybody either. That is a fact about two-node clusters rather than a
shortcoming here, and it is why clusters have an odd number of nodes.

### Taking a copy over by hand

```
PROMOTE orders
+OK
```

The copy's journal is already the queue's journal — the same file, the same
bytes — so starting a queue from it is all there is to do. What was a stand-in
for somebody else's queue is replaced, and every connection picks the new one
up on its next command, because a connection asks the registry each time
rather than remembering. The id it hands out next is the proof: a queue that
had taken two messages answers `+OK 3` on the node that took it over.

`PROMOTE` is what an election does at the end, and it is there on its own for
the times a person has decided something the cluster cannot: a node that is
never coming back, in a cluster too small to elect anybody.

## Routing

An exchange is a strand, like a queue, and for the same reason: its bindings
are its own and nothing else touches them. Three kinds:

| kind | takes a message when |
|---|---|
| `direct` | the binding key is the routing key |
| `fanout` | always — every queue bound to it |
| `topic` | the binding pattern matches the routing key |

A topic pattern is words separated by dots, where `*` stands for one word and
`#` for any number of them, none included. `*.error` takes `app.error` and
`db.error`; `app.#` takes `app.error` and `app.db.warn` and `app` itself.
Matching it is the one piece of real algorithm in RillMQ, because `#` can
swallow any prefix and the only way to know whether it should is to try.

```
XDECL logs topic
BIND logs errs *.error
BIND logs under app.#
XPUB logs app.error 9
not found
```

and the same three things over AMQP are `Exchange.Declare`, `Queue.Bind` and a
`Basic.Publish` that names an exchange.

**The default exchange is not one of these.** Publishing to `""` means the
routing key is a queue name, and a connection goes straight to that queue — no
hop, no bindings, nothing to look up. That is the path most messages take and
it costs what it costs; named exchanges are the ones with something to decide.

**The table is written down.** Exchanges and bindings go into `_routes.log`,
the same journal a queue's messages go into — the same record, the same
checksum, the same rule about what follows a record that does not add up —
holding what was declared rather than what was published. Reading it back in
order gives the table as it stood: an exchange declared, a binding made, a
binding taken away. A client does not have to declare anything again after a
restart.

Reading it back does not write it out again, which is the one thing that would
have made the file longer on every restart for a table that had not changed.
A binding is written and nothing waits for it: it is one small record on a file
that is synced with the next batch, and a binding lost in that window is one
the client is about to declare again anyway, because declaring is what a client
does on the way in.

Nowhere to go is not an error. A message published to a key nothing is bound
to is dropped, which is what every broker does, and the publisher is answered
all the same because it asked. One queue is the ordinary case and costs
nothing extra: the publisher's own channel goes to the queue and the answer
comes back from there. More than one needs somebody to wait for all of them
and confirm once, and that is a strand made for the purpose and gone as soon
as it has counted.

## Passwords

Off by default, because a broker on a machine of its own behind nothing else
should be. `users=<file>` turns it on, and then nobody gets in without a name
and a word — over RillMQ's own protocol, over AMQP, and on the management
pages alike.

```sh
./rillmq passwd data/users alice hunter2
./rillmq passwd data/users bob s3cret monitoring
./rillmq 6789 amqp=5672 users=data/users
```

The file is one person a line and the word is not in it:

```
alice:9f3c…:4096:1a7b…:administrator
 name  salt  work  hash   what they may do
```

The hash is PBKDF2-HMAC-SHA-1, which `lib/passwd.rill` had already written and
checked against the published vectors. The salt is why two people who chose
the same word do not share a line. The work factor is written down beside the
hash rather than compiled in, so raising it later leaves everybody already in
the file able to sign in with the count that made them.

Four thousand rounds is about sixty-five milliseconds here. It is a
compromise, and worth saying which way: the number cannot go much higher,
because a broker is a place a thousand clients may all come back to at once
after a restart and this is time on the one thread that then owes them all an
answer. RabbitMQ, for comparison, stretches not at all — one SHA-256 over salt
and word — so this is four thousand times its cost per guess and still under a
tenth of a second per login.

A name nobody has costs the same as a name somebody has with the wrong word:
the hashing runs either way, so the clock does not answer a question the
broker declines to.

On the wire it is `AUTH name word` before anything else, and over AMQP it is
SASL `PLAIN`, which is what every client already sends. A word that is wrong
ends an AMQP connection with no method and no reason given — what the
specification asks for, and the only answer that does not say whether the
*name* was the part that was wrong.

## TLS

Two more ports, `tls=` for RillMQ's own protocol and `amqps=` for AMQP, both
from one certificate:

```sh
openssl req -x509 -newkey rsa:2048 -keyout key.pem -out cert.pem \
        -days 365 -nodes -subj "/CN=broker.example" \
        -addext "subjectAltName=DNS:broker.example"
./rillmq 6789 amqps=5671 cert=cert.pem key=key.pem users=data/users
```

`RabbitMQ.Client` connects to `amqps=` with `Ssl.Enabled = true` and nothing
else changed, which is the whole point.

The handshake is OpenSSL's, reached through Rill's `extern`, so this is a page
of declarations in `src/tls.rill` rather than a TLS implementation. Nothing
above that file knows which kind of connection it has: a connection carries an
`SSL*` that is null when it is in the clear, and `p_read`, `p_write` and
`p_close` are what the broker calls instead of the socket. That was the whole
of the change TLS made to the rest of it.

A handshake happens on the connection's own strand rather than in the accept
loop, and parks on the descriptor exactly as a plain read does — so a client
that connects and then says nothing costs a parked strand and no processor.
A certificate that will not load stops the broker rather than quietly leaving
the plain ports up: somebody who asked for TLS and got a broker without it has
been told the opposite of the truth.

Building now needs OpenSSL 3. On a Mac that is `brew install openssl@3`; on a
Linux box it is already where the linker looks.

## The management pages

`manage=15672` opens an HTTP server on the same registry everything else
talks to. There is no second copy of anything: a page asks the registry what a
client's `QUEUES` asks it, and a form that publishes publishes down the
channel a publisher uses.

```sh
./rillmq 6789 dir=data users=data/users manage=15672
open http://localhost:15672/
```

It shows the totals and a row per queue — ready, in flight, consumers, and how
many have ever been published — and it can publish a message, empty a queue,
and add or remove a person. `/api/queues` and `/api/overview` are the same
numbers as JSON, for a program. Everything is behind the same password file as
the brokers, over HTTP's own `Authorization: Basic`.

The pages are Rill's own HTTP server (`lib/http.rill`) and Rill's own template
compiler: `web/*.tmpl` are turned into Rill by `tools/pages.sh`, so rendering a
page is a run of appends to a builder with nothing parsed and no tag looked at
while somebody is waiting. The generated files are checked in, so building the
broker stays `rill build src/main.rill` and nothing else.

Emptying a queue writes an acknowledgement record per message, so a queue
emptied from a page is still empty after a restart. What is in flight is left
alone: it is somebody's to answer for.

## When the answer is no

A channel is where a refusal lands, which is what a channel is for: a
connection carrying eight of them loses one and keeps seven. The broker sends
`Channel.Close` with a code and a reason, cancels that channel's consumers,
and then ignores everything on it until the client says `Channel.Close-Ok` —
the client has frames in flight that were written before it heard, and
answering those would be answering questions asked of a channel that is gone.

| what happened | code | what the client sees |
|---|---:|---|
| a method this broker has not written | 540 | an exception naming the class and method |
| acknowledging a tag it was never given | 406 | `unknown delivery tag` |
| a consumer tag already in use | 406 | `consumer tag ... is already in use` |
| publishing to or binding an exchange nobody declared | 404 | `no exchange ... in vhost /` |

Each of those used to be silence, and silence is the worst of the three
answers a broker can give. A client calling `Queue.Purge` waited for a reply
that was never coming, which is harder to work out than being refused. A
client acknowledging the same message twice — the ordinary shape of a
double-delivery bug — was told nothing by the one party that knew. A second
consumer registered under a name already in use quietly replaced the first,
which stayed subscribed in the queue with nothing left pointing at it.

The 404 is the one a client earns most often, by typing a name wrong, and
making the exchange instead was the expensive kind of silence: the messages
went somewhere real, with nothing bound to it, and were dropped one at a time
by something that looked like it was working. The registry gained a look that
does not make — `RExchangeIf`, answering with the exchange or with nothing —
so this costs no round trip that a publish was not already paying. The default
exchange is not in that table and does not need to be: a queue is reachable by
its own name through it, and always was.

Channel zero is the connection's own and cannot be closed this way, so what is
not written there is passed over instead.

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
| `XDECL <exchange> <kind>` | `+OK` — `direct`, `fanout` or `topic` |
| `BIND <exchange> <queue> <key>` | `+OK` |
| `XPUB <exchange> <key> <len>\r\n<body>` | `+OK` |
| `STATS` | `+STATS <queues> <ready> <inflight>` |
| `QUEUES` | `+QUEUES <n>`, then `n` rows of `Q <name> <ready> <inflight> <subs> <published>` |
| `QSTAT <queue>` | `+QSTAT <ready> <inflight> <subs> <published>` |
| `DRAIN <queue>` | `+DRAIN <n>` — everything waiting thrown away, what is in flight left alone |
| `AUTH <name> <word>` | `+OK`, or `-ERR no`; wanted first when the broker was given a `users=` file |
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

## Against RabbitMQ

Apple M4, one worker, RabbitMQ 4 on the same machine, and the same program
pointed at each in turn — `RabbitMQ.Client` 6.8.1, 64-byte messages, a queue
of its own each run. Neither broker knows which it is.

| over AMQP, through RabbitMQ's own .NET client | RabbitMQ | RillMQ |
|---|---:|---:|
| publish, transient | 205,000/sec | 837,000/sec |
| deliver and acknowledge | 160,000/sec | 265,000/sec |
| deliver, `no-ack` | 285,000/sec | 470,000/sec |
| publish, durable, waiting for confirms | 282,000/sec | 819,000/sec |
| resident after 170,000 messages | 214 MB | 95 MB |

The transient publish figure is the one to read carefully: with confirms off
there is nothing to wait for, so it measures how fast a broker takes messages
rather than how fast it makes them durable.

The durable row was 329,000/sec, and the reason it was so much closer to
RabbitMQ than the others is worth writing down, because the obvious
explanation was wrong. It looked like the disk: a confirmed publish waits for
a sync, a sync is the drive, and no broker is faster than the drive it is
waiting for. It was not the disk. A hundred thousand confirmed publishes took
251 ms with a journal and 231 ms with no journal at all — group commit had
already made the syncs nearly free, and twenty milliseconds of a quarter of a
second is not where a measurement goes.

What it was is the writing back. Every confirmed publish got a `Basic.Ack` of
its own: a frame built, and a `write` for it. A hundred thousand publishes
meant a hundred thousand syscalls, which is about what the missing time cost.
`Basic.Ack` has a `multiple` bit meaning "and everything before this one" —
which is what RabbitMQ sends and what RillMQ was not — so the confirms are now
counted as they arrive and said at the moment the session runs out of things
to do: one frame for however many became safe while it was busy. A hundred
thousand confirmed durable publishes went from 251 ms to 126 ms, and without a
journal from 231 ms to 99 ms.

Two things are worth taking from that. The first is that the fix was the
cheap kind — the protocol already had the bit, and the broker was not using
it. The second is that the disk had stopped being the interesting cost some
time ago and nobody had checked: the honest way to find that out was to run
the same measurement against a broker with no journal at all, which took one
extra process and settled the question in one line.

The 126 ms now has 56 ms of client in it — that is how long `RabbitMQ.Client`
takes to write a hundred thousand publishes into a socket before RillMQ has
had to do anything at all — so the broker's own share of a confirmed durable
publish is nearer 70 ms, or 1.4 million a second.

What that measurement found on the way was a deadlock in RillMQ's own AMQP
session, and it is worth writing down because it is the shape of bug an actor
model invites. The session both asks queues questions and is where their
deliveries come out. Merging both into one inbox was simpler to write and was
a deadlock: the session stood still on an answer, while the queue stood still
trying to put a delivery into the very pipe the session had stopped emptying.
Twenty thousand publishes and then one `Queue.Declare` was enough. Two inboxes
rather than one, and an offer to send made in the same breath as a readiness
to receive, is the fix — `am_hand` and `am_wait` in `src/amqp_serve.rill`.

### Why delivery is only two thirds ahead

Publishing is four times RabbitMQ's rate and delivery is not, and the reason
is that they are not the same kind of measurement. A publish is one way: the
client writes and the broker absorbs, and the number says how fast the broker
absorbs. A delivery is a round: the broker writes, the client's own consumer
dispatch takes it apart and hands it to a thread, and an acknowledgement goes
back. Two thirds of that is the client's, and the client is the same one in
both columns, so it flattens the ratio.

Acknowledging is what most of the cost is, and both brokers pay it in the same
proportion. With the acknowledgements taken out — `no-ack`, where the client
says it will not answer for what it is given — RillMQ goes from 265,000 to
470,000 and RabbitMQ from 160,000 to 285,000. Each roughly doubles, so the
ratio is what it was: the acknowledgement is not where RillMQ's advantage goes.

With no journal and no acknowledgements the broker holds 568,000/sec, and does
it three runs in a row within half a per cent of itself. A number that stable
across runs is usually not the thing being measured — but replacing the
client's own bookkeeping with an interlocked counter did not raise it, so the
remaining cost is the frames themselves: this broker writing them and that
client parsing them.

Finding this out is also what turned up a plain bug. `no-ack` could not be
measured, because RillMQ read the flag off `Basic.Consume` and did nothing with
it: a consumer that asked for it was given one prefetch window and then
nothing at all, for ever. Handing messages out is driven by requests arriving —
an acknowledgement is what usually says "there is room now" — and a consumer
that has been told it need not acknowledge sends none. It now says so in one
message a batch instead, at the moment the connection has caught up.

## Numbers

Apple M4, one worker, 200,000 messages of 64 bytes on one connection.
Publishes are pipelined a thousand at a time; deliveries are acknowledged one
at a time. `file_sync` on macOS is `F_FULLFSYNC`, which waits for the drive
rather than for the cache.

| RillMQ's own protocol | with a journal | keeping nothing |
|---|---:|---:|
| publish | 119,000/sec | 366,000/sec |
| deliver and acknowledge | 303,000/sec | 329,000/sec |

Both of those were about forty per cent lower until the connection's writer
was given a write buffer. Every `+OK` and every `MSG` was a `write` of its
own, so a publisher pipelining a thousand requests cost a thousand syscalls to
answer. `sock_buffered(fd)` queues them and the runtime empties the queue
whenever the strand parks — which, for a writer, is the `select` it does
between frames, and so is exactly the moment the client is waiting to hear.
It is one line, and it is the same mistake as the confirms above, found by
looking for the same mistake in the other protocol.

Over AMQP, through `RabbitMQ.Client`, 50,000 messages of 64 bytes:

| AMQP | with a journal | keeping nothing |
|---|---:|---:|
| publish | 918,000/sec | 935,000/sec |
| deliver and acknowledge | 261,000/sec | 269,000/sec |

The AMQP publish figure is higher than the native one and it is not a better
number: with confirms off — which is how the measurement above was taken, and
how most publishing is done — there is no reply to wait for, so it measures
the rate at which the broker takes messages rather than the rate at which it
makes them durable. That is also why having a journal does not slow it down.
`ConfirmSelect()` puts the wait back, and what it costs is one frame per batch
rather than one per message; see the section above for how that came to be
true and what it was before.

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
./rillmq-test 7700           # 11 checks
./rillmq-bench 7700 200000 64

sh test/persistence.sh       # 18 checks, most of which stop the broker
sh test/amqp.sh              # 30 checks through RabbitMQ's own .NET client
sh test/secure.sh            # 9 checks of passwords and TLS
sh test/manage.sh            # 15 checks of the management pages
sh test/cluster.sh           # 7 checks across two nodes
sh test/majority.sh          # 5 checks across three
sh test/failover.sh          # 7 checks, one node killed with nobody watching
```

The AMQP checks need the .NET SDK; `test/dotnet` is a plain console program
with a `PackageReference` to `RabbitMQ.Client`, and nothing else.

The test client has its own parser rather than the broker's, because a wire
format that only ever reads itself has not been tested. The persistence checks are a shell script because they need
the broker itself to end, including once by `kill -9` and once with half a
record appended by hand.

All hundred and two of them pass, with or without a journal.

## What it deliberately is not, yet

- **Replay is one pass and no more.** Starting up reads every journal from the
  front. A rewrite keeps that bounded by the size of the queue rather than by
  its history, but there is no snapshot and no index, so a broker holding ten
  million messages reads ten million records to start.
- **A rewrite holds the whole live queue as records at once.** The queue builds
  them and hands them over as a list, which for a large queue is a second copy
  of it in memory for as long as the write takes.
- **An election is one round and no heartbeats.** Nothing notices a node is
  gone until somebody wants a queue it held. A cluster nobody is using does
  not notice anything, which is the right amount of noticing for a cluster
  nobody is using and the wrong amount for one about to be.
- **A term is per queue, and there is no log to catch up on.** A candidate
  that is behind is refused and stays behind; nothing copies it the records it
  is missing. In a cluster that has been up long enough for a majority to have
  everything this is a distinction without a difference, and in one that has
  not, it means the election waits for a node that is ahead to be asked for
  the queue.
- **A copy is a file, not a queue.** The node keeping it does not serve it,
  count it, or list it. Making it serve is a restart with a different
  `peers`.
- **A cluster is a list, not a membership.** Peers are given on the command
  line, in the same order on every node, and nothing joins or leaves while it
  is running. Peers are addresses, because Rill's sockets have no name
  resolution.
- **A proxy asks one thing at a time.** A queue standing in for a remote one
  waits for each answer before sending the next question, so one queue's
  traffic across a link is a round trip deep rather than a pipeline.
- **The routing journal is never rewritten.** It gains a record per binding
  made and per binding taken away, and nothing ever shortens it. A queue's
  journal is compacted; this one is not, on the grounds that a table which
  changes as often as messages arrive is not a routing table.
- **A queue is still made the first time it is named, over AMQP too.**
  Exchanges are not any more, but `Basic.Consume` on a queue nobody declared
  makes it and then hands over nothing, where RabbitMQ answers 404. It is the
  same silence the exchange had, and the same fix; it is not done because a
  queue is made on demand by the cluster and by replay as well as by a client,
  and telling those apart is the work.
- **`Basic.Qos` counts per consumer, not per channel.** Two consumers made on
  one channel each get the window the channel asked for, rather than sharing
  it, and `global=true` is read and treated as `false`. For the usual shape —
  one consumer to a channel — these are the same thing.
- **A consumer that answers for nothing has no window.** Prefetch does not
  apply under `no-ack`, which is what the specification says and what RabbitMQ
  does, so such a consumer is given whatever the connection will take as fast
  as it will take it. Nothing is between a fast queue and a slow reader but
  the socket, and that is what the flag asks for.
- **No `headers` exchange, and no `Queue.Unbind` over AMQP.** The exchange
  understands unbinding, and a replayed journal can ask it; nothing else does.
- **`durable` is not a choice.** Every exchange and every binding is written
  down, whatever the flag said, exactly as every queue is.
- **One virtual host, and one permission.** `Connection.Open` takes whatever
  virtual host it is given. A name and a word are checked; what that person
  may then do is not, beyond a tag the management pages read.
- **TLS is server-side only, and there is no client certificate.** A client
  proves nothing about itself but its password, and one node proves nothing to
  another but `peer=`.
- **No flow control back to publishers, only a wall.** A publisher that
  outruns its consumers is refused rather than slowed, so it finds out by
  being told no rather than by being made to wait.
- **The cluster checks want a quiet machine.** `majority.sh` and
  `failover.sh` publish and wait on deadlines of a second or two. On a machine
  also running RabbitMQ, a .NET build and three other brokers they time out,
  and on a quiet one they pass sixty runs out of sixty. Whether that is only
  load or whether there is a lost wake-up behind it is not settled: a reply
  was once observed sitting in a connection's channel until the next command
  arrived and shook it loose, and that has not been reproduced since.
- **Nothing is measured over time.** `QUEUES` says what is true now; there is
  no rate, no age of the oldest message, and no way to see how big a queue's
  journal has grown.
