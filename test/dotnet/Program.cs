// The acceptance test: RabbitMQ's own .NET client, pointed at RillMQ.
//
//     dotnet run --project test/dotnet -- <port>
//
// Nothing here knows it is not talking to RabbitMQ, which is the point.
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

class Program
{
    static int passed = 0;
    static int failed = 0;

    static void Check(string name, object got, object want)
    {
        if (Equals(got?.ToString(), want?.ToString()))
        {
            Console.WriteLine($"ok   {name}");
            passed++;
        }
        else
        {
            Console.WriteLine($"FAIL {name}: wanted `{want}`, got `{got}`");
            failed++;
        }
    }

    // Two consumers on one channel with a prefetch of two, given ten and
    // acknowledging none. What they end up holding is the whole answer.
    static int Outstanding(IConnection conn, bool shared)
    {
        using var och = conn.CreateModel();
        var oq = och.QueueDeclare("", false, true, true, null).QueueName;
        och.BasicQos(0, 2, global: shared);
        var held = 0;
        for (var i = 0; i < 2; i++)
        {
            var ocon = new EventingBasicConsumer(och);
            ocon.Received += (_, e) => Interlocked.Increment(ref held);
            och.BasicConsume(oq, false, "o" + i, ocon);
        }
        for (var i = 0; i < 10; i++) och.BasicPublish("", oq, null, Encoding.UTF8.GetBytes("m"));
        Thread.Sleep(1200);
        return Volatile.Read(ref held);
    }

    static int Main(string[] args)
    {
        int port = args.Length > 0 ? int.Parse(args[0]) : 5672;
        string user = Environment.GetEnvironmentVariable("RILLMQ_USER") ?? "guest";
        string word = Environment.GetEnvironmentVariable("RILLMQ_PASS") ?? "guest";

        // `auth <user> <word>` is the whole of that mode: it says whether the
        // broker let this pair in, and nothing else. The suite runs it twice,
        // once with the word and once with the wrong one.
        // `xdel make|gone <exchange>` is the two halves of one question asked
        // either side of a restart: does an exchange somebody deleted stay
        // deleted? The routing journal replays declarations and bindings, so
        // it has to replay the deletion too.
        if (args.Length > 3 && args[1] == "xdel")
        {
            var xf = new ConnectionFactory { HostName = "127.0.0.1", Port = port, UserName = user, Password = word, VirtualHost = "/" };
            using var xc = xf.CreateConnection();
            using var xm = xc.CreateModel();
            string name = args[3];
            if (args[2] == "make")
            {
                xm.ExchangeDeclare(name, "fanout", true);
                xm.QueueDeclare(name + "-q", true, false, false, null);
                xm.QueueBind(name + "-q", name, "");
                xm.ExchangeDelete(name);
                Console.WriteLine("deleted");
            }
            else
            {
                var said = new BlockingCollection<string>();
                xm.ModelShutdown += (_, e) => said.Add(e.ReplyCode.ToString());
                try { xm.BasicPublish(name, "", null, Encoding.UTF8.GetBytes("x")); Thread.Sleep(700); } catch (Exception) { }
                Console.WriteLine(said.TryTake(out var w, 3000) ? w : "still there");
            }
            return 0;
        }

        // `blocked <queue>` fills the broker until it is over whatever `mem=`
        // it was started with, and says what the client was told on the way.
        // A broker with no room left used to answer individual publishes with
        // an error; AMQP has a way of saying "stop, I am full" to the whole
        // connection instead, and RabbitMQ.Client raises it as an event.
        if (args.Length > 2 && args[1] == "blocked")
        {
            var bf = new ConnectionFactory { HostName = "127.0.0.1", Port = port, UserName = user, Password = word, VirtualHost = "/" };
            using var bc = bf.CreateConnection();
            var heard = new BlockingCollection<string>();
            bc.ConnectionBlocked += (_, e) => heard.Add("blocked: " + e.Reason);
            bc.ConnectionUnblocked += (_, _) => heard.Add("unblocked");
            using var bm = bc.CreateModel();
            bm.QueueDeclare(args[2], true, false, false, null);
            var fat = new byte[64 * 1024];
            for (int i = 0; i < 40000 && heard.Count == 0; i++)
            {
                bm.BasicPublish("", args[2], null, fat);
                if (i % 200 == 0) Thread.Sleep(1);
            }
            Console.WriteLine(heard.TryTake(out var w, 4000) ? w : "(nothing was said)");
            return 0;
        }

        // `cancelnotify` on its own, so the same question can be put to
        // RabbitMQ and to RillMQ and the answers compared.
        if (args.Length > 1 && args[1] == "cancelnotify")
        {
            var nf = new ConnectionFactory { HostName = "127.0.0.1", Port = port, UserName = user, Password = word, VirtualHost = "/" };
            using var nc = nf.CreateConnection();
            using var nm = nc.CreateModel();
            string cnq = "cancel-" + DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            nm.QueueDeclare(cnq, true, false, false, null);
            var told = new BlockingCollection<string>();
            var ncons = new EventingBasicConsumer(nm);
            // `Unregistered` is the answer to a cancel this client asked for.
            // A cancel the *broker* sends arrives as `ConsumerCancelled`.
            ncons.ConsumerCancelled += (_, e) => told.Add("cancelled");
            nm.BasicConsume(cnq, true, ncons);
            Thread.Sleep(500);
            // From another connection, because a broker has no reason to tell
            // the connection that asked for the deletion anything it does not
            // already know — and RabbitMQ does not.
            using (var other = nf.CreateConnection())
            using (var om = other.CreateModel()) om.QueueDelete(cnq);
            Console.WriteLine(told.TryTake(out var w2, 4000) ? w2 : "(nothing)");
            return 0;
        }

        // `churn <n>` binds and unbinds the same queue n times. Every one of
        // those is a record in the routing journal and none of them survives,
        // so the file is a measure of how much a table that changes leaves
        // behind.
        if (args.Length > 2 && args[1] == "churn")
        {
            var chf = new ConnectionFactory { HostName = "127.0.0.1", Port = port, UserName = user, Password = word, VirtualHost = "/" };
            using var chc = chf.CreateConnection();
            using var chm = chc.CreateModel();
            chm.ExchangeDeclare("churn-x", "direct", true);
            chm.QueueDeclare("churn-q", true, false, false, null);
            int rounds = int.Parse(args[2]);
            for (int i = 0; i < rounds; i++)
            {
                chm.QueueBind("churn-q", "churn-x", "k");
                chm.QueueUnbind("churn-q", "churn-x", "k");
            }
            Thread.Sleep(1500);
            Console.WriteLine("churned " + rounds);
            return 0;
        }

        // `unrouted <exchange> <key>` says whether a mandatory publish came
        // back, which is how to ask whether anything is bound without asking
        // the broker to list its bindings.
        if (args.Length > 3 && args[1] == "unrouted")
        {
            var uf = new ConnectionFactory { HostName = "127.0.0.1", Port = port, UserName = user, Password = word, VirtualHost = "/" };
            using var uc = uf.CreateConnection();
            using var um = uc.CreateModel();
            var returns = new BlockingCollection<string>();
            um.BasicReturn += (_, e) => returns.Add("came back");
            um.BasicPublish(args[2], args[3], true, null, Encoding.UTF8.GetBytes("x"));
            Console.WriteLine(returns.TryTake(out var u, 4000) ? u : "went somewhere");
            return 0;
        }

        // `sharedqos <true|false>` asks what `global` on `Basic.Qos` buys: two
        // consumers on one channel, a count of two, and nothing acknowledged.
        // Shared, the pair holds two between them; not shared, two each.
        if (args.Length > 2 && args[1] == "sharedqos")
        {
            var sf = new ConnectionFactory { HostName = "127.0.0.1", Port = port, UserName = user, Password = word, VirtualHost = "/" };
            using var sc = sf.CreateConnection();
            using var sm = sc.CreateModel();
            var sqn = sm.QueueDeclare("", false, true, true, null).QueueName;
            sm.BasicQos(0, 2, global: args[2] == "true");
            var held = 0;
            for (var i = 0; i < 2; i++)
            {
                var scon = new EventingBasicConsumer(sm);
                scon.Received += (_, e) => Interlocked.Increment(ref held);
                sm.BasicConsume(sqn, false, "s" + i, scon);
            }
            for (var i = 0; i < 10; i++) sm.BasicPublish("", sqn, null, Encoding.UTF8.GetBytes("m"));
            Thread.Sleep(1500);
            Console.WriteLine("held=" + Volatile.Read(ref held));
            return 0;
        }

        // `declareonly <name> <excl> <auto>` declares one queue and stops, so
        // that what is on the disk afterwards can be looked at.
        if (args.Length > 4 && args[1] == "declareonly")
        {
            var qf = new ConnectionFactory { HostName = "127.0.0.1", Port = port, UserName = user, Password = word, VirtualHost = "/" };
            using var qc = qf.CreateConnection();
            using var qm = qc.CreateModel();
            var qn = qm.QueueDeclare(args[2] == "-" ? "" : args[2], false, args[3] == "true", args[4] == "true", null).QueueName;
            Console.WriteLine(qn);
            Thread.Sleep(400);
            return 0;
        }

        // `multiack` acks five deliveries with one `Basic.Ack` carrying the
        // `multiple` bit, then asks the queue what it still thinks is out.
        if (args.Length > 1 && args[1] == "multiack")
        {
            var kf = new ConnectionFactory { HostName = "127.0.0.1", Port = port, UserName = user, Password = word, VirtualHost = "/" };
            using var kc = kf.CreateConnection();
            using var km = kc.CreateModel();
            var kq = "multiack-" + Environment.TickCount;
            km.QueueDeclare(kq, true, false, false, null);
            for (var i = 0; i < 5; i++) km.BasicPublish("", kq, null, Encoding.UTF8.GetBytes("m" + i));
            Thread.Sleep(500);
            km.BasicQos(0, 5, false);
            var kgot = new BlockingCollection<ulong>();
            var kcon = new EventingBasicConsumer(km);
            kcon.Received += (_, e) => kgot.Add(e.DeliveryTag);
            var ktag = km.BasicConsume(kq, false, "", kcon);
            ulong last = 0;
            for (var i = 0; i < 5; i++) if (kgot.TryTake(out var t, 3000)) last = t;
            km.BasicAck(last, multiple: true);
            Thread.Sleep(700);
            km.BasicCancel(ktag);
            Thread.Sleep(300);
            using var km2 = kc.CreateModel();
            Console.WriteLine("left=" + km2.QueueDeclare(kq, true, false, false, null).MessageCount);
            km2.QueueDelete(kq);
            return 0;
        }

        // `innerpub <exchange>` publishes to one exchange and says what came
        // back, which is how the restart suite asks whether `internal`
        // survived being written down and read again.
        if (args.Length > 2 && args[1] == "innerpub")
        {
            var pf = new ConnectionFactory { HostName = "127.0.0.1", Port = port, UserName = user, Password = word, VirtualHost = "/" };
            using var pc = pf.CreateConnection();
            using var pm = pc.CreateModel();
            var pstop = new BlockingCollection<string>();
            pm.ModelShutdown += (_, e) => pstop.Add(e.ReplyCode.ToString());
            try { pm.BasicPublish(args[2], "", null, Encoding.UTF8.GetBytes("x")); Thread.Sleep(400); } catch (Exception) { }
            Console.WriteLine(pstop.TryTake(out var pc2, 3000) ? pc2 : "took it");
            return 0;
        }

        // `vhost <open|split|name> [host]` is the virtual host, which is a
        // namespace and nothing else: the same queue name in two of them is
        // two queues, and a host this broker was not told about is refused
        // rather than made.
        if (args.Length > 2 && args[1] == "vhost")
        {
            if (args[2] == "open")
            {
                var vf = new ConnectionFactory { HostName = "127.0.0.1", Port = port, UserName = user, Password = word, VirtualHost = args[3] };
                try { using var vc = vf.CreateConnection(); Console.WriteLine("in"); }
                catch (Exception) { Console.WriteLine("out"); }
                return 0;
            }
            if (args[2] == "may")
            {
                var yf = new ConnectionFactory { HostName = "127.0.0.1", Port = port, UserName = user, Password = word, VirtualHost = args[3] };
                using var yc = yf.CreateConnection();
                var said = new List<string>();
                foreach (var what in new[] { "declare", "publish", "consume" })
                {
                    using var ym = yc.CreateModel();
                    var stop = new BlockingCollection<string>();
                    ym.ModelShutdown += (_, e) => stop.Add(e.ReplyCode.ToString());
                    try
                    {
                        if (what == "declare") ym.QueueDeclare("rights-q", true, false, false, null);
                        else if (what == "publish") ym.BasicPublish("", "rights-q", null, Encoding.UTF8.GetBytes("x"));
                        else ym.BasicConsume("rights-q", true, "", new EventingBasicConsumer(ym));
                    }
                    catch (Exception) { }
                    Thread.Sleep(300);
                    said.Add(what + "=" + (stop.TryTake(out var sc, 1500) ? sc : "yes"));
                }
                Console.WriteLine(string.Join(" ", said));
                return 0;
            }
            if (args[2] == "name")
            {
                var nf = new ConnectionFactory { HostName = "127.0.0.1", Port = port, UserName = user, Password = word, VirtualHost = args[3] };
                using var nc = nf.CreateConnection();
                using var nm = nc.CreateModel();
                Console.WriteLine(nm.QueueDeclare("vh-named", true, false, false, null).QueueName);
                return 0;
            }
            var af = new ConnectionFactory { HostName = "127.0.0.1", Port = port, UserName = user, Password = word, VirtualHost = "/" };
            var bf = new ConnectionFactory { HostName = "127.0.0.1", Port = port, UserName = user, Password = word, VirtualHost = args[3] };
            using var ac2 = af.CreateConnection();
            using var bc2 = bf.CreateConnection();
            using var am2 = ac2.CreateModel();
            using var bm2 = bc2.CreateModel();
            am2.QueueDeclare("vh-shared", true, false, false, null);
            bm2.QueueDeclare("vh-shared", true, false, false, null);
            am2.BasicPublish("", "vh-shared", null, Encoding.UTF8.GetBytes("home"));
            Thread.Sleep(500);
            var mine = bm2.BasicGet("vh-shared", true);
            var his = am2.BasicGet("vh-shared", true);
            Console.WriteLine("there=" + (mine == null ? "(nothing)" : Encoding.UTF8.GetString(mine.Body.ToArray()))
                + " home=" + (his == null ? "(nothing)" : Encoding.UTF8.GetString(his.Body.ToArray())));
            return 0;
        }

        // `autodel` walks the auto-delete case one step at a time and prints
        // what it sees, for when the acceptance check says it did not happen.
        if (args.Length > 1 && args[1] == "autodel")
        {
            var df = new ConnectionFactory { HostName = "127.0.0.1", Port = port, UserName = user, Password = word, VirtualHost = "/" };
            using var dc = df.CreateConnection();
            using var dm = dc.CreateModel();
            var dq = dm.QueueDeclare("", false, true, true, null).QueueName;
            Console.WriteLine("declared " + dq);
            var dtag = dm.BasicConsume(dq, true, "", new EventingBasicConsumer(dm));
            Thread.Sleep(300);
            dm.BasicCancel(dtag);
            Thread.Sleep(600);
            var dgone = new BlockingCollection<string>();
            using var dm2 = dc.CreateModel();
            dm2.ModelShutdown += (_, e) => dgone.Add(e.ReplyCode + " " + e.ReplyText);
            try { dm2.BasicConsume(dq, true, "", new EventingBasicConsumer(dm2)); } catch (Exception) { }
            Console.WriteLine("after cancel: " + (dgone.TryTake(out var dg, 3000) ? dg : "still there"));
            return 0;
        }

        // `manycons` is two consumers on one channel, taking from one queue.
        // A worker pool is usually written this way, and the question is
        // whether each delivery arrives under the tag of the consumer it was
        // actually given to.
        if (args.Length > 1 && args[1] == "manycons")
        {
            var mf = new ConnectionFactory { HostName = "127.0.0.1", Port = port, UserName = user, Password = word, VirtualHost = "/" };
            using var mc = mf.CreateConnection();
            using var mm = mc.CreateModel();
            var mqn = mm.QueueDeclare("", false, true, true, null).QueueName;
            mm.BasicQos(0, 1, false);
            var tags = new BlockingCollection<string>();
            for (var i = 0; i < 2; i++)
            {
                var mcon = new EventingBasicConsumer(mm);
                mcon.Received += (_, e) => { tags.Add(e.ConsumerTag); mm.BasicAck(e.DeliveryTag, false); };
                mm.BasicConsume(mqn, false, "w" + i, mcon);
            }
            for (var i = 0; i < 6; i++) mm.BasicPublish("", mqn, null, Encoding.UTF8.GetBytes("m"));
            var tally = new SortedDictionary<string, int>();
            for (var i = 0; i < 6; i++)
                if (tags.TryTake(out var mt, 3000)) tally[mt] = tally.TryGetValue(mt, out var mn) ? mn + 1 : 1;
            Console.WriteLine(string.Join(" ", tally.Select(kv => kv.Key + "=" + kv.Value)));
            return 0;
        }

        // `transient make|check` is the two halves of one question either side
        // of a restart: does a queue a client asked not to keep, stay?
        if (args.Length > 2 && args[1] == "transient")
        {
            var tf2 = new ConnectionFactory { HostName = "127.0.0.1", Port = port, UserName = user, Password = word, VirtualHost = "/" };
            using var tc2 = tf2.CreateConnection();
            using var tm2 = tc2.CreateModel();
            if (args[2] == "make")
            {
                tm2.QueueDeclare("lasting-q", durable: true, false, false, null);
                tm2.QueueDeclare("passing-q", durable: false, false, false, null);
                tm2.BasicPublish("", "lasting-q", null, Encoding.UTF8.GetBytes("keep"));
                tm2.BasicPublish("", "passing-q", null, Encoding.UTF8.GetBytes("lose"));
                Thread.Sleep(500);
                Console.WriteLine("made");
            }
            else
            {
                var gone = new BlockingCollection<string>();
                tm2.ModelShutdown += (_, e) => gone.Add(e.ReplyCode.ToString());
                uint kept = 0;
                try { kept = tm2.QueueDeclare("lasting-q", true, false, false, null).MessageCount; } catch (Exception) { }
                using var tm3 = tc2.CreateModel();
                var gone2 = new BlockingCollection<string>();
                tm3.ModelShutdown += (_, e) => gone2.Add(e.ReplyCode.ToString());
                try { tm3.BasicGet("passing-q", true); } catch (Exception) { }
                Console.WriteLine("kept=" + kept + " passing=" + (gone2.TryTake(out var g3, 3000) ? g3 : "still there"));
            }
            return 0;
        }

        // `tls` connects over AMQPS and says whether it got in. The
        // certificate is one RillMQ made for itself, so the name is checked
        // and the chain is not: this says TLS works, not that a self-signed
        // certificate is trustworthy.
        if (args.Length > 1 && args[1] == "tls")
        {
            var tf = new ConnectionFactory { HostName = "localhost", Port = port, UserName = user, Password = word, VirtualHost = "/", RequestedConnectionTimeout = TimeSpan.FromSeconds(10) };
            tf.Ssl.Enabled = true;
            tf.Ssl.ServerName = "localhost";
            tf.Ssl.AcceptablePolicyErrors = System.Net.Security.SslPolicyErrors.RemoteCertificateChainErrors | System.Net.Security.SslPolicyErrors.RemoteCertificateNameMismatch;
            tf.Ssl.CertificateValidationCallback = (_, _, _, _) => true;
            try
            {
                using var tc = tf.CreateConnection();
                using var tch = tc.CreateModel();
                string tq = "tls-" + DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                tch.QueueDeclare(tq, true, false, false, null);
                tch.BasicPublish("", tq, null, Encoding.UTF8.GetBytes("over tls"));
                Thread.Sleep(400);
                var over = tch.BasicGet(tq, true);
                Console.WriteLine(over == null ? "no message" : Encoding.UTF8.GetString(over.Body.ToArray()));
            }
            catch (Exception e)
            {
                Console.WriteLine("failed: " + e.Message + (e.InnerException == null ? "" : " / " + e.InnerException.GetType().Name + ": " + e.InnerException.Message));
            }
            return 0;
        }

        if (args.Length > 3 && args[1] == "auth")
        {
            var af = new ConnectionFactory { HostName = "127.0.0.1", Port = port, UserName = args[2], Password = args[3], VirtualHost = "/", RequestedConnectionTimeout = TimeSpan.FromSeconds(5) };
            try
            {
                using var ac = af.CreateConnection();
                Console.WriteLine(ac.IsOpen ? "in" : "out");
            }
            catch (Exception)
            {
                Console.WriteLine("out");
            }
            return 0;
        }

        var factory = new ConnectionFactory
        {
            HostName = "127.0.0.1",
            Port = port,
            UserName = user,
            Password = word,
            VirtualHost = "/",
            RequestedHeartbeat = TimeSpan.Zero,
        };

        using var conn = factory.CreateConnection();

        // `benchc <n>` is the same measurement with the message marked
        // persistent and the channel in confirm mode: both brokers are then
        // being asked for the same promise, which is the only way the two
        // numbers mean the same thing.
        if (args.Length > 2 && args[1] == "benchc")
        {
            int count = int.Parse(args[2]);
            using var dch = conn.CreateModel();
            string dq = "benchc-" + DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            dch.QueueDeclare(dq, durable: true, exclusive: false, autoDelete: false, arguments: null);
            var props = dch.CreateBasicProperties();
            props.Persistent = true;
            var payload = new byte[64];
            dch.ConfirmSelect();
            var d0 = DateTime.UtcNow;
            for (int i = 0; i < count; i++) dch.BasicPublish("", dq, props, payload);
            // The two halves separately: how long the client took to write them
            // all, and how much longer it then waited to be told they were
            // safe. Together they are the number; apart they say which end the
            // time is being spent at.
            var dw = DateTime.UtcNow;
            dch.WaitForConfirms(TimeSpan.FromMinutes(5));
            var d1 = DateTime.UtcNow;
            Console.WriteLine($"durable publish: {count} in {(int)(d1 - d0).TotalMilliseconds} ms, {(int)(count / (d1 - d0).TotalSeconds)}/sec (wrote in {(int)(dw - d0).TotalMilliseconds} ms, then waited {(int)(d1 - dw).TotalMilliseconds} ms)");
            return 0;
        }

        // `bench <n>` measures, over the same client anything else would use.
        if (args.Length > 2 && args[1] == "bench")
        {
            int count = int.Parse(args[2]);
            using var bch = conn.CreateModel();
            string bq = "bench-" + DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            bch.QueueDeclare(bq, true, false, false, null);
            var payload = new byte[64];
            var t0 = DateTime.UtcNow;
            for (int i = 0; i < count; i++) bch.BasicPublish("", bq, null, payload);
            while (bch.QueueDeclare(bq, true, false, false, null).MessageCount < count) Thread.Sleep(50);
            var t1 = DateTime.UtcNow;
            // `bench <n> auto` takes the acknowledgements out of it: the
            // broker sends and the client never answers. What is left is
            // delivery alone, which is how much of the cost the answering was.
            bool auto = args.Length > 3 && args[3] == "auto";
            // `thin` takes the client's own bookkeeping out of it: an interlocked
            // counter instead of a blocking queue handed between two threads.
            // If the rate moves, the ceiling was this program's and not the
            // broker's, which is the only way to tell from out here.
            bool thin = args.Length > 4 && args[4] == "thin";
            var seen2 = new BlockingCollection<int>();
            int counted = 0;
            bch.BasicQos(0, 64, false);
            var c2 = new EventingBasicConsumer(bch);
            if (thin)
                c2.Received += (_, ea) => { Interlocked.Increment(ref counted); if (!auto) bch.BasicAck(ea.DeliveryTag, false); };
            else
                c2.Received += (_, ea) => { seen2.Add(1); if (!auto) bch.BasicAck(ea.DeliveryTag, false); };
            bch.BasicConsume(bq, auto, c2);
            int n2 = 0;
            var end = DateTime.UtcNow.AddSeconds(120);
            if (thin)
            {
                while (Volatile.Read(ref counted) < count && DateTime.UtcNow < end) Thread.Sleep(0);
                n2 = Volatile.Read(ref counted);
            }
            else while (n2 < count && DateTime.UtcNow < end) if (seen2.TryTake(out _, 1000)) n2++;
            var t2 = DateTime.UtcNow;
            Console.WriteLine($"publish: {count} in {(int)(t1 - t0).TotalMilliseconds} ms, {(int)(count / (t1 - t0).TotalSeconds)}/sec");
            Console.WriteLine($"deliver and acknowledge: {n2} in {(int)(t2 - t1).TotalMilliseconds} ms, {(int)(n2 / (t2 - t1).TotalSeconds)}/sec");
            return 0;
        }

        // `pub <queue> <n>` publishes and leaves, for checking that what AMQP
        // put in a queue is still there after the broker has been restarted.
        if (args.Length > 3 && args[1] == "pub")
        {
            using var pch = conn.CreateModel();
            pch.QueueDeclare(args[2], true, false, false, null);
            for (int i = 0; i < int.Parse(args[3]); i++)
                pch.BasicPublish("", args[2], null, Encoding.UTF8.GetBytes($"amqp message {i}"));
            Thread.Sleep(600);
            Console.WriteLine($"published {args[3]} to {args[2]} over amqp");
            return 0;
        }

        Check("the connection opens", conn.IsOpen, true);

        using var ch = conn.CreateModel();
        Check("a channel opens", ch.IsOpen, true);

        string q = "dotnet-" + DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var declared = ch.QueueDeclare(queue: q, durable: true, exclusive: false, autoDelete: false, arguments: null);
        Check("declaring a queue answers with its name", declared.QueueName, q);

        const int n = 500;
        var body = Encoding.UTF8.GetBytes("hello from dotnet");
        for (int i = 0; i < n; i++)
            ch.BasicPublish(exchange: "", routingKey: q, basicProperties: null, body: body);

        Thread.Sleep(700);
        var again = ch.QueueDeclare(queue: q, durable: true, exclusive: false, autoDelete: false, arguments: null);
        Check($"{n} published, and the queue says so", again.MessageCount, (uint)n);

        var seen = new BlockingCollection<string>();
        ch.BasicQos(prefetchSize: 0, prefetchCount: 32, global: false);
        var consumer = new EventingBasicConsumer(ch);
        consumer.Received += (_, ea) =>
        {
            seen.Add(Encoding.UTF8.GetString(ea.Body.ToArray()));
            ch.BasicAck(ea.DeliveryTag, multiple: false);
        };
        ch.BasicConsume(queue: q, autoAck: false, consumer: consumer);

        int got = 0;
        string first = null;
        var deadline = DateTime.UtcNow.AddSeconds(25);
        while (got < n && DateTime.UtcNow < deadline)
            if (seen.TryTake(out var s, 500)) { if (got == 0) first = s; got++; }
        Check("the body arrives unchanged", first, "hello from dotnet");
        Check($"all {n} came back", got, n);

        Thread.Sleep(700);
        var empty = ch.QueueDeclare(queue: q, durable: true, exclusive: false, autoDelete: false, arguments: null);
        Check("and the queue is empty afterwards", empty.MessageCount, 0u);

        // A queue of its own: the consumer above is still subscribed to q and
        // would take this before BasicGet could, which is true of RabbitMQ too.
        string g = q + "-get";
        ch.QueueDeclare(queue: g, durable: true, exclusive: false, autoDelete: false, arguments: null);
        ch.BasicPublish(exchange: "", routingKey: g, basicProperties: null, body: Encoding.UTF8.GetBytes("just one"));
        Thread.Sleep(500);
        var one = ch.BasicGet(g, autoAck: true);
        Check("BasicGet takes one", one == null ? "(nothing)" : Encoding.UTF8.GetString(one.Body.ToArray()), "just one");

        // Publisher confirms: the broker says when a message is on the disk,
        // which is the one thing an AMQP publisher cannot otherwise learn.
        using var cch = conn.CreateModel();
        string cq = q + "-confirm";
        cch.QueueDeclare(cq, true, false, false, null);
        cch.ConfirmSelect();
        for (int i = 0; i < 200; i++)
            cch.BasicPublish("", cq, null, Encoding.UTF8.GetBytes("durable " + i));
        bool all = cch.WaitForConfirms(TimeSpan.FromSeconds(20));
        Check("every publish is confirmed once it is durable", all, true);
        Check("and the queue holds them", cch.QueueDeclare(cq, true, false, false, null).MessageCount, 200u);

        // Exchanges: a topic exchange with two bindings, and a fanout.
        string tx = q + "-topic";
        string qa = q + "-errs";
        string qb = q + "-app";
        ch.ExchangeDeclare(tx, ExchangeType.Topic, durable: true);
        ch.QueueDeclare(qa, true, false, false, null);
        ch.QueueDeclare(qb, true, false, false, null);
        ch.QueueBind(qa, tx, "*.error");
        ch.QueueBind(qb, tx, "app.#");
        foreach (var key in new[] { "app.error", "db.error", "app.db.warn" })
            ch.BasicPublish(tx, key, null, Encoding.UTF8.GetBytes(key));
        Thread.Sleep(700);
        Check("a topic binding takes the keys it matches",
            ch.QueueDeclare(qa, true, false, false, null).MessageCount, 2u);
        Check("and a hash takes everything under a word",
            ch.QueueDeclare(qb, true, false, false, null).MessageCount, 2u);

        string fx = q + "-fan";
        string fa = q + "-f1";
        string fb = q + "-f2";
        ch.ExchangeDeclare(fx, ExchangeType.Fanout, durable: true);
        ch.QueueDeclare(fa, true, false, false, null);
        ch.QueueDeclare(fb, true, false, false, null);
        ch.QueueBind(fa, fx, "");
        ch.QueueBind(fb, fx, "");
        ch.BasicPublish(fx, "ignored", null, Encoding.UTF8.GetBytes("both"));
        Thread.Sleep(700);
        Check("a fanout reaches every queue bound to it",
            ch.QueueDeclare(fa, true, false, false, null).MessageCount + "," + ch.QueueDeclare(fb, true, false, false, null).MessageCount,
            "1,1");

        // `no-ack`, where the client says it will not answer for what it is
        // given. The broker must neither wait for an answer nor stop after one
        // prefetch window, which is what it did when it read the flag and
        // ignored it: a consumer got sixty-four messages and then silence.
        string nq = q + "-noack";
        ch.QueueDeclare(nq, true, false, false, null);
        for (int i = 0; i < 300; i++) ch.BasicPublish("", nq, null, Encoding.UTF8.GetBytes("no answer " + i));
        Thread.Sleep(500);
        var quick = new BlockingCollection<int>();
        using (var nch = conn.CreateModel())
        {
            nch.BasicQos(0, 64, false);
            var nc = new EventingBasicConsumer(nch);
            nc.Received += (_, ea) => quick.Add(1);
            nch.BasicConsume(nq, true, nc);
            int quickly = 0;
            var until = DateTime.UtcNow.AddSeconds(10);
            while (quickly < 300 && DateTime.UtcNow < until) if (quick.TryTake(out _, 500)) quickly++;
            Check("a consumer that answers for nothing gets all of it", quickly, 300);
        }
        Thread.Sleep(300);
        Check("and the queue keeps none of it back",
            ch.QueueDeclare(nq, true, false, false, null).MessageCount, 0u);

        // Channels. A connection may have several, each with its own consumers
        // and its own `Basic.Qos`, and what happens on one is not supposed to
        // happen on another.
        string ca = q + "-chan-a";
        string cb = q + "-chan-b";
        using (var m1 = conn.CreateModel())
        using (var m2 = conn.CreateModel())
        {
            m1.QueueDeclare(ca, true, false, false, null);
            m2.QueueDeclare(cb, true, false, false, null);
            for (int i = 0; i < 5; i++) m1.BasicPublish("", ca, null, Encoding.UTF8.GetBytes("a" + i));
            for (int i = 0; i < 5; i++) m1.BasicPublish("", cb, null, Encoding.UTF8.GetBytes("b" + i));
            Thread.Sleep(400);

            var onA = new BlockingCollection<string>();
            var onB = new BlockingCollection<string>();
            var k1 = new EventingBasicConsumer(m1);
            k1.Received += (_, ea) => { onA.Add(Encoding.UTF8.GetString(ea.Body.ToArray())); m1.BasicAck(ea.DeliveryTag, false); };
            var k2 = new EventingBasicConsumer(m2);
            k2.Received += (_, ea) => { onB.Add(Encoding.UTF8.GetString(ea.Body.ToArray())); m2.BasicAck(ea.DeliveryTag, false); };
            m1.BasicConsume(ca, false, k1);
            m2.BasicConsume(cb, false, k2);
            Thread.Sleep(900);
            Check("each channel's consumer gets its own queue", onA.Count + "," + onB.Count, "5,5");
            Check("and nothing crossed over", onA.All(x => x[0] == 'a') && onB.All(x => x[0] == 'b'), true);
        }

        // `Basic.Qos` with a count of one: the broker may have exactly one
        // message out at a time on that channel. Answering `Qos-Ok` and then
        // handing out sixty-four anyway is not fair dispatch, and is what it
        // did until this was written.
        string fq = q + "-qos";
        using (var m3 = conn.CreateModel())
        {
            m3.QueueDeclare(fq, true, false, false, null);
            for (int i = 0; i < 20; i++) m3.BasicPublish("", fq, null, Encoding.UTF8.GetBytes("f" + i));
            Thread.Sleep(400);
            int most = 0, outNow = 0;
            var held = new object();
            var qc = new EventingBasicConsumer(m3);
            qc.Received += (_, ea) =>
            {
                lock (held) { outNow++; if (outNow > most) most = outNow; }
                Thread.Sleep(15);
                lock (held) { outNow--; }
                m3.BasicAck(ea.DeliveryTag, false);
            };
            m3.BasicQos(0, 1, false);
            m3.BasicConsume(fq, false, qc);
            Thread.Sleep(1500);
            Check("a prefetch of one means one at a time", most, 1);
        }

        // The four methods that used to be answered with 540. Three had the
        // machinery already — purge is the same request as the native
        // protocol's DRAIN, unbind is what a replayed journal has always been
        // able to ask an exchange — and nothing over AMQP could reach any of
        // them.
        string mq = q + "-methods";
        ch.QueueDeclare(mq, true, false, false, null);
        for (int i = 0; i < 7; i++) ch.BasicPublish("", mq, null, Encoding.UTF8.GetBytes("m" + i));
        Thread.Sleep(400);
        Check("purging a queue says how much it threw away", ch.QueuePurge(mq), 7u);
        Check("and leaves it empty", ch.QueueDeclare(mq, true, false, false, null).MessageCount, 0u);

        string ux = q + "-unbind-x";
        string uq = q + "-unbind-q";
        ch.ExchangeDeclare(ux, "direct", true);
        ch.QueueDeclare(uq, true, false, false, null);
        ch.QueueBind(uq, ux, "k");
        ch.BasicPublish(ux, "k", null, Encoding.UTF8.GetBytes("bound"));
        Thread.Sleep(400);
        Check("a binding carries a message", ch.QueueDeclare(uq, true, false, false, null).MessageCount, 1u);
        ch.QueueUnbind(uq, ux, "k");
        ch.BasicPublish(ux, "k", null, Encoding.UTF8.GetBytes("unbound"));
        Thread.Sleep(400);
        Check("and unbinding stops it", ch.QueueDeclare(uq, true, false, false, null).MessageCount, 1u);

        Check("deleting a queue says what was in it", ch.QueueDelete(uq), 1u);
        Check("and it is gone", refused(m => m.BasicGet(uq, true)).Split(' ')[0], "404");

        ch.ExchangeDelete(ux);
        Check("deleting an exchange leaves nothing to publish to",
            refused(m => { m.BasicPublish(ux, "k", null, Encoding.UTF8.GetBytes("x")); Thread.Sleep(700); }).Split(' ')[0],
            "404");

        // `mandatory`: a publisher saying it would rather have the message
        // back than have it disappear. Unroutable is not an error — the
        // channel stays up — so this is the one thing a publisher can be told
        // that is neither a refusal nor a confirmation.
        var returned = new BlockingCollection<string>();
        using (var rm = conn.CreateModel())
        {
            rm.BasicReturn += (_, ea) => returned.Add(ea.ReplyCode + " " + ea.ReplyText + " " + Encoding.UTF8.GetString(ea.Body.ToArray()));
            string rx = q + "-return-x";
            rm.ExchangeDeclare(rx, "direct", true);
            rm.BasicPublish(rx, "nobody-is-bound-here", true, null, Encoding.UTF8.GetBytes("came back"));
            Check("an unroutable mandatory message comes back",
                returned.TryTake(out var r1, 5000) ? r1 : "(nothing)", "312 NO_ROUTE came back");

            rm.BasicPublish("", "no-queue-of-that-name", true, null, Encoding.UTF8.GetBytes("also back"));
            Check("and so does one to a queue that is not there",
                returned.TryTake(out var r2, 5000) ? r2 : "(nothing)", "312 NO_ROUTE also back");

            rm.BasicPublish(rx, "nobody-is-bound-here", false, null, Encoding.UTF8.GetBytes("dropped"));
            Check("without the flag it is dropped in silence",
                returned.TryTake(out var r3, 1200) ? r3 : "(nothing)", "(nothing)");
            Check("and the channel is still up through all of it", rm.IsOpen, true);
        }

        // A `headers` exchange routes by what a message says about itself
        // rather than by a word in a key. It is the one place this broker
        // reads inside a message; everywhere else the properties are bytes it
        // hands back untouched.
        string hx = q + "-headers";
        ch.ExchangeDeclare(hx, "headers", true);
        string hAll = q + "-h-all", hAny = q + "-h-any";
        ch.QueueDeclare(hAll, true, false, false, null);
        ch.QueueDeclare(hAny, true, false, false, null);
        ch.QueueBind(hAll, hx, "", new Dictionary<string, object> {
            { "x-match", "all" }, { "shape", "square" }, { "colour", "red" } });
        ch.QueueBind(hAny, hx, "", new Dictionary<string, object> {
            { "x-match", "any" }, { "shape", "square" }, { "colour", "blue" } });

        void send(params (string, string)[] hs)
        {
            var hp = ch.CreateBasicProperties();
            hp.Headers = new Dictionary<string, object>();
            foreach (var (k, v) in hs) hp.Headers[k] = v;
            ch.BasicPublish(hx, "ignored", hp, Encoding.UTF8.GetBytes("h"));
        }
        send(("shape", "square"), ("colour", "red"));   // all: yes   any: yes
        send(("shape", "square"), ("colour", "green")); // all: no    any: yes
        send(("shape", "round"), ("colour", "blue"));   // all: no    any: yes
        send(("shape", "round"), ("colour", "green"));  // all: no    any: no
        Thread.Sleep(700);
        Check("x-match all wants every header to agree",
            ch.QueueDeclare(hAll, true, false, false, null).MessageCount, 1u);
        Check("and x-match any wants one of them to",
            ch.QueueDeclare(hAny, true, false, false, null).MessageCount, 3u);

        // `if-unused` and `if-empty` were read off the wire and ignored, so a
        // client that asked for a queue to go only if it was empty got it gone
        // either way — the wrong answer to a question it was right to ask.
        string dq2 = q + "-conditional";
        ch.QueueDeclare(dq2, true, false, false, null);
        ch.BasicPublish("", dq2, null, Encoding.UTF8.GetBytes("in the way"));
        Thread.Sleep(400);
        Check("a queue with something in it refuses if-empty",
            refused(m => m.QueueDelete(dq2, false, true)).Split(' ')[0], "406");
        Check("and is still there afterwards",
            ch.QueueDeclare(dq2, true, false, false, null).MessageCount, 1u);
        Check("but goes when nothing is asked of it", ch.QueueDelete(dq2, false, false), 1u);

        // And a consumer reading a queue that is deleted is told, rather than
        // left waiting on something that will never speak again.
        string goneq = q + "-cancelled";
        ch.QueueDeclare(goneq, true, false, false, null);
        using (var cm = conn.CreateModel())
        {
            var told = new BlockingCollection<string>();
            var cc = new EventingBasicConsumer(cm);
            cc.ConsumerCancelled += (_, e) => told.Add("cancelled");
            cm.BasicConsume(goneq, true, cc);
            Thread.Sleep(500);
            cm.QueueDelete(goneq);
            var why = "";
            cm.ModelShutdown += (_, e) => why = e.ReplyCode + " " + e.ReplyText;
            var saw = told.TryTake(out var t2, 4000) ? t2 : "(nothing)";
            Check("a consumer is told its queue has gone",
                saw + (saw == "cancelled" ? "" : " open=" + cm.IsOpen + " " + why), "cancelled");
        }

        // A channel is where a refusal lands. Each of these used to be
        // silence: the client waited for a reply that was never coming, or
        // went on believing something the broker had quietly not done.
        string refused(Action<IModel> ask)
        {
            using var bad = conn.CreateModel();
            var why = new BlockingCollection<string>();
            bad.ModelShutdown += (_, e) => why.Add(e.ReplyCode + " " + e.ReplyText);
            try { ask(bad); } catch (Exception) { }
            return why.TryTake(out var w, 5000) ? w : "(nothing was said)";
        }

        // Transactions, which RillMQ does not do and says so. This check used
        // to use `Queue.Purge`, until `Queue.Purge` was written — a test that
        // asserts something is missing has to be moved as things stop being.
        Check("a method the broker has not written closes the channel",
            refused(m => m.TxSelect()).Split(' ')[0], "540");
        Check("acknowledging a tag it never gave out closes it too",
            refused(m => m.BasicAck(9999, false)), "406 unknown delivery tag");
        Check("and so does a consumer tag already in use",
            refused(m =>
            {
                m.QueueDeclare(q + "-dup", true, false, false, null);
                m.BasicConsume(q + "-dup", true, "twice", new EventingBasicConsumer(m));
                m.BasicConsume(q + "-dup", true, "twice", new EventingBasicConsumer(m));
                Thread.Sleep(800);
            }).Split(' ')[0], "406");

        // A name nobody declared. This is the refusal a mistyped exchange
        // earns, and making the exchange instead was the expensive kind of
        // silence: the messages went somewhere real, with nothing bound to it,
        // and were dropped one at a time by a thing that looked like it worked.
        Check("publishing to an exchange nobody declared closes the channel",
            refused(m => { m.BasicPublish("no-such-exchange", "k", null, Encoding.UTF8.GetBytes("x")); Thread.Sleep(800); }),
            "404 no exchange `no-such-exchange` in vhost `/`");
        Check("and so does binding to one",
            refused(m => { m.QueueDeclare(q + "-b404", true, false, false, null); m.QueueBind(q + "-b404", "also-not-there", "k"); }).Split(' ')[0],
            "404");
        Check("but the default exchange is always there",
            refused(m => { m.BasicPublish("", q, null, Encoding.UTF8.GetBytes("x")); Thread.Sleep(600); }),
            "(nothing was said)");

        // The same for a queue. Consuming from one nobody declared used to make
        // it and then hand over nothing, which looks exactly like a queue with
        // no messages in it — the failure a mistyped queue name deserves to be
        // told about rather than left to look like an empty Tuesday.
        Check("consuming from a queue nobody declared closes the channel",
            refused(m => m.BasicConsume("no-such-queue", true, new EventingBasicConsumer(m))),
            "404 no queue `no-such-queue` in vhost `/`");
        Check("and so does getting from one",
            refused(m => m.BasicGet("nor-this-one", true)).Split(' ')[0], "404");
        Check("and binding one that is not there",
            refused(m => { m.ExchangeDeclare(q + "-x404", "direct", true); m.QueueBind("missing-queue", q + "-x404", "k"); }).Split(' ')[0], "404");

        // But publishing to a queue that is not there is not an error: the
        // default exchange routes by name, a name matching nothing routes
        // nowhere, and AMQP drops it. What it must not do is make the queue.
        Check("publishing to a queue that is not there is not an error",
            refused(m => { m.BasicPublish("", "vanishes-quietly", null, Encoding.UTF8.GetBytes("x")); Thread.Sleep(600); }),
            "(nothing was said)");
        Check("and it did not make the queue either",
            refused(m => m.BasicGet("vanishes-quietly", true)).Split(' ')[0], "404");

        // And the connection is not what closes: everything above happened on
        // a channel of its own, and this one is still up.
        Check("the connection survives all of that", conn.IsOpen, true);
        Check("and so does the channel that did nothing wrong",
            ch.QueueDeclare(q, true, false, false, null).QueueName, q);

        // Message properties, which is what the request-and-reply pattern is
        // made of: a message says where to answer and what to call the answer,
        // and the broker hands both back untouched.
        string pq = q + "-props";
        ch.QueueDeclare(pq, true, false, false, null);
        var sent = ch.CreateBasicProperties();
        sent.ReplyTo = "somewhere-else";
        sent.CorrelationId = "abc-123";
        sent.ContentType = "application/json";
        sent.Persistent = true;
        sent.Headers = new Dictionary<string, object> { { "tenant", Encoding.UTF8.GetBytes("acme") } };
        ch.BasicPublish("", pq, sent, Encoding.UTF8.GetBytes("{}"));
        Thread.Sleep(600);
        var back = ch.BasicGet(pq, autoAck: true);
        Check("reply-to comes back as it went",
            back == null ? "(nothing)" : back.BasicProperties.ReplyTo, "somewhere-else");
        Check("and so does correlation-id",
            back == null ? "(nothing)" : back.BasicProperties.CorrelationId, "abc-123");
        Check("and content-type",
            back == null ? "(nothing)" : back.BasicProperties.ContentType, "application/json");
        Check("and a header a client put there",
            back == null ? "(nothing)" : Encoding.UTF8.GetString((byte[])back.BasicProperties.Headers["tenant"]), "acme");

        // An exchange bound to an exchange: a message published to the source
        // is routed again by the destination, which is how a topology is
        // built out of exchanges rather than out of the client.
        using var xch2 = conn.CreateModel();
        xch2.ExchangeDeclare("up-x", ExchangeType.Topic, true);
        xch2.ExchangeDeclare("down-x", ExchangeType.Fanout, true);
        xch2.ExchangeBind("down-x", "up-x", "orders.#");
        var xq2 = "chained-" + Environment.TickCount;
        xch2.QueueDeclare(xq2, true, false, false, null);
        xch2.QueueBind(xq2, "down-x", "");
        xch2.BasicPublish("up-x", "orders.new", null, Encoding.UTF8.GetBytes("through"));
        xch2.BasicPublish("up-x", "other.new", null, Encoding.UTF8.GetBytes("not this"));
        Thread.Sleep(600);
        var xgot = xch2.BasicGet(xq2, true);
        Check("a message routed by one exchange into another arrives",
            xgot == null ? "(nothing)" : Encoding.UTF8.GetString(xgot.Body.ToArray()), "through");
        Check("and one the first exchange did not match does not",
            xch2.BasicGet(xq2, true) == null ? "(nothing)" : "something", "(nothing)");
        xch2.ExchangeUnbind("down-x", "up-x", "orders.#");
        xch2.BasicPublish("up-x", "orders.new", null, Encoding.UTF8.GetBytes("after"));
        Thread.Sleep(600);
        Check("and after unbinding, nothing comes through",
            xch2.BasicGet(xq2, true) == null ? "(nothing)" : "something", "(nothing)");
        xch2.QueueDelete(xq2);
        xch2.ExchangeDelete("down-x");
        xch2.ExchangeDelete("up-x");

        // `internal`: only other exchanges may publish here. It means
        // something now that an exchange can be bound to an exchange.
        using var ich = conn.CreateModel();
        ich.ExchangeDeclare("front-x", ExchangeType.Fanout, true);
        // `shut-x` is declared over the broker's own protocol by the shell
        // script that runs this, because RabbitMQ.Client 6 has no way to say
        // `internal` — the flag is in the specification and in other client
        // libraries, and not in this one.
        ich.ExchangeBind("shut-x", "front-x", "");
        var iq = "inner-" + Environment.TickCount;
        ich.QueueDeclare(iq, true, false, false, null);
        ich.QueueBind(iq, "shut-x", "");
        ich.BasicPublish("front-x", "", null, Encoding.UTF8.GetBytes("allowed"));
        Thread.Sleep(600);
        var igot = ich.BasicGet(iq, true);
        Check("an internal exchange takes what another exchange routes to it",
            igot == null ? "(nothing)" : Encoding.UTF8.GetString(igot.Body.ToArray()), "allowed");
        var istop = new BlockingCollection<string>();
        using (var ich2 = conn.CreateModel())
        {
            ich2.ModelShutdown += (_, e) => istop.Add(e.ReplyCode.ToString());
            try { ich2.BasicPublish("shut-x", "", null, Encoding.UTF8.GetBytes("refused")); Thread.Sleep(400); } catch (Exception) { }
        }
        Check("and refuses one a client sends it", istop.TryTake(out var ic, 3000) ? ic : "took it", "403");
        using var ich3 = conn.CreateModel();
        ich3.QueueDelete(iq);
        ich3.ExchangeDelete("shut-x");
        ich3.ExchangeDelete("front-x");

        // `multiple` on `Basic.Ack`: one frame settles everything this
        // channel is holding up to the tag it names. Ignoring the bit left
        // four of five in flight for ever.
        using var mch = conn.CreateModel();
        var mq2 = "multi-" + Environment.TickCount;
        mch.QueueDeclare(mq2, true, false, false, null);
        for (var i = 0; i < 5; i++) mch.BasicPublish("", mq2, null, Encoding.UTF8.GetBytes("m"));
        Thread.Sleep(500);
        mch.BasicQos(0, 5, false);
        var mtags = new BlockingCollection<ulong>();
        var mcon2 = new EventingBasicConsumer(mch);
        mcon2.Received += (_, e) => mtags.Add(e.DeliveryTag);
        var mctag = mch.BasicConsume(mq2, false, "", mcon2);
        ulong mlast = 0;
        for (var i = 0; i < 5; i++) if (mtags.TryTake(out var mt, 3000)) mlast = mt;
        mch.BasicAck(mlast, multiple: true);
        Thread.Sleep(600);
        mch.BasicCancel(mctag);
        Thread.Sleep(400);
        using var mch2 = conn.CreateModel();
        Check("one acknowledgement with `multiple` settles them all",
            (int)mch2.QueueDeclare(mq2, true, false, false, null).MessageCount, 0);
        mch2.QueueDelete(mq2);

        // `Basic.Reject` without `requeue` means throw it away, not hand it
        // back. Handing it back is a message the consumer has refused coming
        // round again, and again.
        using var rch = conn.CreateModel();
        var rq = "reject-" + Environment.TickCount;
        rch.QueueDeclare(rq, true, false, false, null);
        rch.BasicPublish("", rq, null, Encoding.UTF8.GetBytes("no thanks"));
        Thread.Sleep(500);
        var rgot = rch.BasicGet(rq, autoAck: false);
        Check("a get that does not acknowledge is answered", rgot == null ? "(nothing)" : "something", "something");
        if (rgot != null) rch.BasicReject(rgot.DeliveryTag, requeue: true);
        Thread.Sleep(600);
        Check("and rejecting it with requeue puts it back",
            (int)rch.QueueDeclare(rq, true, false, false, null).MessageCount, 1);
        var rgot2 = rch.BasicGet(rq, autoAck: false);
        if (rgot2 != null) rch.BasicReject(rgot2.DeliveryTag, requeue: false);
        Thread.Sleep(600);
        Check("and rejecting it without requeue does not",
            (int)rch.QueueDeclare(rq, true, false, false, null).MessageCount, 0);
        rch.QueueDelete(rq);

        // A queue with no name is the broker's to name, and two of them are
        // two queues. They used to both be a queue genuinely called the empty
        // string, so a program that asked for two temporary queues got one.
        using var tmpch = conn.CreateModel();
        var tmpa = tmpch.QueueDeclare("", false, true, true, null).QueueName;
        var tmpb = tmpch.QueueDeclare("", false, true, true, null).QueueName;
        Check("a queue with no name is given one", tmpa.StartsWith("amq.gen-") ? "named" : tmpa, "named");
        Check("and two of them are two queues", tmpa == tmpb ? "the same" : "different", "different");
        tmpch.BasicPublish("", tmpa, null, Encoding.UTF8.GetBytes("one"));
        Thread.Sleep(400);
        Check("what goes into one does not come out of the other",
            tmpch.BasicGet(tmpb, true) == null ? "(nothing)" : "something", "(nothing)");

        // `auto-delete`: the queue lasts as long as its consumers do.
        var acon = new EventingBasicConsumer(tmpch);
        var acontag = tmpch.BasicConsume(tmpa, true, "", acon);
        Thread.Sleep(300);
        tmpch.BasicCancel(acontag);
        Thread.Sleep(400);
        var afterCancel = new BlockingCollection<string>();
        using (var probe = conn.CreateModel())
        {
            probe.ModelShutdown += (_, e) => afterCancel.Add(e.ReplyCode.ToString());
            try { probe.BasicConsume(tmpa, true, "", new EventingBasicConsumer(probe)); } catch (Exception) { }
        }
        Check("a queue that lasts as long as its consumers goes when they do",
            afterCancel.TryTake(out var ac1, 3000) ? ac1 : "still there", "404");

        // `exclusive`: the queue belongs to the connection that declared it,
        // and another connection is refused it rather than given it.
        using var oconn = factory.CreateConnection();
        using var och2 = oconn.CreateModel();
        var locked = new BlockingCollection<string>();
        och2.ModelShutdown += (_, e) => locked.Add(e.ReplyCode.ToString());
        try { och2.BasicConsume(tmpb, true, "", new EventingBasicConsumer(och2)); } catch (Exception) { }
        Check("a queue one connection has to itself is refused to another",
            locked.TryTake(out var l1, 3000) ? l1 : "allowed", "405");

        // `passive` asks whether a queue is there. Answering it by making the
        // queue answers yes to every question ever asked.
        var pgone = new BlockingCollection<string>();
        using (var pch = conn.CreateModel())
        {
            pch.ModelShutdown += (_, e) => pgone.Add(e.ReplyCode.ToString());
            try { pch.QueueDeclarePassive("no-such-queue-at-all"); } catch (Exception) { }
        }
        Check("asking passively after a queue that is not there is a 404",
            pgone.TryTake(out var pg, 3000) ? pg : "made one", "404");
        using var pch2 = conn.CreateModel();
        pch2.QueueDeclare("here-i-am", true, false, false, null);
        Check("and after one that is, its count",
            (int)pch2.QueueDeclarePassive("here-i-am").MessageCount, 0);

        // An exchange that lasts as long as something is bound to it.
        using var xch = conn.CreateModel();
        xch.ExchangeDeclare("going-x", ExchangeType.Direct, durable: false, autoDelete: true, arguments: null);
        var xq = xch.QueueDeclare("", false, true, true, null).QueueName;
        xch.QueueBind(xq, "going-x", "k");
        xch.QueueUnbind(xq, "going-x", "k");
        Thread.Sleep(400);
        var xgone = new BlockingCollection<string>();
        using (var xprobe = conn.CreateModel())
        {
            xprobe.ModelShutdown += (_, e) => xgone.Add(e.ReplyCode.ToString());
            try { xprobe.ExchangeDeclarePassive("going-x"); } catch (Exception) { }
        }
        Check("an exchange that lasts as long as its bindings goes when the last one does",
            xgone.TryTake(out var xg, 3000) ? xg : "still there", "404");

        // A worker pool: several consumers on one channel, all on one queue.
        // Their names to the queue used to be the channel's, so the second
        // consumer took the first one's place and everything went out under
        // one tag.
        using var wch = conn.CreateModel();
        var wq = wch.QueueDeclare("", false, true, true, null).QueueName;
        wch.BasicQos(0, 1, false);
        var wtags = new BlockingCollection<string>();
        for (var i = 0; i < 2; i++)
        {
            var wcon = new EventingBasicConsumer(wch);
            wcon.Received += (_, e) => { wtags.Add(e.ConsumerTag); wch.BasicAck(e.DeliveryTag, false); };
            wch.BasicConsume(wq, false, "w" + i, wcon);
        }
        for (var i = 0; i < 6; i++) wch.BasicPublish("", wq, null, Encoding.UTF8.GetBytes("m"));
        var wtally = new SortedDictionary<string, int>();
        for (var i = 0; i < 6; i++)
            if (wtags.TryTake(out var wt, 3000)) wtally[wt] = wtally.TryGetValue(wt, out var wn) ? wn + 1 : 1;
        Check("two consumers on one channel share the work",
            string.Join(" ", wtally.Select(kv => kv.Key + "=" + kv.Value)), "w0=3 w1=3");

        // `Basic.Qos` and its `global` bit, measured by what is outstanding
        // when nothing is acknowledged: a count each, or a count between them.
        Check("a prefetch of two, not shared, is two each", Outstanding(conn, false), 4);
        Check("and shared, it is two between them", Outstanding(conn, true), 2);

        Console.WriteLine();
        Console.WriteLine($"{passed} of {passed + failed} passed");
        return failed == 0 ? 0 : 1;
    }
}
