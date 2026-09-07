// The acceptance test: RabbitMQ's own .NET client, pointed at RillMQ.
//
//     dotnet run --project test/dotnet -- <port>
//
// Nothing here knows it is not talking to RabbitMQ, which is the point.
using System;
using System.Collections.Concurrent;
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

    static int Main(string[] args)
    {
        int port = args.Length > 0 ? int.Parse(args[0]) : 5672;
        var factory = new ConnectionFactory
        {
            HostName = "127.0.0.1",
            Port = port,
            UserName = "guest",
            Password = "guest",
            VirtualHost = "/",
            RequestedHeartbeat = TimeSpan.Zero,
        };

        using var conn = factory.CreateConnection();

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
            var seen2 = new BlockingCollection<int>();
            bch.BasicQos(0, 64, false);
            var c2 = new EventingBasicConsumer(bch);
            c2.Received += (_, ea) => { seen2.Add(1); bch.BasicAck(ea.DeliveryTag, false); };
            bch.BasicConsume(bq, false, c2);
            int n2 = 0;
            var end = DateTime.UtcNow.AddSeconds(120);
            while (n2 < count && DateTime.UtcNow < end) if (seen2.TryTake(out _, 1000)) n2++;
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

        Console.WriteLine();
        Console.WriteLine($"{passed} of {passed + failed} passed");
        return failed == 0 ? 0 : 1;
    }
}
