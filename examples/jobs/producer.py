"""Puts jobs on the `jobs` queue and returns once every one is on the disk.

    python3 producer.py [count]

Written for RabbitMQ with `pika`, and run against RillMQ unchanged.
"""
import os
import sys

import pika

count = int(sys.argv[1]) if len(sys.argv) > 1 else 10
params = pika.ConnectionParameters(
    host=os.environ.get("AMQP_HOST", "127.0.0.1"),
    port=int(os.environ.get("AMQP_PORT", "5672")),
)

conn = pika.BlockingConnection(params)
ch = conn.channel()
# Durable, so the queue is still there after the broker restarts.
ch.queue_declare(queue="jobs", durable=True)
# Each publish now waits for the broker to say the message reached the disk.
ch.confirm_delivery()

jobs = [f"job {i}" for i in range(1, count + 1)]
# One job kills the worker that takes it before it is acknowledged.
jobs.insert(len(jobs) // 2, "crash")
for job in jobs:
    ch.basic_publish(
        exchange="",
        routing_key="jobs",
        body=job.encode(),
        properties=pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent),
    )
print(f"producer: {len(jobs)} jobs on the disk", flush=True)
conn.close()
