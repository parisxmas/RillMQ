"""Takes jobs from the `jobs` queue one at a time and acknowledges each.

    python3 worker.py [name]

A job is acknowledged only once it is done. A worker that dies holding one has
not acknowledged it, so the broker gives it to another worker, marked as
delivered before.
"""
import os
import sys
import time

import pika

name = sys.argv[1] if len(sys.argv) > 1 else f"worker-{os.getpid()}"
params = pika.ConnectionParameters(
    host=os.environ.get("AMQP_HOST", "127.0.0.1"),
    port=int(os.environ.get("AMQP_PORT", "5672")),
)

conn = pika.BlockingConnection(params)
ch = conn.channel()
ch.queue_declare(queue="jobs", durable=True)
# One job at a time: a slow worker is not handed a pile it cannot get through.
ch.basic_qos(prefetch_count=1)


def handle(ch, method, properties, body):
    job = body.decode()
    again = " (delivered before)" if method.redelivered else ""
    print(f"{name}: {job}{again}", flush=True)
    if job == "crash" and not method.redelivered:
        print(f"{name}: dies without acknowledging it", flush=True)
        os._exit(1)
    time.sleep(0.1)  # the work
    ch.basic_ack(delivery_tag=method.delivery_tag)


ch.basic_consume(queue="jobs", on_message_callback=handle)
try:
    ch.start_consuming()
except KeyboardInterrupt:
    pass
