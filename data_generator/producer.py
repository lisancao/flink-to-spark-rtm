#!/usr/bin/env python3
"""
Kafka producer for flink-to-spark-rtm benchmark pipelines.

Produces JSON events with embedded timestamps to the benchmark-input topic.
Events simulate user interactions (clicks, purchases, views).

Adapted from the spark-rtm-quickstart producer with a higher default rate
(10,000 events/sec) suitable for latency benchmarking.

Usage:
    python producer.py [--rate 10000] [--keys 100] [--topic benchmark-input] \
                       [--bootstrap-servers localhost:9092]

Requires: kafka-python-ng
"""

import argparse
import json
import random
import signal
import sys
import time
import uuid

from kafka import KafkaProducer

# ── event generation ──────────────────────────────────────────────────
EVENT_TYPES = ["click", "purchase", "view"]
EVENT_WEIGHTS = [0.5, 0.1, 0.4]  # clicks most common, purchases rare


def create_event(num_keys: int) -> dict:
    """Generate a single event with an embedded epoch-millisecond timestamp."""
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": random.choices(EVENT_TYPES, weights=EVENT_WEIGHTS, k=1)[0],
        "key": f"user_{random.randint(1, num_keys)}",
        "value": random.randint(1, 1000),
        "produced_at": int(time.time() * 1000),  # epoch milliseconds
    }


def main():
    parser = argparse.ArgumentParser(
        description="Kafka producer for flink-to-spark-rtm benchmarks"
    )
    parser.add_argument(
        "--rate", type=int, default=10000,
        help="Target events per second (default: 10000)",
    )
    parser.add_argument(
        "--keys", type=int, default=100,
        help="Number of unique user keys (default: 100)",
    )
    parser.add_argument(
        "--topic", type=str, default="benchmark-input",
        help="Kafka topic to produce to (default: benchmark-input)",
    )
    parser.add_argument(
        "--bootstrap-servers", type=str, default="localhost:9092",
        help="Kafka bootstrap servers (default: localhost:9092)",
    )
    args = parser.parse_args()

    # ── Kafka producer with batching for throughput ────────────────────
    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_servers,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks=1,
        linger_ms=5,
        batch_size=32768,
    )

    # ── graceful shutdown on SIGINT / SIGTERM ──────────────────────────
    running = True

    def shutdown(signum, frame):
        nonlocal running
        print(f"\nReceived signal {signum}, shutting down...")
        running = False

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    sent = 0
    errors = 0
    start_time = time.time()

    print(f"Producing {args.rate:,} events/sec to topic '{args.topic}'")
    print(f"  keys={args.keys}, bootstrap={args.bootstrap_servers}")
    print("Press Ctrl+C to stop.\n")

    # ── error callback for async sends ────────────────────────────────
    def on_error(exc):
        nonlocal errors
        errors += 1

    try:
        while running:
            batch_start = time.time()

            # Send one second's worth of events
            for _ in range(args.rate):
                if not running:
                    break
                event = create_event(args.keys)
                producer.send(
                    args.topic, key=event["key"], value=event
                ).add_errback(on_error)
                sent += 1

            # Flush to ensure delivery before measuring timing
            producer.flush()

            # ── rate reporting ────────────────────────────────────────
            elapsed = time.time() - start_time
            actual_rate = sent / elapsed if elapsed > 0 else 0
            error_str = f" | errors: {errors}" if errors else ""
            print(
                f"  Sent {sent:>10,} events | "
                f"Elapsed: {elapsed:6.1f}s | "
                f"Rate: {actual_rate:>10,.0f} events/sec{error_str}"
            )

            # Sleep for remainder of the 1-second batch window
            batch_elapsed = time.time() - batch_start
            sleep_time = max(0, 1.0 - batch_elapsed)
            if sleep_time > 0:
                time.sleep(sleep_time)

    finally:
        producer.flush()
        producer.close()
        elapsed = time.time() - start_time
        final_rate = sent / elapsed if elapsed > 0 else 0
        print(
            f"\nDone. Sent {sent:,} events in {elapsed:.1f}s "
            f"({final_rate:,.0f} events/sec)"
        )
        if errors:
            print(f"Delivery errors: {errors}")
        sys.exit(0)


if __name__ == "__main__":
    main()
