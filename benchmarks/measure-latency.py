#!/usr/bin/env python3
"""
End-to-end latency measurement consumer for flink-to-spark-rtm benchmarks.

Reads events from an output topic and computes latency as:
    latency = consumer_wall_clock_ms - produced_at

Reports p50 / p95 / p99 percentiles every 5 seconds.  After --duration
seconds, prints a final summary with:
  - total events consumed
  - median of all p99 buckets
  - list of individual p99 values from each reporting interval

This script uses group_id=None so it does not participate in consumer
group coordination (avoids rebalance delays that would pollute latency).

Usage:
    python measure-latency.py [--topic benchmark-output] \
                              [--bootstrap-servers localhost:9092] \
                              [--duration 60]

Requires: kafka-python-ng
"""

import argparse
import json
import signal
import statistics
import sys
import time

from kafka import KafkaConsumer


# ── percentile helper ─────────────────────────────────────────────────
def percentile(sorted_data: list, pct: float) -> float:
    """Return the value at the given percentile from a pre-sorted list."""
    if not sorted_data:
        return 0.0
    idx = int(len(sorted_data) * pct)
    idx = min(idx, len(sorted_data) - 1)
    return sorted_data[idx]


def main():
    parser = argparse.ArgumentParser(
        description="End-to-end latency consumer for benchmark pipelines"
    )
    parser.add_argument(
        "--topic", type=str, default="benchmark-output",
        help="Kafka topic to consume from (default: benchmark-output)",
    )
    parser.add_argument(
        "--bootstrap-servers", type=str, default="localhost:9092",
        help="Kafka bootstrap servers (default: localhost:9092)",
    )
    parser.add_argument(
        "--duration", type=int, default=60,
        help="Measurement duration in seconds (default: 60)",
    )
    args = parser.parse_args()

    # ── Kafka consumer (no group coordination) ────────────────────────
    consumer = KafkaConsumer(
        args.topic,
        bootstrap_servers=args.bootstrap_servers,
        auto_offset_reset="latest",
        group_id=None,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )

    # ── graceful shutdown ─────────────────────────────────────────────
    running = True

    def shutdown(_signum, _frame):
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    print(f"Measuring latency on topic '{args.topic}' for {args.duration}s")
    print(f"  bootstrap={args.bootstrap_servers}, group_id=None")
    print("  Reporting p50/p95/p99 every 5 seconds\n")

    # ── state ─────────────────────────────────────────────────────────
    bucket_latencies: list[float] = []   # latencies in current 5-second bucket
    all_p99s: list[float] = []           # p99 from each bucket
    total_events = 0
    report_interval = 5.0                # seconds between percentile reports

    run_start = time.time()
    last_report = run_start
    bucket_num = 0

    # ── main loop ─────────────────────────────────────────────────────
    while running:
        now = time.time()
        # Check duration limit
        if now - run_start >= args.duration:
            break

        records = consumer.poll(timeout_ms=500)
        now_ms = int(time.time() * 1000)

        for _tp, messages in records.items():
            for msg in messages:
                produced_at = msg.value.get("produced_at")
                if produced_at is not None:
                    latency_ms = now_ms - int(produced_at)
                    bucket_latencies.append(latency_ms)
                    total_events += 1

        # ── periodic report ───────────────────────────────────────────
        now = time.time()
        if now - last_report >= report_interval:
            bucket_num += 1
            if bucket_latencies:
                bucket_latencies.sort()
                n = len(bucket_latencies)
                p50 = percentile(bucket_latencies, 0.50)
                p95 = percentile(bucket_latencies, 0.95)
                p99 = percentile(bucket_latencies, 0.99)
                mean_lat = statistics.mean(bucket_latencies)
                all_p99s.append(p99)
                print(
                    f"  [{bucket_num:3d}] n={n:>7,} | "
                    f"p50={p50:>6.0f}ms | p95={p95:>6.0f}ms | "
                    f"p99={p99:>6.0f}ms | mean={mean_lat:>6.1f}ms | "
                    f"min={bucket_latencies[0]:.0f}ms | "
                    f"max={bucket_latencies[-1]:.0f}ms"
                )
                bucket_latencies.clear()
            else:
                print(f"  [{bucket_num:3d}] No data in this interval")
            last_report = now

    # ── flush any remaining data in the last partial bucket ───────────
    if bucket_latencies:
        bucket_latencies.sort()
        p99 = percentile(bucket_latencies, 0.99)
        all_p99s.append(p99)

    consumer.close()

    # ── final summary ─────────────────────────────────────────────────
    elapsed = time.time() - run_start
    print("\n" + "=" * 65)
    print("LATENCY SUMMARY")
    print("=" * 65)
    print(f"  Duration ......... {elapsed:.1f}s")
    print(f"  Events consumed .. {total_events:,}")

    if all_p99s:
        median_p99 = statistics.median(all_p99s)
        print(f"  Median of p99s ... {median_p99:.0f}ms")
        print(f"  Individual p99s .. {[f'{v:.0f}' for v in all_p99s]}")
    else:
        print("  No latency data collected.")

    print("=" * 65)
    sys.exit(0)


if __name__ == "__main__":
    main()
