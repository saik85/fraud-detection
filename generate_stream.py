"""Generate a stream of card/payment transactions as many small JSON files,
so Spark Structured Streaming can pick them up micro-batch by micro-batch
(simulating a real Kafka/Kinesis feed without extra infrastructure)."""
import json
import os
import random
import time
from datetime import datetime

STREAM_DIR = os.path.join("data", "stream")
CHANNELS = ["ACH", "WIRE", "CARD", "DIGITAL"]
COUNTRIES = ["US", "US", "US", "US", "CA", "GB", "NG", "RU"]  # weighted to US


def make_event(i: int) -> dict:
    amount = round(random.expovariate(1 / 120.0), 2)          # most small, some large
    country = random.choice(COUNTRIES)
    # seed a few obvious fraud patterns
    r = random.random()
    if r < 0.03:
        amount = round(random.uniform(4000, 12000), 2)        # very large
    return {
        "txn_id": f"TX{i:09d}",
        "event_time": datetime.utcnow().isoformat(),
        "account_id": f"A{random.randint(1, 3000):05d}",
        "channel": random.choice(CHANNELS),
        "amount": amount,
        "country": country,
        "device_new": random.random() < 0.15,                 # new/unknown device
    }


def main(batches: int = 6, per_batch: int = 400, pause: float = 0.4):
    os.makedirs(STREAM_DIR, exist_ok=True)
    n = 0
    for b in range(batches):
        events = [make_event(n + k) for k in range(per_batch)]
        n += per_batch
        path = os.path.join(STREAM_DIR, f"batch_{b:03d}.json")
        with open(path, "w") as f:
            for e in events:
                f.write(json.dumps(e) + "\n")
        print(f"  emitted {path}  ({per_batch} events)")
        time.sleep(pause)
    print(f"Emitted {n} events across {batches} files -> {STREAM_DIR}")


if __name__ == "__main__":
    main()
