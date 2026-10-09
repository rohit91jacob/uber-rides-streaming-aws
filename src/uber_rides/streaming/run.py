"""``uber-rides stream``: replay a window of real trips through Kinesis -> Lambda -> DynamoDB.

Steps: bootstrap the emulated resources -> produce (optionally concurrently with consuming) ->
drain every shard through the Lambda handler -> export the landing objects, the DynamoDB
counters and the DLQ into the lake -> reconcile the counters against the batch trips.
"""

from __future__ import annotations

import gzip
import json
import threading
import time
import uuid
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
from pyiceberg.expressions import In

from uber_rides.config import Settings
from uber_rides.lake.catalog import get_catalog
from uber_rides.lake.tables import STREAM_DLQ, ZONE_WINDOW_COUNTERS, ensure_table, to_arrow
from uber_rides.logs import get_logger
from uber_rides.streaming import producer
from uber_rides.streaming.aws import aws_context, bootstrap, client, queue_url
from uber_rides.streaming.consumer import Consumer, ConsumerConfig
from uber_rides.streaming.events import EVENT_TYPES, TIMESTAMP_COLUMN, window_start
from uber_rides.streaming.poller import ShardPoller

log = get_logger(__name__)


def consumer_config(settings: Settings, dlq_url: str) -> ConsumerConfig:
    return ConsumerConfig(
        dedupe_table=settings.dedupe_table,
        counters_table=settings.counters_table,
        dlq_url=dlq_url,
        landing_bucket=settings.landing_bucket,
        landing_prefix=settings.landing_prefix,
        window_minutes=settings.counter_window_minutes,
        dedupe_ttl_days=settings.dedupe_ttl_days,
        endpoint_url=settings.aws_endpoint_url if settings.aws_mode == "endpoint" else None,
        region=settings.aws_region,
    )


# --------------------------------------------------------------------------- exports
def mirror_landing(settings: Settings, run_id: str) -> int:
    """Copy this run's landing objects from (emulated) S3 to the local landing mirror."""
    s3 = client("s3", settings)
    target = settings.events_landing_dir / f"run={run_id}"
    n = 0
    for page in s3.get_paginator("list_objects_v2").paginate(
        Bucket=settings.landing_bucket, Prefix=f"{settings.landing_prefix}/"
    ):
        for obj in page.get("Contents", []):
            dest = target / obj["Key"]
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_bytes(s3.get_object(Bucket=settings.landing_bucket, Key=obj["Key"])["Body"].read())
            n += 1
    return n


def scan_counters(settings: Settings) -> list[dict[str, Any]]:
    ddb = client("dynamodb", settings)
    items = []
    for page in ddb.get_paginator("scan").paginate(TableName=settings.counters_table):
        for it in page["Items"]:
            counts = {t: int(it.get(t, {"N": "0"})["N"]) for t in EVENT_TYPES}
            items.append(
                {
                    "zone_id": int(it["pk"]["S"].removeprefix("zone#")),
                    "window_start": datetime.fromisoformat(it["sk"]["S"]),
                    "window_minutes": settings.counter_window_minutes,
                    **counts,
                    "surge_proxy": round(counts["requested"] / max(counts["dropped_off"], 1), 4),
                }
            )
    return sorted(items, key=lambda r: (r["window_start"], r["zone_id"]))


def export_counters(settings: Settings, items: list[dict[str, Any]]) -> int:
    snapshot_at = datetime.now(UTC).replace(tzinfo=None, microsecond=0)
    table = ensure_table(get_catalog(settings), settings, ZONE_WINDOW_COUNTERS)
    data = to_arrow(
        ZONE_WINDOW_COUNTERS, pa.Table.from_pylist([{**r, "snapshot_at": snapshot_at} for r in items])
    )
    table.overwrite(data)
    return data.num_rows


def drain_dlq(settings: Settings) -> list[dict[str, Any]]:
    sqs = client("sqs", settings)
    url = queue_url(settings)
    out = []
    while True:
        resp = sqs.receive_message(QueueUrl=url, MaxNumberOfMessages=10, WaitTimeSeconds=0)
        msgs = resp.get("Messages", [])
        if not msgs:
            break
        for m in msgs:
            body = json.loads(m["Body"])
            out.append(
                {
                    "message_id": m["MessageId"],
                    "reason": body.get("reason"),
                    "error": body.get("error"),
                    "shard_id": body.get("shard_id"),
                    "sequence_number": body.get("sequence_number"),
                    "raw": body.get("raw"),
                    "received_at": datetime.fromisoformat(body["received_at"]).replace(tzinfo=None),
                }
            )
        sqs.delete_message_batch(
            QueueUrl=url,
            Entries=[{"Id": str(i), "ReceiptHandle": m["ReceiptHandle"]} for i, m in enumerate(msgs)],
        )
    return out


def export_dlq(settings: Settings, rows: list[dict[str, Any]]) -> int:
    if not rows:
        ensure_table(get_catalog(settings), settings, STREAM_DLQ)
        return 0
    table = ensure_table(get_catalog(settings), settings, STREAM_DLQ)
    data = to_arrow(STREAM_DLQ, pa.Table.from_pylist(rows))
    table.overwrite(data, overwrite_filter=In("message_id", [r["message_id"] for r in rows]))
    return data.num_rows


# --------------------------------------------------------------------------- reconciliation
def expected_counters(trips: pa.Table, window_minutes: int) -> Counter:
    expected: Counter = Counter()
    for trip in trips.to_pylist():
        for etype in EVENT_TYPES:
            ts = trip.get(TIMESTAMP_COLUMN[etype])
            if ts is None:
                continue
            zone = trip["do_location_id"] if etype == "dropped_off" else trip["pu_location_id"]
            expected[(zone, window_start(ts.isoformat(timespec="seconds"), window_minutes), etype)] += 1
    return expected


def reconcile(trips: pa.Table, counters: list[dict[str, Any]], window_minutes: int) -> dict[str, Any]:
    expected = expected_counters(trips, window_minutes)
    actual: Counter = Counter()
    for r in counters:
        for etype in EVENT_TYPES:
            if r[etype]:
                actual[(r["zone_id"], r["window_start"].isoformat(timespec="seconds"), etype)] += r[etype]
    keys = set(expected) | set(actual)
    mismatched = [k for k in keys if expected[k] != actual[k]]
    return {
        "expected_events": sum(expected.values()),
        "counted_events": sum(actual.values()),
        "cells_compared": len(keys),
        "cells_mismatched": len(mismatched),
        "examples": [
            {"zone": k[0], "window": k[1], "type": k[2], "expected": expected[k], "actual": actual[k]}
            for k in sorted(mismatched)[:5]
        ],
        "ok": not mismatched,
    }


# --------------------------------------------------------------------------- entry point
def run_stream(
    settings: Settings,
    *,
    start: datetime,
    end: datetime,
    sample_rate: float = 1.0,
    speedup: float | None = None,
    duplicate_every: int = 0,
    malformed_every: int = 0,
    seed: int = 7,
    concurrent: bool = False,
    replays: int = 1,
) -> dict[str, Any]:
    run_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:6]
    trips = producer.load_window(settings, start, end, sample_rate)
    events = producer.build_events(trips, produced_at=producer.produced_at_now())
    if not events:
        raise ValueError(f"no trips with request_datetime in [{start}, {end}); load the month first")
    t0 = time.monotonic()
    with aws_context(settings):
        resources = bootstrap(settings) if settings.aws_mode != "aws" else {"dlq_url": queue_url(settings)}
        consumer = Consumer(consumer_config(settings, resources["dlq_url"]))
        poller = ShardPoller(settings, consumer)
        replay_stats = []
        for i in range(replays):
            kwargs = {
                "speedup": speedup,
                "duplicate_every": duplicate_every,
                "malformed_every": malformed_every,
                "seed": seed + i,
                "trips": trips.num_rows,
            }
            if concurrent:
                done = threading.Event()
                box: dict[str, Any] = {}

                def _produce(
                    kw: dict[str, Any] = kwargs, box: dict[str, Any] = box, done: threading.Event = done
                ) -> None:
                    try:
                        box["stats"] = producer.replay(settings, events, **kw)
                    except Exception as exc:  # surfaced after the drain
                        box["error"] = exc
                    finally:
                        done.set()

                thread = threading.Thread(target=_produce, name="producer", daemon=True)
                thread.start()
                poll = poller.drain(producer_done=done.is_set)
                thread.join()
                if "error" in box:
                    raise box["error"]
                replay_stats.append(box["stats"].to_dict())
            else:
                replay_stats.append(producer.replay(settings, events, **kwargs).to_dict())
                poll = poller.drain()
        mirrored = mirror_landing(settings, run_id)
        counters = scan_counters(settings)
        dlq_rows = drain_dlq(settings)
    exported_counters = export_counters(settings, counters)
    exported_dlq = export_dlq(settings, dlq_rows)
    recon = reconcile(trips, counters, settings.counter_window_minutes)
    summary = {
        "run_id": run_id,
        "aws_mode": settings.aws_mode,
        "window": {"start": start.isoformat(), "end": end.isoformat(), "sample_rate": sample_rate},
        "replays": replay_stats,
        "consumer": poll.to_dict(),
        "landing_objects_mirrored": mirrored,
        "counter_items": exported_counters,
        "dlq_messages": exported_dlq,
        "dlq_by_reason": dict(Counter(r["reason"] for r in dlq_rows)),
        "reconciliation": recon,
        "elapsed_s": round(time.monotonic() - t0, 2),
        "simulated": {
            "delivery": "replayed at speedup; events carry real TLC timestamps",
            "duplicates_and_malformed": "injected by the producer drills (seeded)",
        },
    }
    summary["events_per_second"] = (
        round(poll.records / summary["elapsed_s"], 1) if summary["elapsed_s"] else None
    )
    out = settings.ops_dir / "stream_runs" / f"{run_id}.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(summary, indent=2, default=str), "utf-8")
    (settings.ops_dir / "stream_runs" / "latest.json").write_text(
        json.dumps(summary, indent=2, default=str), "utf-8"
    )
    log.info("stream run finished", extra={"run_id": run_id, "ok": recon["ok"]})
    return summary


def read_landing_object(path: Path) -> list[dict[str, Any]]:
    with gzip.open(path, "rt", encoding="utf-8") as fh:
        return [json.loads(line) for line in fh]
