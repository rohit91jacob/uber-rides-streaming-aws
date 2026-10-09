"""Kinesis -> Lambda consumer: validate, land, dedupe, count.

Deployed as the Lambda handler ``uber_rides.streaming.consumer.handler`` (event source mapping
with ``ReportBatchItemFailures``). Locally the same function is driven by
:mod:`uber_rides.streaming.poller` against moto or LocalStack.

Per batch:

1. **Validate** every record. Invalid ones go to the SQS dead-letter queue with a reason.
2. **Land** all valid records as one gzip JSON-lines object in S3. The key is derived from the
   shard and the first/last sequence numbers, so a retried batch overwrites the same object.
   The landing zone is at-least-once; the Iceberg loader deduplicates on ``event_id``.
3. **Dedupe + count, exactly once.** For each event a DynamoDB transaction does
   ``Put(dedupe item, attribute_not_exists(event_id))`` and ``ADD`` on the zone/window counter
   item. A redelivered event fails the condition, so the whole transaction is cancelled and the
   counter is not incremented twice. Events are grouped by (zone, window) and groups are packed
   into transactions of at most 100 items (one Put per event plus one Update per counter item,
   pre-aggregated because a transaction may touch each item only once).
4. On a transient failure the handler returns the first unprocessed sequence number in
   ``batchItemFailures`` so the batch is retried from there (Kinesis ordering is preserved).

Stdlib + boto3 only: boto3 is provided by the Lambda Python runtime.
"""

from __future__ import annotations

import base64
import gzip
import json
import os
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from uber_rides.streaming.events import EVENT_TYPES, Invalid, truncate_raw, validate, window_start

MAX_TRANSACTION_ITEMS = 100  # DynamoDB TransactWriteItems limit
COUNTER_TTL_DAYS = 35
RETRYABLE = {
    "ProvisionedThroughputExceededException",
    "ThrottlingException",
    "RequestLimitExceeded",
    "InternalServerError",
    "TransactionConflictException",
    "TransactionInProgressException",
}


@dataclass(frozen=True)
class ConsumerConfig:
    dedupe_table: str
    counters_table: str
    dlq_url: str
    landing_bucket: str
    landing_prefix: str = "events"
    window_minutes: int = 15
    dedupe_ttl_days: int = 7
    endpoint_url: str | None = None
    region: str = "us-east-1"
    max_transaction_attempts: int = 5

    @classmethod
    def from_env(cls) -> ConsumerConfig:
        env = os.environ
        return cls(
            dedupe_table=env["UBER_DEDUPE_TABLE"],
            counters_table=env["UBER_COUNTERS_TABLE"],
            dlq_url=env["UBER_DLQ_URL"],
            landing_bucket=env["UBER_LANDING_BUCKET"],
            landing_prefix=env.get("UBER_LANDING_PREFIX", "events").strip("/"),
            window_minutes=int(env.get("UBER_COUNTER_WINDOW_MINUTES", "15")),
            dedupe_ttl_days=int(env.get("UBER_DEDUPE_TTL_DAYS", "7")),
            endpoint_url=env.get("UBER_AWS_ENDPOINT_URL") or None,
            region=env.get("AWS_REGION", "us-east-1"),
        )


@dataclass
class BatchResult:
    received: int = 0
    accepted: int = 0
    duplicates: int = 0
    invalid: int = 0
    landed_object: str | None = None
    failed_from: str | None = None
    error: str | None = None
    invalid_by_reason: dict[str, int] = field(default_factory=lambda: defaultdict(int))

    def response(self) -> dict[str, Any]:
        failures = [{"itemIdentifier": self.failed_from}] if self.failed_from else []
        return {"batchItemFailures": failures}


class Consumer:
    def __init__(self, config: ConsumerConfig, *, clients: dict[str, Any] | None = None) -> None:
        self.config = config
        if clients is None:
            kw: dict[str, Any] = {
                "region_name": config.region,
                "config": Config(retries={"mode": "standard", "max_attempts": 5}),
            }
            if config.endpoint_url:
                kw["endpoint_url"] = config.endpoint_url
            clients = {s: boto3.client(s, **kw) for s in ("dynamodb", "sqs", "s3")}
        self.ddb = clients["dynamodb"]
        self.sqs = clients["sqs"]
        self.s3 = clients["s3"]

    # ------------------------------------------------------------------ entry point
    def handle(self, event: dict[str, Any]) -> tuple[dict[str, Any], BatchResult]:
        records = event.get("Records", [])
        result = BatchResult(received=len(records))
        if not records:
            return result.response(), result
        valid: list[tuple[dict[str, Any], dict[str, Any]]] = []
        dead: list[dict[str, Any]] = []
        for rec in records:
            kin = rec["kinesis"]
            raw = base64.b64decode(kin["data"])
            parsed = validate(raw)
            meta = {
                "shard_id": rec.get("eventID", ":").split(":", 1)[0],
                "sequence_number": kin["sequenceNumber"],
            }
            if isinstance(parsed, Invalid):
                result.invalid += 1
                result.invalid_by_reason[parsed.reason] += 1
                dead.append(
                    {**meta, "reason": parsed.reason, "error": parsed.error, "raw": truncate_raw(raw)}
                )
            else:
                valid.append((parsed, meta))
        if dead:
            self._send_dlq(dead)
        if valid:
            result.landed_object = self._land(valid)
            self._dedupe_and_count(valid, result)
        return result.response(), result

    # ------------------------------------------------------------------ steps
    def _send_dlq(self, dead: list[dict[str, Any]]) -> None:
        now = datetime.now(UTC).isoformat(timespec="seconds")
        for i in range(0, len(dead), 10):
            chunk = dead[i : i + 10]
            entries = [
                {"Id": str(j), "MessageBody": json.dumps({**d, "received_at": now})}
                for j, d in enumerate(chunk)
            ]
            resp = self.sqs.send_message_batch(QueueUrl=self.config.dlq_url, Entries=entries)
            if resp.get("Failed"):
                raise RuntimeError(f"DLQ send failed: {resp['Failed']}")

    def _land(self, valid: list[tuple[dict[str, Any], dict[str, Any]]]) -> str:
        first, last = valid[0][1], valid[-1][1]
        key = (
            f"{self.config.landing_prefix}/shard={first['shard_id']}/"
            f"{first['sequence_number']}-{last['sequence_number']}.jsonl.gz"
        )
        consumed_at = datetime.now(UTC).isoformat(timespec="seconds")
        lines = "".join(
            json.dumps({**ev, "_kinesis": meta, "_consumed_at": consumed_at}, separators=(",", ":")) + "\n"
            for ev, meta in valid
        )
        self.s3.put_object(
            Bucket=self.config.landing_bucket,
            Key=key,
            Body=gzip.compress(lines.encode(), mtime=0),
            ContentType="application/x-ndjson",
            ContentEncoding="gzip",
        )
        return key

    def _dedupe_and_count(
        self, valid: list[tuple[dict[str, Any], dict[str, Any]]], result: BatchResult
    ) -> None:
        seen: set[str] = set()
        groups: dict[tuple[int, str], list[tuple[dict[str, Any], dict[str, Any]]]] = {}
        for ev, meta in valid:
            if ev["event_id"] in seen:  # duplicate inside the same batch
                result.duplicates += 1
                continue
            seen.add(ev["event_id"])
            key = (ev["zone_id"], window_start(ev["event_time"], self.config.window_minutes))
            groups.setdefault(key, []).append((ev, meta))
        chunks = pack_transactions(list(groups.values()))
        for i, chunk in enumerate(chunks):
            try:
                accepted, dupes = self._commit_chunk(chunk)
            except ClientError as exc:
                result.error = f"{exc.response['Error']['Code']}: {exc.response['Error'].get('Message', '')}"[
                    :500
                ]
                # Retry from the earliest event not yet committed; committed ones after it are
                # rejected by the dedupe condition on redelivery, so nothing is counted twice.
                remaining = [m["sequence_number"] for c in chunks[i:] for _, m in c]
                result.failed_from = min(remaining, key=int)
                return
            result.accepted += accepted
            result.duplicates += dupes

    def _commit_chunk(self, chunk: list[tuple[dict[str, Any], dict[str, Any]]]) -> tuple[int, int]:
        duplicates = 0
        attempt = 0
        while chunk:
            items = self._transaction_items(chunk)
            try:
                self.ddb.transact_write_items(TransactItems=items)
                return len(chunk), duplicates
            except ClientError as exc:
                code = exc.response["Error"]["Code"]
                if code == "TransactionCanceledException":
                    reasons = exc.response.get("CancellationReasons") or []
                    dup_idx = {
                        j
                        for j, r in enumerate(reasons[: len(chunk)])
                        if r.get("Code") == "ConditionalCheckFailed"
                    }
                    if dup_idx:
                        duplicates += len(dup_idx)
                        chunk = [c for j, c in enumerate(chunk) if j not in dup_idx]
                        continue
                    if not any(r.get("Code") not in (None, "None") for r in reasons):
                        raise  # cancelled without a reason we understand
                    code = next(r["Code"] for r in reasons if r.get("Code") not in (None, "None"))
                attempt += 1
                if code not in RETRYABLE or attempt >= self.config.max_transaction_attempts:
                    raise
                time.sleep(min(2.0, 0.05 * 2**attempt))
        return 0, duplicates

    def _transaction_items(self, chunk: list[tuple[dict[str, Any], dict[str, Any]]]) -> list[dict[str, Any]]:
        now = datetime.now(UTC)
        dedupe_ttl = int((now + timedelta(days=self.config.dedupe_ttl_days)).timestamp())
        counter_ttl = int((now + timedelta(days=COUNTER_TTL_DAYS)).timestamp())
        puts = []
        counts: dict[tuple[int, str], dict[str, int]] = defaultdict(lambda: dict.fromkeys(EVENT_TYPES, 0))
        for ev, meta in chunk:
            puts.append(
                {
                    "Put": {
                        "TableName": self.config.dedupe_table,
                        "Item": {
                            "event_id": {"S": ev["event_id"]},
                            "trip_id": {"S": ev["trip_id"]},
                            "event_type": {"S": ev["event_type"]},
                            "shard_id": {"S": meta["shard_id"]},
                            "sequence_number": {"S": meta["sequence_number"]},
                            "first_seen_at": {"S": now.isoformat(timespec="seconds")},
                            "expires_at": {"N": str(dedupe_ttl)},
                        },
                        "ConditionExpression": "attribute_not_exists(event_id)",
                    }
                }
            )
            key = (ev["zone_id"], window_start(ev["event_time"], self.config.window_minutes))
            counts[key][ev["event_type"]] += 1
        updates = []
        for (zone, window), c in counts.items():
            names = {f"#{t}": t for t in EVENT_TYPES if c[t]}
            values = {f":{t}": {"N": str(c[t])} for t in EVENT_TYPES if c[t]}
            adds = ", ".join(f"#{t} :{t}" for t in EVENT_TYPES if c[t])
            updates.append(
                {
                    "Update": {
                        "TableName": self.config.counters_table,
                        # pk = zone#<id>, sk = window start (NYC local, ISO-8601)
                        "Key": {"pk": {"S": f"zone#{zone}"}, "sk": {"S": window}},
                        "UpdateExpression": f"ADD {adds} SET expires_at = :ttl",
                        "ExpressionAttributeNames": names,
                        "ExpressionAttributeValues": {**values, ":ttl": {"N": str(counter_ttl)}},
                    }
                }
            )
        return puts + updates


def pack_transactions(
    groups: list[list[tuple[dict[str, Any], dict[str, Any]]]],
) -> list[list[tuple[dict[str, Any], dict[str, Any]]]]:
    """Pack (zone, window) groups into transactions of <= 100 items (events + 1 per group)."""
    chunks: list[list[tuple[dict[str, Any], dict[str, Any]]]] = []
    current: list[tuple[dict[str, Any], dict[str, Any]]] = []
    items = 0
    for group in groups:
        for start in range(0, len(group), MAX_TRANSACTION_ITEMS - 1):
            part = group[start : start + MAX_TRANSACTION_ITEMS - 1]
            need = len(part) + 1
            if items + need > MAX_TRANSACTION_ITEMS:
                chunks.append(current)
                current, items = [], 0
            current.extend(part)
            items += need
    if current:
        chunks.append(current)
    return chunks


_CONSUMER: Consumer | None = None


def handler(event: dict[str, Any], context: Any = None) -> dict[str, Any]:
    """AWS Lambda entry point (Kinesis event source, ReportBatchItemFailures)."""
    global _CONSUMER
    if _CONSUMER is None:
        _CONSUMER = Consumer(ConsumerConfig.from_env())
    response, result = _CONSUMER.handle(event)
    print(
        json.dumps(
            {
                "msg": "batch processed",
                "received": result.received,
                "accepted": result.accepted,
                "duplicates": result.duplicates,
                "invalid": result.invalid,
                "failed_from": result.failed_from,
                "error": result.error,
                "landed_object": result.landed_object,
            }
        )
    )
    return response
