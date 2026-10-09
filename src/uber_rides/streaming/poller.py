"""A local stand-in for the Lambda Kinesis event source mapping.

On AWS, Lambda polls each shard, invokes the function with up to ``BatchSize`` records, and
honours ``batchItemFailures`` (retrying from the reported sequence number). After
``MaximumRetryAttempts`` it sends the failed batch's metadata to the on-failure destination.
This module reproduces that contract against moto or LocalStack, calling the very same
:class:`~uber_rides.streaming.consumer.Consumer`.
"""

from __future__ import annotations

import base64
import json
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from uber_rides.config import Settings
from uber_rides.logs import get_logger
from uber_rides.streaming.aws import client, queue_url, stream_arn
from uber_rides.streaming.consumer import BatchResult, Consumer

log = get_logger(__name__)


@dataclass
class PollStats:
    invocations: int = 0
    records: int = 0
    accepted: int = 0
    duplicates: int = 0
    invalid: int = 0
    retried_batches: int = 0
    failed_batches_to_dlq: int = 0
    landing_objects: set[str] = field(default_factory=set)
    invalid_by_reason: dict[str, int] = field(default_factory=dict)

    def add(self, r: BatchResult) -> None:
        self.invocations += 1
        self.records += r.received
        self.accepted += r.accepted
        self.duplicates += r.duplicates
        self.invalid += r.invalid
        if r.landed_object:
            self.landing_objects.add(r.landed_object)
        for k, v in r.invalid_by_reason.items():
            self.invalid_by_reason[k] = self.invalid_by_reason.get(k, 0) + v

    def to_dict(self) -> dict[str, Any]:
        d = dict(self.__dict__)
        d["landing_objects"] = len(self.landing_objects)
        return d


def _lambda_event(records: list[dict[str, Any]], shard_id: str, arn: str) -> dict[str, Any]:
    return {
        "Records": [
            {
                "kinesis": {
                    "kinesisSchemaVersion": "1.0",
                    "partitionKey": r["PartitionKey"],
                    "sequenceNumber": r["SequenceNumber"],
                    "data": base64.b64encode(r["Data"]).decode(),
                    "approximateArrivalTimestamp": r["ApproximateArrivalTimestamp"].timestamp(),
                },
                "eventSource": "aws:kinesis",
                "eventVersion": "1.0",
                "eventID": f"{shard_id}:{r['SequenceNumber']}",
                "eventName": "aws:kinesis:record",
                "eventSourceARN": arn,
                "awsRegion": arn.split(":")[3],
            }
            for r in records
        ]
    }


class ShardPoller:
    """Drains every shard from TRIM_HORIZON (or the last checkpoint) through the consumer."""

    def __init__(self, settings: Settings, consumer: Consumer, *, batch_size: int | None = None) -> None:
        self.settings = settings
        self.consumer = consumer
        self.kinesis = client("kinesis", settings)
        self.sqs = client("sqs", settings)
        self.batch_size = batch_size or settings.consumer_batch_size
        self.arn = stream_arn(settings)
        self.dlq_url = queue_url(settings)
        self.checkpoints: dict[str, str] = {}
        self.stats = PollStats()

    def _iterator(self, shard_id: str) -> str:
        if shard_id in self.checkpoints:
            return self.kinesis.get_shard_iterator(
                StreamName=self.settings.kinesis_stream,
                ShardId=shard_id,
                ShardIteratorType="AFTER_SEQUENCE_NUMBER",
                StartingSequenceNumber=self.checkpoints[shard_id],
            )["ShardIterator"]
        return self.kinesis.get_shard_iterator(
            StreamName=self.settings.kinesis_stream, ShardId=shard_id, ShardIteratorType="TRIM_HORIZON"
        )["ShardIterator"]

    def _on_failure(self, shard_id: str, records: list[dict[str, Any]], error: str) -> None:
        body = {
            "reason": "batch_retries_exhausted",
            "error": error[:500],
            "shard_id": shard_id,
            "sequence_number": records[0]["SequenceNumber"],
            "last_sequence_number": records[-1]["SequenceNumber"],
            "raw": json.dumps({"records": len(records)}),
            "received_at": datetime.now(UTC).isoformat(timespec="seconds"),
        }
        self.sqs.send_message(QueueUrl=self.dlq_url, MessageBody=json.dumps(body))
        self.stats.failed_batches_to_dlq += 1

    def _process(self, shard_id: str, records: list[dict[str, Any]]) -> None:
        attempt = 0
        while records:
            try:
                _, result = self.consumer.handle(_lambda_event(records, shard_id, self.arn))
            except Exception as exc:  # the whole invocation failed: retry the whole batch
                result = BatchResult(received=len(records), failed_from=records[0]["SequenceNumber"])
                error = repr(exc)
            else:
                error = "batchItemFailures"
            self.stats.add(result)
            if not result.failed_from:
                self.checkpoints[shard_id] = records[-1]["SequenceNumber"]
                return
            attempt += 1
            idx = next(i for i, r in enumerate(records) if r["SequenceNumber"] == result.failed_from)
            if idx:
                self.checkpoints[shard_id] = records[idx - 1]["SequenceNumber"]
            records = records[idx:]
            if attempt > self.settings.consumer_max_retries:
                self._on_failure(shard_id, records, error)
                self.checkpoints[shard_id] = records[-1]["SequenceNumber"]
                return
            self.stats.retried_batches += 1
            time.sleep(min(2.0, 0.1 * 2**attempt))

    def drain(
        self, *, producer_done: Callable[[], bool] = lambda: True, idle_sleep: float = 0.2
    ) -> PollStats:
        """Poll until every shard is empty *and* the producer has finished."""
        shards = [
            s["ShardId"] for s in self.kinesis.list_shards(StreamName=self.settings.kinesis_stream)["Shards"]
        ]
        iterators = {s: self._iterator(s) for s in shards}
        while True:
            got_any = False
            for shard_id in shards:
                resp = self.kinesis.get_records(ShardIterator=iterators[shard_id], Limit=self.batch_size)
                iterators[shard_id] = resp["NextShardIterator"]
                if resp["Records"]:
                    got_any = True
                    self._process(shard_id, resp["Records"])
                    iterators[shard_id] = self._iterator(shard_id)
            if not got_any:
                if producer_done():
                    # one more pass after the producer is done, to catch its final flush
                    final = [
                        self.kinesis.get_records(ShardIterator=self._iterator(s), Limit=self.batch_size)
                        for s in shards
                    ]
                    if not any(r["Records"] for r in final):
                        break
                    continue
                time.sleep(idle_sleep)
        log.info("poller drained", extra=self.stats.to_dict())
        return self.stats
