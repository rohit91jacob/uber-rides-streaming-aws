"""The Lambda consumer against moto: validation, DLQ, exactly-once counters, redelivery."""

from __future__ import annotations

import base64
import json
from datetime import datetime

import pytest
from botocore.exceptions import ClientError

from uber_rides.streaming import consumer as consumer_mod
from uber_rides.streaming import events, run
from uber_rides.streaming.aws import aws_context, bootstrap, client


def _trip(i: int, zone: int = 161) -> dict:
    return {
        "trip_id": f"HV0003-2026-06-{i:09d}",
        "hvfhs_license_num": "HV0003",
        "request_datetime": datetime(2026, 6, 12, 18, i % 15, 0),
        "on_scene_datetime": datetime(2026, 6, 12, 18, i % 15, 30),
        "pickup_datetime": datetime(2026, 6, 12, 18, i % 15, 59),
        "dropoff_datetime": datetime(2026, 6, 12, 18, 40, 0),
        "pu_location_id": zone,
        "do_location_id": 132,
        "trip_miles": 3.0,
        "trip_time_s": 600,
        "base_passenger_fare": 20.0,
        "driver_pay": 15.0,
    }


def _kinesis_event(payloads: list[bytes], shard: str = "shardId-000000000000") -> dict:
    return {
        "Records": [
            {
                "kinesis": {
                    "sequenceNumber": str(1000 + i),
                    "data": base64.b64encode(p).decode(),
                    "partitionKey": "k",
                },
                "eventID": f"{shard}:{1000 + i}",
            }
            for i, p in enumerate(payloads)
        ]
    }


@pytest.fixture
def aws(settings):
    with aws_context(settings):
        resources = bootstrap(settings)
        yield settings, consumer_mod.Consumer(run.consumer_config(settings, resources["dlq_url"]))


def test_batch_is_validated_landed_deduped_and_counted(aws):
    settings, consumer = aws
    evs = [e for i in range(30) for e in events.events_for_trip(_trip(i))]
    payloads = [events.encode(e) for e in evs]
    payloads += [payloads[0], payloads[5]]  # in-batch duplicates
    payloads += [b"not json", events.encode({**evs[0], "zone_id": 500})]
    response, result = consumer.handle(_kinesis_event(payloads))
    assert response == {"batchItemFailures": []}
    assert result.accepted == 120
    assert result.duplicates == 2
    assert result.invalid == 2
    counters = run.scan_counters(settings)
    assert sum(c["requested"] for c in counters) == 30
    assert sum(c["dropped_off"] for c in counters) == 30
    dlq = run.drain_dlq(settings)
    assert sorted(d["reason"] for d in dlq) == ["malformed_json", "zone_out_of_range"]
    objects = client("s3", settings).list_objects_v2(Bucket=settings.landing_bucket)["Contents"]
    assert len(objects) == 1


def test_redelivered_batch_does_not_double_count(aws):
    settings, consumer = aws
    payloads = [events.encode(e) for i in range(10) for e in events.events_for_trip(_trip(i))]
    consumer.handle(_kinesis_event(payloads))
    _, again = consumer.handle(_kinesis_event(payloads))
    assert again.accepted == 0
    assert again.duplicates == 40
    counters = run.scan_counters(settings)
    assert (
        sum(c["requested"] + c["driver_on_scene"] + c["picked_up"] + c["dropped_off"] for c in counters) == 40
    )
    # the retried batch overwrites the same landing object (deterministic key)
    objects = client("s3", settings).list_objects_v2(Bucket=settings.landing_bucket)["Contents"]
    assert len(objects) == 1


def test_transient_failure_reports_earliest_uncommitted_sequence(aws, monkeypatch):
    settings, consumer = aws
    payloads = [events.encode(e) for i in range(60) for e in events.events_for_trip(_trip(i, zone=1 + i))]
    calls = {"n": 0}
    original = consumer._commit_chunk

    def flaky(chunk):
        calls["n"] += 1
        if calls["n"] == 2:
            raise ClientError(
                {"Error": {"Code": "InternalServerError", "Message": "boom"}}, "TransactWriteItems"
            )
        return original(chunk)

    monkeypatch.setattr(consumer, "_commit_chunk", flaky)
    response, result = consumer.handle(_kinesis_event(payloads))
    failed_from = response["batchItemFailures"][0]["itemIdentifier"]
    assert result.failed_from == failed_from
    # retry from the reported sequence number: everything ends up counted exactly once
    monkeypatch.setattr(consumer, "_commit_chunk", original)
    idx = int(failed_from) - 1000
    consumer.handle({"Records": _kinesis_event(payloads)["Records"][idx:]})
    counters = run.scan_counters(settings)
    assert sum(c["requested"] for c in counters) == 60
    assert sum(c["dropped_off"] for c in counters) == 60


def test_pack_transactions_respects_item_limit():
    groups = [[({"i": i}, {"sequence_number": str(i)})] * size for i, size in enumerate([150, 3, 3, 97, 1])]
    chunks = consumer_mod.pack_transactions(groups)
    assert sum(len(c) for c in chunks) == 254
    for chunk in chunks:
        distinct_groups = len({id(x[0]) for x in chunk})
        assert len(chunk) + distinct_groups <= consumer_mod.MAX_TRANSACTION_ITEMS


def test_handler_reads_config_from_env(aws, monkeypatch):
    settings, consumer = aws
    dlq_url = consumer.config.dlq_url
    monkeypatch.setenv("UBER_DEDUPE_TABLE", settings.dedupe_table)
    monkeypatch.setenv("UBER_COUNTERS_TABLE", settings.counters_table)
    monkeypatch.setenv("UBER_DLQ_URL", dlq_url)
    monkeypatch.setenv("UBER_LANDING_BUCKET", settings.landing_bucket)
    monkeypatch.setenv("AWS_REGION", settings.aws_region)
    monkeypatch.setattr(consumer_mod, "_CONSUMER", None)
    payloads = [events.encode(e) for e in events.events_for_trip(_trip(1))]
    assert consumer_mod.handler(_kinesis_event(payloads)) == {"batchItemFailures": []}
    assert json.loads(json.dumps(run.scan_counters(settings), default=str))
