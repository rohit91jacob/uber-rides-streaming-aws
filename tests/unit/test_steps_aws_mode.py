"""The AWS-mode branch of the event-load step, with every AWS call served by moto."""

from __future__ import annotations

import gzip
import json
from datetime import datetime

from uber_rides.lake import load
from uber_rides.orchestration import steps
from uber_rides.sources import tlc
from uber_rides.streaming import events
from uber_rides.streaming.aws import aws_context, bootstrap, client


def test_load_events_in_aws_mode_pulls_s3_counters_and_dlq(settings):
    tlc.ingest_month(settings, "2026-06")
    load.load_trips(settings, "2026-06")
    trip = {
        "trip_id": "HV0003-2026-06-000000001",
        "request_datetime": datetime(2026, 6, 1, 8, 0),
        "on_scene_datetime": datetime(2026, 6, 1, 8, 3),
        "pickup_datetime": datetime(2026, 6, 1, 8, 4),
        "dropoff_datetime": datetime(2026, 6, 1, 8, 20),
        "pu_location_id": 161,
        "do_location_id": 230,
        "trip_miles": 2.0,
        "trip_time_s": 960,
    }
    lines = "".join(json.dumps(e) + "\n" for e in events.events_for_trip(trip))
    with aws_context(settings):
        res = bootstrap(settings)
        client("s3", settings).put_object(
            Bucket=settings.landing_bucket,
            Key=f"{settings.landing_prefix}/shard=shardId-000000000000/1-4.jsonl.gz",
            Body=gzip.compress(lines.encode()),
        )
        client("sqs", settings).send_message(
            QueueUrl=res["dlq_url"],
            MessageBody=json.dumps(
                {"reason": "malformed_json", "raw": "x", "received_at": "2026-06-01T08:00:00+00:00"}
            ),
        )
        aws = settings.with_overrides(aws_mode="aws")
        first = steps.step_load_events(aws, {})
        second = steps.step_load_events(aws, {})
    assert first["landing_objects_fetched"] == 1 and first["rows_loaded"] == 4
    assert first["dlq_messages"] == 1
    assert second["landing_objects_fetched"] == 0 and second["rows_loaded"] == 0
