"""Integration tests against a real emulator *endpoint* (LocalStack in CI, or ``moto_server``).

Run with ``UBER_AWS_ENDPOINT_URL=http://localhost:4566 pytest -m integration``. Unlike the
unit tests (moto in-process), every call here crosses HTTP to a separate emulator process,
exactly as it would against AWS.
"""

from __future__ import annotations

import json
import os
import uuid
from datetime import datetime

import boto3
import pytest

from uber_rides.lake import load
from uber_rides.lake.catalog import get_catalog
from uber_rides.lake.tables import TAXI_ZONES
from uber_rides.orchestration import asl
from uber_rides.sources import tlc, weather
from uber_rides.streaming.aws import aws_context, bootstrap
from uber_rides.streaming.run import run_stream

ENDPOINT = os.environ.get("UBER_AWS_ENDPOINT_URL")
pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(not ENDPOINT, reason="UBER_AWS_ENDPOINT_URL is not set"),
]


@pytest.fixture
def endpoint_settings(settings, monkeypatch):
    for k in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"):
        monkeypatch.setenv(k, "test")
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    suffix = uuid.uuid4().hex[:8]
    return settings.with_overrides(
        aws_mode="endpoint",
        aws_endpoint_url=ENDPOINT,
        s3_endpoint_url=ENDPOINT,
        aws_region="us-east-1",
        kinesis_stream=f"uber-it-{suffix}",
        kinesis_shards=1,
        dedupe_table=f"uber-it-dedupe-{suffix}",
        counters_table=f"uber-it-counters-{suffix}",
        dlq_queue=f"uber-it-dlq-{suffix}",
        landing_bucket=f"uber-it-landing-{suffix}",
    ).validate()


def test_replay_through_emulator_is_exactly_once(endpoint_settings):
    s = endpoint_settings
    tlc.ingest_month(s, "2026-06")
    load.load_trips(s, "2026-06")
    summary = run_stream(
        s,
        start=datetime(2026, 6, 12, 18, 0),
        end=datetime(2026, 6, 12, 18, 1),
        sample_rate=0.25,
        duplicate_every=5,
        malformed_every=9,
        replays=2,
    )
    assert summary["aws_mode"] == "endpoint"
    assert summary["reconciliation"]["ok"], summary["reconciliation"]
    assert summary["consumer"]["accepted"] == summary["replays"][0]["events"] > 0
    assert summary["dlq_messages"] == sum(r["malformed_injected"] for r in summary["replays"])
    # bootstrap is idempotent against a live endpoint
    with aws_context(s):
        assert bootstrap(s)["stream"] == s.kinesis_stream


def test_iceberg_table_on_emulated_s3(endpoint_settings):
    s = endpoint_settings
    bucket = f"{s.landing_bucket}-lake"
    boto3.client("s3", endpoint_url=ENDPOINT, region_name="us-east-1").create_bucket(Bucket=bucket)
    s3_lake = s.with_overrides(warehouse=f"s3://{bucket}/warehouse")
    tlc.ingest_zones(s3_lake)
    assert load.load_zones(s3_lake) == {"zones": 265}
    table = get_catalog(s3_lake).load_table(TAXI_ZONES.identifier(s3_lake))
    assert table.metadata_location.startswith(f"s3://{bucket}/")
    assert table.scan().to_arrow().num_rows == 265
    weather.fetch_month(s3_lake, "2026-06")


def test_state_machine_definition_is_accepted(endpoint_settings):
    sfn = boto3.client("stepfunctions", endpoint_url=ENDPOINT, region_name="us-east-1")
    arn = sfn.create_state_machine(
        name=f"uber-it-{uuid.uuid4().hex[:8]}",
        definition=json.dumps(asl.load_definition("arn:aws:lambda:us-east-1:000000000000:function:batch")),
        roleArn="arn:aws:iam::000000000000:role/sfn",
    )["stateMachineArn"]
    assert sfn.describe_state_machine(stateMachineArn=arn)["name"].startswith("uber-it-")
