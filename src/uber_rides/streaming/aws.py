"""boto3 clients for the three modes, plus emulator bootstrap.

* ``moto`` (default): every AWS call is served in-process by moto. Nothing leaves the machine;
  fake credentials are forced so a real account can never be touched.
* ``endpoint``: calls go to ``UBER_AWS_ENDPOINT_URL`` (LocalStack in CI, or moto server).
* ``aws``: real AWS with the default credential chain. Resources must already exist
  (``infra/`` Terraform); :func:`bootstrap` refuses to run in this mode.
"""

from __future__ import annotations

import contextlib
import os
import time
from collections.abc import Iterator
from typing import Any

import boto3
from botocore.config import Config

from uber_rides.config import Settings
from uber_rides.logs import get_logger

log = get_logger(__name__)

FAKE_CREDENTIALS = {
    "AWS_ACCESS_KEY_ID": "testing",
    "AWS_SECRET_ACCESS_KEY": "testing",
    "AWS_SESSION_TOKEN": "testing",
}


def client(service: str, settings: Settings) -> Any:
    kw: dict[str, Any] = {
        "region_name": settings.aws_region,
        "config": Config(retries={"mode": "standard", "max_attempts": 8}),
    }
    if settings.aws_mode == "endpoint":
        kw["endpoint_url"] = settings.aws_endpoint_url
    return boto3.client(service, **kw)


@contextlib.contextmanager
def aws_context(settings: Settings) -> Iterator[None]:
    """Activate moto for ``moto`` mode; otherwise a no-op."""
    if settings.aws_mode != "moto":
        if settings.aws_mode == "endpoint":
            for k, v in FAKE_CREDENTIALS.items():
                os.environ.setdefault(k, v)
        yield
        return
    from moto import mock_aws

    from uber_rides.streaming import moto_shim

    moto_shim.install()

    saved = {k: os.environ.get(k) for k in (*FAKE_CREDENTIALS, "AWS_PROFILE")}
    os.environ.update(FAKE_CREDENTIALS)
    os.environ.pop("AWS_PROFILE", None)
    try:
        with mock_aws():
            yield
    finally:
        for k, v in saved.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


def queue_url(settings: Settings) -> str:
    return client("sqs", settings).get_queue_url(QueueName=settings.dlq_queue)["QueueUrl"]


def stream_arn(settings: Settings) -> str:
    desc = client("kinesis", settings).describe_stream_summary(StreamName=settings.kinesis_stream)
    return desc["StreamDescriptionSummary"]["StreamARN"]


def bootstrap(settings: Settings) -> dict[str, str]:
    """Create the stream, tables, queue and bucket on an emulator (idempotent)."""
    if settings.aws_mode == "aws":
        raise RuntimeError("bootstrap only runs against emulators; use infra/ Terraform for AWS")
    kinesis = client("kinesis", settings)
    ddb = client("dynamodb", settings)
    sqs = client("sqs", settings)
    s3 = client("s3", settings)

    with contextlib.suppress(kinesis.exceptions.ResourceInUseException):
        kinesis.create_stream(StreamName=settings.kinesis_stream, ShardCount=settings.kinesis_shards)
    for _ in range(60):
        status = kinesis.describe_stream_summary(StreamName=settings.kinesis_stream)[
            "StreamDescriptionSummary"
        ]["StreamStatus"]
        if status == "ACTIVE":
            break
        time.sleep(0.5)

    tables = {
        settings.dedupe_table: [{"AttributeName": "event_id", "KeyType": "HASH"}],
        settings.counters_table: [
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
    }
    for name, schema in tables.items():
        try:
            ddb.create_table(
                TableName=name,
                KeySchema=schema,
                AttributeDefinitions=[
                    {"AttributeName": k["AttributeName"], "AttributeType": "S"} for k in schema
                ],
                BillingMode="PAY_PER_REQUEST",
            )
            ddb.get_waiter("table_exists").wait(TableName=name)
            ddb.update_time_to_live(
                TableName=name, TimeToLiveSpecification={"Enabled": True, "AttributeName": "expires_at"}
            )
        except ddb.exceptions.ResourceInUseException:
            pass

    url = sqs.create_queue(QueueName=settings.dlq_queue, Attributes={"MessageRetentionPeriod": "1209600"})[
        "QueueUrl"
    ]
    try:
        if settings.aws_region == "us-east-1":
            s3.create_bucket(Bucket=settings.landing_bucket)
        else:
            s3.create_bucket(
                Bucket=settings.landing_bucket,
                CreateBucketConfiguration={"LocationConstraint": settings.aws_region},
            )
    except (s3.exceptions.BucketAlreadyOwnedByYou, s3.exceptions.BucketAlreadyExists):
        pass
    log.info("emulator bootstrapped", extra={"mode": settings.aws_mode})
    return {"stream": settings.kinesis_stream, "dlq_url": url, "bucket": settings.landing_bucket}
