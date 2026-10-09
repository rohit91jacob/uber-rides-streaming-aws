"""Settings, read from ``UBER_*`` environment variables (see ``.env.example``).

Every setting has a safe local default, so ``uber-rides`` works from a fresh clone with no
configuration: AWS services are emulated in-process with moto, Iceberg lives on the local
filesystem behind a SQLite (SQL) catalog, and dbt runs on DuckDB.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field, replace
from pathlib import Path


class ConfigError(ValueError):
    """A setting is missing or invalid."""


def _env(name: str, default: str | None = None) -> str | None:
    value = os.environ.get(name)
    return default if value is None or value == "" else value


def _int(name: str, default: int) -> int:
    raw = _env(name)
    try:
        return default if raw is None else int(raw)
    except ValueError as exc:
        raise ConfigError(f"{name} must be an integer, got {raw!r}") from exc


def _float(name: str, default: float) -> float:
    raw = _env(name)
    try:
        return default if raw is None else float(raw)
    except ValueError as exc:
        raise ConfigError(f"{name} must be a number, got {raw!r}") from exc


def _bool(name: str, default: bool) -> bool:
    raw = _env(name)
    if raw is None:
        return default
    if raw.lower() in {"1", "true", "yes", "on"}:
        return True
    if raw.lower() in {"0", "false", "no", "off"}:
        return False
    raise ConfigError(f"{name} must be a boolean, got {raw!r}")


AWS_MODES = ("moto", "endpoint", "aws")
CATALOG_TYPES = ("sql", "glue")


@dataclass(frozen=True)
class Settings:
    # ------------------------------------------------------------------ storage
    data_dir: Path = Path("data")
    # Iceberg warehouse root: a local path or s3://bucket/prefix
    warehouse: str | None = None
    catalog_type: str = "sql"
    catalog_name: str = "uber"
    catalog_uri: str | None = None
    namespace: str = "uber_lake"
    # ------------------------------------------------------------------ sources
    tlc_base_url: str = "https://d37ci6vzurychx.cloudfront.net/trip-data"
    zones_url: str = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
    # A directory that already holds fhvhv_tripdata_YYYY-MM.parquet / taxi_zone_lookup.csv
    # (fixtures, or an earlier download). Files found there are used instead of downloading.
    tlc_source_dir: Path | None = None
    license_num: str = "HV0003"
    weather_url: str = "https://archive-api.open-meteo.com/v1/archive"
    weather_source_dir: Path | None = None
    weather_latitude: float = 40.7128
    weather_longitude: float = -74.0060
    weather_timezone: str = "America/New_York"
    monthly_report_url: str = "https://www.nyc.gov/assets/tlc/downloads/csv/data_reports_monthly.csv"
    base_aggregate_url: str = "https://data.cityofnewyork.us/resource/2v9c-2k7f.json"
    http_timeout: float = 120.0
    http_retries: int = 5
    # nyc.gov's CDN rejects user agents that do not start with "Mozilla/5.0" or that contain a URL.
    user_agent: str = "Mozilla/5.0 uber-rides-streaming-aws/0.1"
    # ------------------------------------------------------------------ AWS (emulated by default)
    aws_mode: str = "moto"
    aws_endpoint_url: str | None = None
    aws_region: str = "us-east-1"
    s3_endpoint_url: str | None = None
    kinesis_stream: str = "uber-ride-events"
    kinesis_shards: int = 2
    dedupe_table: str = "uber-ride-event-dedupe"
    counters_table: str = "uber-zone-supply-demand"
    dlq_queue: str = "uber-ride-events-dlq"
    landing_bucket: str = "uber-rides-landing"
    landing_prefix: str = "events"
    counter_window_minutes: int = 15
    dedupe_ttl_days: int = 7
    # ------------------------------------------------------------------ streaming replay
    replay_speedup: float = 0.0
    consumer_batch_size: int = 500
    consumer_max_retries: int = 3
    # ------------------------------------------------------------------ batch
    dq_fail_on_error: bool = True
    duckdb_path: Path | None = None
    duckdb_memory_limit: str = "4GB"
    dbt_target: str = "dev"
    log_level: str = "INFO"
    log_format: str = "json"
    extra: dict[str, str] = field(default_factory=dict)

    # ------------------------------------------------------------------ derived paths
    @property
    def landing_dir(self) -> Path:
        return self.data_dir / "landing"

    @property
    def tlc_landing_dir(self) -> Path:
        return self.landing_dir / "tlc"

    @property
    def weather_landing_dir(self) -> Path:
        return self.landing_dir / "weather"

    @property
    def published_dir(self) -> Path:
        return self.landing_dir / "published"

    @property
    def events_landing_dir(self) -> Path:
        """Local mirror of the s3://<landing_bucket>/<landing_prefix>/ objects."""
        return self.landing_dir / "events"

    @property
    def ops_dir(self) -> Path:
        return self.data_dir / "ops"

    @property
    def warehouse_uri(self) -> str:
        if self.warehouse:
            return self.warehouse
        return (self.data_dir / "warehouse").resolve().as_uri()

    @property
    def catalog_db_uri(self) -> str:
        if self.catalog_uri:
            return self.catalog_uri
        path = (self.data_dir / "catalog" / "iceberg.db").resolve()
        return f"sqlite:///{path.as_posix()}"

    @property
    def serving_db(self) -> Path:
        return self.duckdb_path or self.data_dir / "serving" / "uber.duckdb"

    @property
    def counter_window_seconds(self) -> int:
        return self.counter_window_minutes * 60

    def with_overrides(self, **kwargs: object) -> Settings:
        return replace(self, **kwargs)

    def validate(self) -> Settings:
        if self.aws_mode not in AWS_MODES:
            raise ConfigError(f"UBER_AWS_MODE must be one of {AWS_MODES}, got {self.aws_mode!r}")
        if self.aws_mode == "endpoint" and not self.aws_endpoint_url:
            raise ConfigError("UBER_AWS_MODE=endpoint needs UBER_AWS_ENDPOINT_URL")
        if self.catalog_type not in CATALOG_TYPES:
            raise ConfigError(f"UBER_CATALOG_TYPE must be one of {CATALOG_TYPES}")
        if self.counter_window_minutes <= 0 or 60 % self.counter_window_minutes:
            raise ConfigError("UBER_COUNTER_WINDOW_MINUTES must divide 60")
        if self.kinesis_shards < 1:
            raise ConfigError("UBER_KINESIS_SHARDS must be >= 1")
        if self.replay_speedup < 0:
            raise ConfigError("UBER_REPLAY_SPEEDUP must be >= 0 (0 = as fast as possible)")
        if not 1 <= self.consumer_batch_size <= 10_000:
            raise ConfigError("UBER_CONSUMER_BATCH_SIZE must be between 1 and 10000")
        return self

    @classmethod
    def from_env(cls) -> Settings:
        source_dir = _env("UBER_TLC_SOURCE_DIR")
        weather_dir = _env("UBER_WEATHER_SOURCE_DIR")
        duckdb_path = _env("UBER_DUCKDB_PATH")
        endpoint = _env("UBER_AWS_ENDPOINT_URL")
        settings = cls(
            data_dir=Path(_env("UBER_DATA_DIR", "data")).expanduser(),
            warehouse=_env("UBER_WAREHOUSE"),
            catalog_type=_env("UBER_CATALOG_TYPE", "sql"),
            catalog_name=_env("UBER_CATALOG_NAME", "uber"),
            catalog_uri=_env("UBER_CATALOG_URI"),
            namespace=_env("UBER_NAMESPACE", "uber_lake"),
            tlc_base_url=_env("UBER_TLC_BASE_URL", cls.tlc_base_url).rstrip("/"),
            zones_url=_env("UBER_ZONES_URL", cls.zones_url),
            tlc_source_dir=Path(source_dir).expanduser() if source_dir else None,
            license_num=_env("UBER_LICENSE_NUM", "HV0003"),
            weather_url=_env("UBER_WEATHER_URL", cls.weather_url),
            weather_source_dir=Path(weather_dir).expanduser() if weather_dir else None,
            weather_latitude=_float("UBER_WEATHER_LATITUDE", 40.7128),
            weather_longitude=_float("UBER_WEATHER_LONGITUDE", -74.0060),
            weather_timezone=_env("UBER_WEATHER_TIMEZONE", "America/New_York"),
            monthly_report_url=_env("UBER_MONTHLY_REPORT_URL", cls.monthly_report_url),
            base_aggregate_url=_env("UBER_BASE_AGGREGATE_URL", cls.base_aggregate_url),
            http_timeout=_float("UBER_HTTP_TIMEOUT_SECONDS", 120.0),
            http_retries=_int("UBER_HTTP_RETRIES", 5),
            user_agent=_env("UBER_HTTP_USER_AGENT", cls.user_agent),
            aws_mode=_env("UBER_AWS_MODE", "endpoint" if endpoint else "moto"),
            aws_endpoint_url=endpoint,
            aws_region=_env("AWS_REGION", _env("AWS_DEFAULT_REGION", "us-east-1")),
            s3_endpoint_url=_env("UBER_S3_ENDPOINT_URL", endpoint),
            kinesis_stream=_env("UBER_KINESIS_STREAM", cls.kinesis_stream),
            kinesis_shards=_int("UBER_KINESIS_SHARDS", 2),
            dedupe_table=_env("UBER_DEDUPE_TABLE", cls.dedupe_table),
            counters_table=_env("UBER_COUNTERS_TABLE", cls.counters_table),
            dlq_queue=_env("UBER_DLQ_QUEUE", cls.dlq_queue),
            landing_bucket=_env("UBER_LANDING_BUCKET", cls.landing_bucket),
            landing_prefix=_env("UBER_LANDING_PREFIX", cls.landing_prefix).strip("/"),
            counter_window_minutes=_int("UBER_COUNTER_WINDOW_MINUTES", 15),
            dedupe_ttl_days=_int("UBER_DEDUPE_TTL_DAYS", 7),
            replay_speedup=_float("UBER_REPLAY_SPEEDUP", 0.0),
            consumer_batch_size=_int("UBER_CONSUMER_BATCH_SIZE", 500),
            consumer_max_retries=_int("UBER_CONSUMER_MAX_RETRIES", 3),
            dq_fail_on_error=_bool("UBER_DQ_FAIL_ON_ERROR", True),
            duckdb_path=Path(duckdb_path).expanduser() if duckdb_path else None,
            duckdb_memory_limit=_env("UBER_DUCKDB_MEMORY_LIMIT", "4GB"),
            dbt_target=_env("DBT_TARGET", "dev"),
            log_level=_env("UBER_LOG_LEVEL", "INFO"),
            log_format=_env("UBER_LOG_FORMAT", "json"),
        )
        return settings.validate()
