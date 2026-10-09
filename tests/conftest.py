from __future__ import annotations

from pathlib import Path

import pytest

from uber_rides.config import Settings

FIXTURES = Path(__file__).parent / "fixtures"
TLC_FIXTURES = FIXTURES / "tlc"
WEATHER_FIXTURES = FIXTURES / "weather"
PUBLISHED_FIXTURES = FIXTURES / "published"
MONTH = "2026-06"


@pytest.fixture
def settings(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Settings:
    for var in ("UBER_AWS_ENDPOINT_URL", "UBER_AWS_MODE", "AWS_PROFILE"):
        monkeypatch.delenv(var, raising=False)
    return Settings(
        data_dir=tmp_path / "lake",
        tlc_source_dir=TLC_FIXTURES,
        weather_source_dir=WEATHER_FIXTURES,
        log_format="text",
        duckdb_memory_limit="1GB",
    ).validate()
