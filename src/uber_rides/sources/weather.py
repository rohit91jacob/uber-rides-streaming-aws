"""Hourly NYC weather from the Open-Meteo historical archive API (no key, CC BY 4.0).

Requested in ``America/New_York`` local time so hours line up with TLC timestamps, which are
local wall-clock times.
"""

from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from uber_rides.config import Settings
from uber_rides.logs import get_logger
from uber_rides.sources.http import session
from uber_rides.sources.tlc import month_bounds

log = get_logger(__name__)

HOURLY_VARIABLES = (
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "rain",
    "snowfall",
    "cloud_cover",
    "wind_speed_10m",
    "weather_code",
)


def weather_file(settings: Settings, month: str) -> Path:
    return settings.weather_landing_dir / f"open_meteo_hourly_{month}.json"


def fetch_month(settings: Settings, month: str, *, force: bool = False) -> dict:
    """Land the month's hourly weather JSON (idempotent; re-fetched only with ``force``)."""
    target = weather_file(settings, month)
    if settings.weather_source_dir:
        local = settings.weather_source_dir / target.name
        if local.exists():
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(local.read_bytes())
            return _summary(target, source=str(local))
    if target.exists() and not force:
        return _summary(target, source="cache")
    start, end = month_bounds(month)
    last_day = min(end - timedelta(days=1), date.today() - timedelta(days=6))
    params = {
        "latitude": settings.weather_latitude,
        "longitude": settings.weather_longitude,
        "start_date": start.isoformat(),
        "end_date": last_day.isoformat(),
        "hourly": ",".join(HOURLY_VARIABLES),
        "timezone": settings.weather_timezone,
    }
    r = session(settings).get(settings.weather_url, params=params, timeout=settings.http_timeout)
    r.raise_for_status()
    payload = r.json()
    if "hourly" not in payload:
        raise ValueError(f"Open-Meteo response has no hourly block: {str(payload)[:200]}")
    payload["_fetched_at"] = datetime.now(UTC).isoformat(timespec="seconds")
    payload["_request"] = params
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(payload), "utf-8")
    log.info("weather landed", extra={"month": month, "hours": len(payload["hourly"]["time"])})
    return _summary(target, source=settings.weather_url)


def _summary(path: Path, source: str) -> dict:
    payload = json.loads(path.read_text("utf-8"))
    times = payload["hourly"]["time"]
    return {"file": str(path), "source": source, "hours": len(times), "first": times[0], "last": times[-1]}


def hourly_rows(path: Path) -> list[dict]:
    """Flatten the Open-Meteo hourly arrays into one dict per hour (local time, naive)."""
    payload = json.loads(path.read_text("utf-8"))
    hourly = payload["hourly"]
    rows = []
    for i, t in enumerate(hourly["time"]):
        row = {"observed_hour": datetime.fromisoformat(t)}
        for var in HOURLY_VARIABLES:
            values = hourly.get(var)
            row[var] = None if values is None else values[i]
        rows.append(row)
    return rows
