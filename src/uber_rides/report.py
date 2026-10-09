"""Static results site (``uber-rides report --out site``), published by ``refresh.yml``.

Every number on the page is read from the dbt marts in the serving DuckDB or from the ops
JSON written by the pipeline (stream run, data-quality gates, verification). Nothing is typed
in by hand. Charts are inline SVG (one series each, hover tooltips via ``<title>``) and every
chart has its data repeated in a table.
"""

from __future__ import annotations

import html
import json
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from uber_rides.config import Settings
from uber_rides.lake.serving import connect
from uber_rides.logs import get_logger

log = get_logger(__name__)

CSS = """
:root { color-scheme: light; --surface:#fcfcfb; --fg:#0b0b0b; --muted:#52514e; --line:#dedcd5;
  --series:#2a78d6; --accent:#1f5fae; --ok:#008300; --bad:#c62828; --warn:#a66a00; }
@media (prefers-color-scheme: dark) { :root { color-scheme: dark; --surface:#1a1a19; --fg:#ffffff;
  --muted:#c3c2b7; --line:#3a3936; --series:#3987e5; --accent:#7fb2f0; --ok:#5cc35c; --bad:#ff7b72; --warn:#e0a84a; } }
* { box-sizing: border-box; }
body { margin: 0 auto; max-width: 72rem; padding: 0 1rem 3rem; background: var(--surface); color: var(--fg);
  font: 15px/1.5 system-ui, -apple-system, "Segoe UI", sans-serif; }
header { padding: 1.5rem 0 1rem; border-bottom: 1px solid var(--line); }
h1 { margin: 0 0 .25rem; font-size: 1.6rem; } h2 { margin-top: 2.2rem; font-size: 1.2rem; }
a { color: var(--accent); }
.meta, .note, caption, footer { color: var(--muted); font-size: .85rem; }
.tiles { display: grid; grid-template-columns: repeat(auto-fit, minmax(10.5rem, 1fr)); gap: .75rem; margin-top: 1rem; }
.tile { border: 1px solid var(--line); border-radius: .5rem; padding: .75rem; }
.tile .v { font-size: 1.45rem; font-weight: 600; } .tile .k { color: var(--muted); font-size: .8rem; }
table { border-collapse: collapse; width: 100%; margin: .5rem 0 1rem; font-variant-numeric: tabular-nums; }
caption { text-align: left; padding: .25rem 0; }
th, td { padding: .35rem .55rem; border-bottom: 1px solid var(--line); text-align: left; }
td.n, th.n { text-align: right; }
.pass { color: var(--ok); } .fail { color: var(--bad); } .warn { color: var(--warn); }
svg text { fill: var(--muted); font-size: 11px; } svg .bar { fill: var(--series); } svg .bar:hover { opacity: .75; }
svg .axis { stroke: var(--line); }
details summary { cursor: pointer; color: var(--muted); }
.sim { border-left: 3px solid var(--warn); padding: .25rem .75rem; }
footer { border-top: 1px solid var(--line); margin-top: 3rem; padding-top: 1rem; }
"""


# --------------------------------------------------------------------------- formatting
def _esc(v: Any) -> str:
    return html.escape("" if v is None else str(v))


def fmt(v: Any, places: int = 2) -> str:
    if v is None:
        return "-"
    if isinstance(v, bool):
        return "yes" if v else "no"
    if isinstance(v, int):
        return f"{v:,}"
    if isinstance(v, float):
        return f"{v:,.{places}f}"
    return str(v)


def pct(v: Any, places: int = 1) -> str:
    return "-" if v is None else f"{100 * float(v):.{places}f}%"


def table(
    caption: str, headers: Sequence[str], rows: Sequence[Sequence[Any]], numeric: set[int] | None = None
) -> str:
    numeric = (
        numeric
        if numeric is not None
        else {i for i, v in enumerate(rows[0]) if isinstance(v, int | float)}
        if rows
        else set()
    )
    head = "".join(f'<th class="{"n" if i in numeric else ""}">{_esc(h)}</th>' for i, h in enumerate(headers))
    body = "".join(
        "<tr>"
        + "".join(f'<td class="{"n" if i in numeric else ""}">{c}</td>' for i, c in enumerate(r))
        + "</tr>"
        for r in rows
    )
    if not rows:
        body = f'<tr><td colspan="{len(headers)}" class="note">no rows</td></tr>'
    return f"<table><caption>{_esc(caption)}</caption><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table>"


def bar_chart(
    title: str,
    labels: Sequence[str],
    values: Sequence[float],
    unit: str = "",
    width: int = 760,
    height: int = 220,
) -> str:
    """Single-series vertical bars, baseline-anchored, 2px gaps, hover tooltip per bar."""
    if not values:
        return ""
    top = max(values) or 1.0
    pad_l, pad_b, pad_t = 8, 24, 12
    n = len(values)
    slot = (width - pad_l * 2) / n
    bar_w = max(slot - 2, 1)
    plot_h = height - pad_b - pad_t
    parts = [
        f'<svg viewBox="0 0 {width} {height}" width="100%" role="img" aria-label="{_esc(title)}">',
        f"<title>{_esc(title)}</title>",
        f'<line class="axis" x1="{pad_l}" x2="{width - pad_l}" y1="{height - pad_b}" y2="{height - pad_b}"/>',
    ]
    step = max(1, n // 12)
    for i, (lab, val) in enumerate(zip(labels, values, strict=True)):
        h = plot_h * (val / top)
        x = pad_l + i * slot + 1
        y = height - pad_b - h
        r = min(4, bar_w / 2, h / 2) if h > 0 else 0
        parts.append(
            f'<path class="bar" d="M{x:.1f},{height - pad_b} V{y + r:.1f} Q{x:.1f},{y:.1f} {x + r:.1f},{y:.1f} '
            f'H{x + bar_w - r:.1f} Q{x + bar_w:.1f},{y:.1f} {x + bar_w:.1f},{y + r:.1f} V{height - pad_b} Z">'
            f"<title>{_esc(lab)}: {fmt(val, 0 if top >= 100 else 2)}{_esc(unit)}</title></path>"
        )
        if i % step == 0:
            parts.append(
                f'<text x="{x + bar_w / 2:.1f}" y="{height - 8}" text-anchor="middle">{_esc(lab)}</text>'
            )
    parts.append(
        f'<text x="{pad_l}" y="{pad_t - 2}">max {fmt(top, 0 if top >= 100 else 2)}{_esc(unit)}</text></svg>'
    )
    return "".join(parts)


# --------------------------------------------------------------------------- data
def _rows(con: Any, sql: str) -> list[tuple]:
    try:
        return con.execute(sql).fetchall()
    except Exception as exc:  # a mart missing on a partial build
        log.warning("report query failed", extra={"error": str(exc).splitlines()[0][:200]})
        return []


def _json(path: Path) -> dict | None:
    return json.loads(path.read_text("utf-8")) if path.exists() else None


def collect(settings: Settings) -> dict[str, Any]:
    con = connect(settings, read_only=True)
    try:
        q = lambda sql: _rows(con, sql)  # noqa: E731
        headline = q(
            """SELECT count(*) AS trips, min(request_date), max(request_date), avg(trip_miles), avg(base_fare),
                      avg(driver_pay), sum(driver_pay) / nullif(sum(base_fare), 0),
                      avg(CASE WHEN wait_seconds >= 0 THEN wait_seconds END) / 60.0,
                      count(*) FILTER (WHERE is_cbd_charged), count(*) FILTER (WHERE airport_direction IS NOT NULL),
                      count(*) FILTER (WHERE is_shared_request), count(DISTINCT source_month)
               FROM intermediate.int_trips_enriched"""
        )
        months = q("SELECT DISTINCT source_month FROM intermediate.int_trips_enriched ORDER BY 1")
        data = {
            "headline": headline[0] if headline else None,
            "months": [m[0] for m in months],
            "by_hour": q(
                """SELECT request_hour_of_day, count(*) * 1.0 / count(DISTINCT request_date) AS per_day,
                          avg(CASE WHEN wait_seconds >= 0 THEN wait_seconds END) / 60.0
                   FROM intermediate.int_trips_enriched GROUP BY 1 ORDER BY 1"""
            ),
            "wait_hist": q(
                "SELECT wait_bucket, sum(trips) FROM marts.mart_wait_time_distribution GROUP BY 1 ORDER BY 1"
            ),
            "wait_pct": q(
                """SELECT borough, sum(trips), sum(p50_wait_minutes * trips) / sum(trips),
                          sum(p90_wait_minutes * trips) / sum(trips)
                   FROM marts.mart_wait_time_percentiles WHERE borough NOT IN ('Unknown', 'N/A')
                   GROUP BY 1 ORDER BY 2 DESC"""
            ),
            "fast_pickup": q(
                """SELECT borough, sum(requests), sum(picked_up_within_target) * 1.0 / sum(requests)
                   FROM marts.mart_hourly_zone_demand GROUP BY 1 ORDER BY 2 DESC"""
            ),
            "top_zones": q(
                """SELECT zone_name, borough, sum(requests), sum(requests) * 1.0 / nullif(sum(dropoffs_in_zone), 0),
                          sum(picked_up_within_target) * 1.0 / sum(requests)
                   FROM marts.mart_hourly_zone_demand GROUP BY 1, 2 ORDER BY 3 DESC LIMIT 10"""
            ),
            "pay": q(
                """SELECT borough, sum(trips), sum(driver_pay_usd) / sum(base_fare_usd),
                          (sum(driver_pay_usd) + sum(tips_usd)) / (sum(rider_total_ex_tips_usd) + sum(tips_usd)),
                          sum(driver_pay_usd) / sum(trips)
                   FROM marts.mart_driver_pay_share GROUP BY 1 ORDER BY 2 DESC"""
            ),
            "airport": q(
                """SELECT airport_code, airport_direction, sum(trips), sum(avg_base_fare_usd * trips) / sum(trips),
                          sum(avg_trip_miles * trips) / sum(trips), sum(airport_fee_usd)
                   FROM marts.mart_airport_trips GROUP BY 1, 2 ORDER BY 3 DESC"""
            ),
            "cbd": q(
                """SELECT is_cbd_charged, sum(trips), sum(cbd_fee_usd), sum(avg_base_fare_usd * trips) / sum(trips),
                          sum(avg_speed_mph * trips) / sum(trips), sum(avg_trip_miles * trips) / sum(trips)
                   FROM marts.mart_cbd_congestion_fee GROUP BY 1 ORDER BY 1"""
            ),
            "shared": q(
                """SELECT sum(shared_requests), sum(shared_matches), sum(shared_matches) * 1.0 / nullif(sum(shared_requests), 0)
                   FROM marts.mart_shared_rides"""
            ),
            "weather": q(
                """SELECT precipitation_band, hours, requests, avg_demand_index, avg_wait_minutes
                   FROM marts.mart_weather_demand_summary ORDER BY 1"""
            ),
            "flows": q(
                """SELECT pu_borough, do_borough, trips, share_of_trips, avg_trip_miles, avg_base_fare_usd
                   FROM marts.mart_borough_flows ORDER BY trips DESC LIMIT 12"""
            ),
            "stream_events": q(
                "SELECT event_type, events, trips FROM marts.mart_stream_event_summary ORDER BY events DESC"
            ),
            "surge": q(
                """SELECT zone_name, borough, window_start, requested, dropped_off, surge_proxy
                   FROM marts.mart_stream_zone_supply_demand WHERE requested >= 3
                   ORDER BY surge_proxy DESC, requested DESC LIMIT 10"""
            ),
        }
    finally:
        con.close()
    ops = settings.ops_dir
    data["stream_run"] = _json(ops / "stream_runs" / "latest.json")
    data["batch_run"] = _json(ops / "batch_runs" / "latest.json")
    data["dq"] = {
        layer: _json(ops / "dq" / f"{layer}.json") for layer in ("landing", "lake", "stream", "marts")
    }
    data["verification"] = {p.stem: _json(p) for p in sorted((ops / "verification").glob("*.json"))}
    return data


# --------------------------------------------------------------------------- page
def _tiles(h: tuple | None) -> str:
    if not h:
        return '<p class="note">No trips loaded yet.</p>'
    trips, first, last, miles, fare, pay, share, wait, cbd, airport, shared, _ = h
    items = [
        ("Uber trips", fmt(trips)),
        ("Request dates", f"{first} to {last}"),
        ("Avg trip miles", fmt(miles)),
        ("Avg base fare", f"${fmt(fare)}"),
        ("Avg driver pay", f"${fmt(pay)}"),
        ("Driver pay / base fare", pct(share)),
        ("Avg request-to-pickup wait", f"{fmt(wait, 1)} min"),
        ("Trips charged the CBD fee", f"{fmt(cbd)} ({pct(cbd / trips if trips else None)})"),
        ("Airport trips", fmt(airport)),
        ("Shared-ride requests", fmt(shared)),
    ]
    return (
        '<div class="tiles">'
        + "".join(
            f'<div class="tile"><div class="v">{_esc(v)}</div><div class="k">{_esc(k)}</div></div>'
            for k, v in items
        )
        + "</div>"
    )


def _status(s: str) -> str:
    cls = {
        "pass": "pass",
        "match": "pass",
        "fail": "fail",
        "mismatch": "fail",
        "close": "warn",
        "skip": "warn",
    }.get(s, "")
    return f'<span class="{cls}">{_esc(s)}</span>'


def _verification(ver: dict[str, dict]) -> str:
    if not ver:
        return '<p class="note">No verification run yet (<code>uber-rides verify --month YYYY-MM</code>).</p>'
    out = []
    for month, v in ver.items():
        rows = [
            [
                _esc(c["check"]),
                fmt(c["ours"]),
                fmt(c["published"]),
                "-" if c["diff_pct"] is None else f"{c['diff_pct']:+.2f}%",
                _status(c["status"]),
                _esc(c["note"]),
            ]
            for c in v["published_checks"]
        ]
        out.append(
            table(
                f"{month}: our figures vs TLC-published figures",
                ["Check", "Ours", "Published", "Diff", "Status", "Note"],
                rows,
                {1, 2, 3},
            )
        )
    return "".join(out)


def _dq(dq: dict[str, dict | None]) -> str:
    rows = []
    for layer, doc in dq.items():
        if not doc:
            continue
        for r in doc["results"]:
            status = r["status"] if r["status"] != "fail" or r["severity"] == "error" else "warn"
            rows.append(
                [
                    _esc(layer),
                    _esc(r["check_name"]),
                    _esc(r["severity"]),
                    _status(status),
                    fmt(r["failing_rows"]),
                    fmt(r["total_rows"]),
                    _esc(r["detail"]),
                ]
            )
    return table(
        "Latest gate run per layer (error-level failures stop the pipeline)",
        ["Layer", "Check", "Severity", "Status", "Failing", "Rows", "Rule"],
        rows,
        {4, 5},
    )


def _stream(run: dict | None, events: list[tuple], surge: list[tuple]) -> str:
    if not run:
        return '<p class="note">No streaming replay has been run yet.</p>'
    c, r = run["consumer"], run["reconciliation"]
    rep = run["replays"]
    rows = [
        [
            "Replay window (request time)",
            f"{run['window']['start']} to {run['window']['end']} (sample rate {run['window']['sample_rate']})",
        ],
        ["Emulation mode", _esc(run["aws_mode"])],
        ["Replays of the window", fmt(len(rep))],
        ["Distinct events produced", fmt(rep[0]["events"])],
        ["Records read from Kinesis", fmt(c["records"])],
        ["Accepted exactly once", fmt(c["accepted"])],
        ["Duplicates absorbed by the DynamoDB condition", fmt(c["duplicates"])],
        ["Malformed records sent to the SQS DLQ", fmt(run["dlq_messages"])],
        [
            "Counter cells compared with batch / mismatched",
            f"{fmt(r['cells_compared'])} / {fmt(r['cells_mismatched'])}",
        ],
        ["Wall time (emulated, single process)", f"{fmt(run['elapsed_s'], 1)} s"],
    ]
    html_rows = table(
        "Latest replay through the emulated Kinesis -> Lambda -> DynamoDB path",
        ["Measure", "Value"],
        rows,
        {1},
    )
    ev = table(
        "Distinct landed events by type (after dedupe)",
        ["Event type", "Events", "Trips"],
        [[_esc(e), fmt(n), fmt(t)] for e, n, t in events],
    )
    sg = table(
        "Highest surge proxy (requests / dropoffs) in the replayed windows, zones with >= 3 requests",
        ["Zone", "Borough", "Window start", "Requested", "Dropped off", "Surge proxy"],
        [[_esc(z), _esc(b), _esc(w), fmt(rq), fmt(d), fmt(s)] for z, b, w, rq, d, s in surge],
    )
    note = (
        '<p class="sim">Simulated: only the <em>delivery</em> is simulated. Event times are the real TLC timestamps; '
        "the replay speed, the injected duplicates and the corrupted records are generated by the producer with a fixed seed.</p>"
    )
    return note + html_rows + ev + sg


def render(data: dict[str, Any], generated: str) -> str:
    h = data["headline"]
    by_hour = data["by_hour"]
    body = [
        "<header><h1>Uber rides in New York City: streaming + batch on AWS (emulated)</h1>",
        f'<p class="meta">TLC High Volume FHV trip records, licence HV0003 (Uber). Months: {_esc(", ".join(data["months"]) or "none")}. '
        f"Generated {generated}. Source code: "
        '<a href="https://github.com/rohit91jacob/uber-rides-streaming-aws">rohit91jacob/uber-rides-streaming-aws</a>.</p></header>',
        _tiles(h),
        "<h2>Demand by hour of day</h2>",
        bar_chart(
            "Average Uber requests per day by request hour",
            [f"{int(r[0]):02d}" for r in by_hour],
            [float(r[1]) for r in by_hour],
        ),
        "<details><summary>Table</summary>"
        + table(
            "Requests per day and average wait by request hour",
            ["Hour", "Requests / day", "Avg wait (min)"],
            [[f"{int(a):02d}", fmt(b, 0), fmt(c, 1)] for a, b, c in by_hour],
        )
        + "</details>",
        "<h2>Wait time (request to pickup)</h2>",
        bar_chart(
            "Trips by wait bucket",
            [r[0] for r in data["wait_hist"]],
            [float(r[1]) for r in data["wait_hist"]],
            " trips",
        ),
        table(
            "Wait percentiles by pickup borough (trip-weighted across time bands)",
            ["Borough", "Trips", "p50 (min)", "p90 (min)"],
            [[_esc(b), fmt(t), fmt(p5, 1), fmt(p9, 1)] for b, t, p5, p9 in data["wait_pct"]],
        ),
        table(
            "Fulfilment proxy: share of requests picked up within 10 minutes (TLC data has no cancellations)",
            ["Borough", "Requests", "Within 10 min"],
            [[_esc(b), fmt(n), pct(p)] for b, n, p in data["fast_pickup"]],
        ),
        table(
            "Busiest pickup zones",
            ["Zone", "Borough", "Requests", "Requests / dropoffs", "Within 10 min"],
            [[_esc(z), _esc(b), fmt(n), fmt(r), pct(p)] for z, b, n, r, p in data["top_zones"]],
        ),
        "<h2>Driver pay share of fare</h2>",
        table(
            "Driver pay (excl. tips) vs base fare, and pay+tips vs rider spend, by pickup borough",
            ["Borough", "Trips", "Pay / base fare", "Pay+tips / rider spend", "Pay per trip"],
            [[_esc(b), fmt(t), pct(s1), pct(s2), f"${fmt(p)}"] for b, t, s1, s2, p in data["pay"]],
        ),
        "<h2>Airport trips</h2>",
        table(
            "JFK, LGA and EWR",
            ["Airport", "Direction", "Trips", "Avg base fare", "Avg miles", "Airport fees"],
            [
                [_esc(a), _esc(d), fmt(t), f"${fmt(f)}", fmt(m), f"${fmt(af, 0)}"]
                for a, d, t, f, m, af in data["airport"]
            ],
        ),
        "<h2>Congestion Relief Zone (CBD) fee</h2>",
        table(
            "Charged vs not charged (descriptive, not causal)",
            ["Charged", "Trips", "Fees collected", "Avg base fare", "Avg speed (mph)", "Avg miles"],
            [
                [fmt(c), fmt(t), f"${fmt(f, 0)}", f"${fmt(af)}", fmt(sp, 1), fmt(m)]
                for c, t, f, af, sp, m in data["cbd"]
            ],
        ),
        "<h2>Shared rides</h2>",
        table(
            "Shared-ride match rate",
            ["Shared requests", "Matched", "Match rate"],
            [[fmt(a), fmt(b), pct(c)] for a, b, c in data["shared"]],
        ),
        "<h2>Weather and demand</h2>",
        table(
            "Demand index = requests in the hour / average for the same weekday and hour (Open-Meteo hourly precipitation)",
            ["Precipitation", "Hours", "Requests", "Avg demand index", "Avg wait (min)"],
            [[_esc(b), fmt(hr), fmt(r), fmt(i, 3), fmt(w, 1)] for b, hr, r, i, w in data["weather"]],
        ),
        "<h2>Borough flows</h2>",
        table(
            "Top origin-destination pairs",
            ["From", "To", "Trips", "Share", "Avg miles", "Avg base fare"],
            [[_esc(a), _esc(b), fmt(t), pct(s), fmt(m), f"${fmt(f)}"] for a, b, t, s, m, f in data["flows"]],
        ),
        "<h2>Streaming path</h2>",
        _stream(data["stream_run"], data["stream_events"], data["surge"]),
        "<h2>Verification against TLC-published figures</h2>",
        _verification(data["verification"]),
        "<h2>Data quality</h2>",
        _dq(data["dq"]),
        "<footer>Data: NYC Taxi &amp; Limousine Commission trip records and aggregated reports (NYC Open Data); weather: "
        '<a href="https://open-meteo.com/">Open-Meteo</a> (CC BY 4.0). AWS services are emulated locally (moto); '
        "nothing here ran in a real AWS account. See DATA_LICENSE.md.</footer>",
    ]
    return (
        '<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">'
        f"<title>Uber rides NYC - results</title><style>{CSS}</style></head><body>"
        + "".join(body)
        + "</body></html>"
    )


def build_site(settings: Settings, out: Path) -> dict[str, Any]:
    data = collect(settings)
    generated = datetime.now(UTC).strftime("%Y-%m-%d %H:%M UTC")
    out.mkdir(parents=True, exist_ok=True)
    (out / "index.html").write_text(render(data, generated), "utf-8")
    h = data["headline"]
    summary = {
        "generated": generated,
        "months": data["months"],
        "trips": h[0] if h else 0,
        "avg_trip_miles": round(h[3], 3) if h and h[3] is not None else None,
        "avg_base_fare_usd": round(h[4], 2) if h and h[4] is not None else None,
        "avg_driver_pay_usd": round(h[5], 2) if h and h[5] is not None else None,
        "driver_pay_share_of_fare": round(h[6], 4) if h and h[6] is not None else None,
        "avg_wait_minutes": round(h[7], 2) if h and h[7] is not None else None,
        "stream_reconciled": (data["stream_run"] or {}).get("reconciliation", {}).get("ok"),
        "verification": {m: v["summary"] for m, v in data["verification"].items() if v},
        "dq_errors": sum(
            1
            for doc in data["dq"].values()
            if doc
            for r in doc["results"]
            if r["status"] == "fail" and r["severity"] == "error"
        ),
    }
    (out / "summary.json").write_text(json.dumps(summary, indent=2, default=str), "utf-8")
    (out / ".nojekyll").write_text("", "utf-8")
    return summary
