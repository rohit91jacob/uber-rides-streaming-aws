from __future__ import annotations

import json
from datetime import datetime

import pytest

from uber_rides.streaming import events
from uber_rides.streaming.events import Invalid


def _trip(**over):
    trip = {
        "trip_id": "HV0003-2026-06-000000001",
        "hvfhs_license_num": "HV0003",
        "request_datetime": datetime(2026, 6, 12, 18, 0, 5),
        "on_scene_datetime": datetime(2026, 6, 12, 18, 4, 0),
        "pickup_datetime": datetime(2026, 6, 12, 18, 5, 0),
        "dropoff_datetime": datetime(2026, 6, 12, 18, 25, 0),
        "pu_location_id": 161,
        "do_location_id": 132,
        "trip_miles": 15.2,
        "trip_time_s": 1200,
        "base_passenger_fare": 61.5,
        "driver_pay": 48.1,
        "tips": 5.0,
        "cbd_congestion_fee": 1.5,
        "shared_request": False,
        "shared_match": False,
    }
    trip.update(over)
    return trip


def test_four_lifecycle_events_in_order_with_real_times():
    evs = events.events_for_trip(_trip())
    assert [e["event_type"] for e in evs] == list(events.EVENT_TYPES)
    assert [e["event_time"] for e in evs] == [
        "2026-06-12T18:00:05",
        "2026-06-12T18:04:00",
        "2026-06-12T18:05:00",
        "2026-06-12T18:25:00",
    ]
    assert evs[-1]["zone_id"] == 132 and evs[0]["zone_id"] == 161
    assert evs[-1]["attributes"]["driver_pay"] == 48.1


def test_missing_on_scene_skips_that_event():
    evs = events.events_for_trip(_trip(on_scene_datetime=None))
    assert "driver_on_scene" not in [e["event_type"] for e in evs]


def test_event_id_is_deterministic_and_type_specific():
    a = events.event_id("t1", "requested")
    assert a == events.event_id("t1", "requested")
    assert a != events.event_id("t1", "picked_up")
    assert len(a) == 32


def test_valid_event_round_trips():
    ev = events.events_for_trip(_trip())[3]
    assert events.validate(events.encode(ev)) == ev


@pytest.mark.parametrize(
    ("mutate", "reason"),
    [
        (lambda e: e.pop("trip_id"), "schema_violation"),
        (lambda e: e.update(zone_id=999), "zone_out_of_range"),
        (lambda e: e.update(event_type="teleported"), "schema_violation"),
        (lambda e: e.update(event_id="0" * 32), "event_id_mismatch"),
        (lambda e: e.update(schema_version=2), "unsupported_schema_version"),
        (lambda e: e.update(event_time="yesterday"), "schema_violation"),
        (lambda e: e["attributes"].update(trip_miles=-1), "schema_violation"),
        (lambda e: e["attributes"].update(driver_pay="lots"), "schema_violation"),
    ],
)
def test_invalid_events_are_classified(mutate, reason):
    ev = json.loads(json.dumps(events.events_for_trip(_trip())[3]))
    mutate(ev)
    result = events.validate(events.encode(ev))
    assert isinstance(result, Invalid)
    assert result.reason == reason


def test_malformed_json_is_rejected():
    result = events.validate(b'{"schema_version": 1, "trip')
    assert isinstance(result, Invalid) and result.reason == "malformed_json"
    assert isinstance(events.validate(b"[1, 2]"), Invalid)


@pytest.mark.parametrize(
    ("ts", "minutes", "expected"),
    [
        ("2026-06-12T18:14:59", 15, "2026-06-12T18:00:00"),
        ("2026-06-12T18:15:00", 15, "2026-06-12T18:15:00"),
        ("2026-06-12T23:59:59", 5, "2026-06-12T23:55:00"),
    ],
)
def test_window_start(ts, minutes, expected):
    assert events.window_start(ts, minutes) == expected


def test_sort_key_orders_by_time_then_trip_then_lifecycle():
    a = events.events_for_trip(_trip(trip_id="b"))
    b = events.events_for_trip(_trip(trip_id="a"))
    ordered = sorted(a + b, key=events.sort_key)
    assert ordered[0]["trip_id"] == "a" and ordered[0]["event_type"] == "requested"
    assert [e["event_type"] for e in ordered[-2:]] == ["dropped_off", "dropped_off"]
