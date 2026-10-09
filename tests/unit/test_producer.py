from __future__ import annotations

from collections import Counter

from uber_rides.streaming import events, producer
from uber_rides.streaming.events import Invalid


def _events(n: int) -> list[dict]:
    out = []
    for i in range(n):
        trip_id = f"HV0003-2026-06-{i:09d}"
        etype = events.EVENT_TYPES[i % 4]
        out.append(
            {
                "schema_version": 1,
                "event_id": events.event_id(trip_id, etype),
                "event_type": etype,
                "event_time": f"2026-06-12T18:{i % 60:02d}:00",
                "trip_id": trip_id,
                "zone_id": 1 + i % 263,
                "pu_location_id": 1 + i % 263,
                "do_location_id": 1 + i % 263,
                "attributes": {"trip_miles": 1.0, "trip_time_s": 60} if etype == "dropped_off" else {},
            }
        )
    return out


def test_drills_are_deterministic_and_counted():
    evs = _events(100)
    stats_a, stats_b = producer.ReplayStats(), producer.ReplayStats()
    a = list(producer.with_drills(evs, duplicate_every=10, malformed_every=7, seed=3, stats=stats_a))
    b = list(producer.with_drills(evs, duplicate_every=10, malformed_every=7, seed=3, stats=stats_b))
    assert a == b
    assert stats_a.duplicates_injected == 10
    assert stats_a.malformed_injected == 14
    assert len(a) == 100 + 10 + 14
    assert sum(stats_a.malformed_by_kind.values()) == 14


def test_every_malformation_fails_validation():
    ev = _events(4)[3]
    for kind in producer.MALFORMATIONS:
        assert isinstance(events.validate(producer._malform(ev, kind)), Invalid), kind


def test_duplicates_are_byte_identical():
    evs = _events(4)
    sent = [p for _, p in producer.with_drills(evs, duplicate_every=2)]
    counts = Counter(sent)
    assert sorted(counts.values()) == [1, 1, 2, 2]


def test_sampling_is_stable():
    ids = [f"HV0003-2026-06-{i:09d}" for i in range(20_000)]
    first = [producer.in_sample(t, 0.1) for t in ids]
    assert first == [producer.in_sample(t, 0.1) for t in ids]
    assert 1500 < sum(first) < 2500
    assert all(producer.in_sample(t, 1.0) for t in ids[:10])
