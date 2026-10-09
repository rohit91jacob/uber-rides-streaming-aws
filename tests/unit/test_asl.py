from __future__ import annotations

import json

import boto3
import pytest

from uber_rides.orchestration import asl
from uber_rides.streaming.aws import aws_context


def _fake(results: dict[str, dict], calls: list[str], *, fail: str | None = None):
    def invoke(payload: dict) -> dict:
        key = payload["step"] if payload["step"] != "dq" else f"dq:{payload['layer']}"
        calls.append(key)
        if key == fail:
            raise RuntimeError(f"{key} exploded")
        return results.get(key, {})

    return invoke


BASE = {
    "resolve_month": {"month": "2026-06", "is_new": True},
    "ingest": {"trips_changed": True},
    "load": {"trips_written": 10, "quarantine_written": 0},
    "load_events": {"rows_loaded": 0},
    "transform": {"ok": True},
    "dq:landing": {"errors": 0},
    "dq:lake": {"errors": 0},
    "dq:stream": {"errors": 0},
    "dq:marts": {"errors": 0},
    "replay": {"reconciled": True},
}


def test_every_transition_targets_an_existing_state():
    definition = asl.load_definition()
    names = set(definition["States"])
    for state in definition["States"].values():
        targets = [state.get("Next"), state.get("Default")]
        targets += [c["Next"] for c in state.get("Choices", [])]
        targets += [c["Next"] for c in state.get("Catch", [])]
        assert {t for t in targets if t} <= names


def test_new_month_runs_every_step_in_order():
    calls: list[str] = []
    ex = asl.execute(asl.load_definition(), {"month": "2026-06"}, _fake(BASE, calls))
    assert ex.status == "SUCCEEDED"
    assert calls == [
        "resolve_month",
        "ingest",
        "dq:landing",
        "load",
        "dq:lake",
        "load_events",
        "dq:stream",
        "transform",
        "dq:marts",
    ]
    assert ex.output["load"]["trips_written"] == 10


def test_already_processed_month_skips_ingest_and_load():
    calls: list[str] = []
    results = {**BASE, "resolve_month": {"month": "2026-06", "is_new": False}}
    ex = asl.execute(asl.load_definition(), {}, _fake(results, calls))
    assert ex.status == "SUCCEEDED"
    assert "ingest" not in calls and "load" not in calls


def test_replay_branch_runs_only_when_requested():
    calls: list[str] = []
    ex = asl.execute(asl.load_definition(), {"replay": {"start": "a", "end": "b"}}, _fake(BASE, calls))
    assert ex.status == "SUCCEEDED" and "replay" in calls


def test_quality_gate_failure_stops_before_transform():
    calls: list[str] = []
    results = {**BASE, "dq:lake": {"errors": 2}}
    ex = asl.execute(asl.load_definition(), {}, _fake(results, calls))
    assert ex.status == "FAILED" and ex.error == "DataQualityGateFailed"
    assert "transform" not in calls


def test_task_exception_is_retried_then_caught():
    calls: list[str] = []
    ex = asl.execute(asl.load_definition(), {}, _fake(BASE, calls, fail="ingest"))
    assert ex.status == "FAILED" and ex.error == "BatchStepFailed"
    assert calls.count("ingest") == 3  # 1 attempt + 2 retries (States.TaskFailed)
    assert "load" not in calls


def test_definition_is_accepted_by_step_functions_api(settings):
    """moto validates the definition shape and role ARN format when creating the machine."""
    with aws_context(settings):
        sfn = boto3.client("stepfunctions", region_name="us-east-1")
        arn = sfn.create_state_machine(
            name="uber-rides-daily-batch",
            definition=json.dumps(
                asl.load_definition("arn:aws:lambda:us-east-1:123456789012:function:batch")
            ),
            roleArn="arn:aws:iam::123456789012:role/sfn",
        )["stateMachineArn"]
        assert sfn.describe_state_machine(stateMachineArn=arn)["status"] == "ACTIVE"


@pytest.mark.parametrize(
    ("path", "expected"),
    [("$", {"a": {"b": 1}}), ("$.a", {"b": 1}), ("$.a.b", 1)],
)
def test_get_path(path, expected):
    assert asl.get_path({"a": {"b": 1}}, path) == expected


def test_set_path_and_resolve():
    assert asl.set_path({"x": 1}, "$.y.z", 2) == {"x": 1, "y": {"z": 2}}
    assert asl.resolve({"m.$": "$.month", "k": "v"}, {"month": "2026-06"}) == {"m": "2026-06", "k": "v"}
    with pytest.raises(asl.TaskError):
        asl.resolve({"m.$": "$.nope"}, {})
