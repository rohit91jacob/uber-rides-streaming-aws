"""A small local interpreter for the daily-batch Amazon States Language definition.

It runs *the same JSON* Terraform deploys to Step Functions, so the control flow (choices,
quality-gate stops, retries, catches) is exercised locally and in CI without an AWS account.
Supported: Task (``arn:aws:states:::lambda:invoke`` only), Choice, Pass, Succeed, Fail;
``Parameters`` / ``ResultSelector`` with ``.$`` JSONPath keys, ``ResultPath``, ``Retry``
(attempt counting; local runs do not sleep) and ``Catch``. JSONPath support is limited to
``$`` and dotted field access, which is all the definition uses.
"""

from __future__ import annotations

import copy
import json
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ASL_PATH = Path(__file__).with_name("daily_batch.asl.json")
LAMBDA_INVOKE = "arn:aws:states:::lambda:invoke"
MAX_TRANSITIONS = 200


class ExecutionFailed(RuntimeError):
    def __init__(self, error: str, cause: str, history: list[dict[str, Any]]) -> None:
        super().__init__(f"{error}: {cause}")
        self.error = error
        self.cause = cause
        self.history = history


class TaskError(RuntimeError):
    """Raised by a task; ``error`` is matched against Retry/Catch ``ErrorEquals``."""

    def __init__(self, error: str, cause: str) -> None:
        super().__init__(f"{error}: {cause}")
        self.error = error
        self.cause = cause


_MISSING = object()


def get_path(data: Any, path: str) -> Any:
    if path == "$":
        return data
    if not path.startswith("$."):
        raise ValueError(f"unsupported JSONPath {path!r}")
    cur = data
    for part in path[2:].split("."):
        if not isinstance(cur, dict) or part not in cur:
            return _MISSING
        cur = cur[part]
    return cur


def set_path(data: Any, path: str | None, value: Any) -> Any:
    if path is None:
        return data
    if path == "$":
        return value
    out = copy.deepcopy(data) if isinstance(data, dict) else {}
    cur = out
    parts = path[2:].split(".")
    for part in parts[:-1]:
        cur = cur.setdefault(part, {})
    cur[parts[-1]] = value
    return out


def resolve(template: Any, data: Any) -> Any:
    """Apply a Parameters/ResultSelector template: keys ending in ``.$`` are JSONPaths."""
    if isinstance(template, dict):
        out = {}
        for k, v in template.items():
            if k.endswith(".$"):
                value = get_path(data, v)
                if value is _MISSING:
                    raise TaskError("States.Runtime", f"JSONPath {v!r} not found in input")
                out[k[:-2]] = value
            else:
                out[k] = resolve(v, data)
        return out
    if isinstance(template, list):
        return [resolve(v, data) for v in template]
    return template


def _choice_matches(rule: dict[str, Any], data: Any) -> bool:
    if "And" in rule:
        return all(_choice_matches(r, data) for r in rule["And"])
    if "Or" in rule:
        return any(_choice_matches(r, data) for r in rule["Or"])
    if "Not" in rule:
        return not _choice_matches(rule["Not"], data)
    value = get_path(data, rule["Variable"])
    if "IsPresent" in rule:
        return (value is not _MISSING) == rule["IsPresent"]
    if value is _MISSING:
        return False
    ops: dict[str, Callable[[Any, Any], bool]] = {
        "BooleanEquals": lambda a, b: a is b,
        "StringEquals": lambda a, b: a == b,
        "NumericEquals": lambda a, b: a == b,
        "NumericGreaterThan": lambda a, b: a > b,
        "NumericGreaterThanEquals": lambda a, b: a >= b,
        "NumericLessThan": lambda a, b: a < b,
    }
    for op, fn in ops.items():
        if op in rule:
            return fn(value, rule[op])
    raise ValueError(f"unsupported Choice rule {rule}")


def _matches(error: str, equals: list[str]) -> bool:
    return (
        "States.ALL" in equals
        or error in equals
        or ("States.TaskFailed" in equals and not error.startswith("States."))
    )


@dataclass
class Execution:
    status: str = "RUNNING"
    output: Any = None
    history: list[dict[str, Any]] = field(default_factory=list)
    error: str | None = None
    cause: str | None = None


def load_definition(
    function_arn: str = "local:uber-rides-batch-step", path: Path = ASL_PATH
) -> dict[str, Any]:
    text = path.read_text("utf-8").replace("${BatchStepFunctionArn}", function_arn)
    return json.loads(text)


def execute(
    definition: dict[str, Any],
    payload: dict[str, Any],
    invoke: Callable[[dict[str, Any]], dict[str, Any]],
) -> Execution:
    """Run ``definition`` with ``payload``; ``invoke`` plays the Lambda (input -> Payload)."""
    states = definition["States"]
    name = definition["StartAt"]
    data: Any = payload
    ex = Execution()
    for _ in range(MAX_TRANSITIONS):
        state = states[name]
        started = datetime.now(UTC)
        entry: dict[str, Any] = {"state": name, "type": state["Type"], "started_at": started.isoformat()}
        ex.history.append(entry)
        kind = state["Type"]
        if kind == "Succeed":
            ex.status, ex.output = "SUCCEEDED", data
            return ex
        if kind == "Fail":
            ex.status, ex.error, ex.cause = "FAILED", state.get("Error"), state.get("Cause")
            entry["error"] = ex.error
            return ex
        if kind == "Pass":
            result = (
                resolve(state["Parameters"], data) if "Parameters" in state else state.get("Result", data)
            )
            data = set_path(data, state.get("ResultPath", "$"), result)
            name = state["Next"]
            continue
        if kind == "Choice":
            nxt = next(
                (r["Next"] for r in state["Choices"] if _choice_matches(r, data)), state.get("Default")
            )
            if nxt is None:
                ex.status, ex.error, ex.cause = "FAILED", "States.NoChoiceMatched", name
                return ex
            entry["next"] = nxt
            name = nxt
            continue
        if kind != "Task":
            raise ValueError(f"state type {kind} is not supported locally")
        if state["Resource"] != LAMBDA_INVOKE:
            raise ValueError(f"only {LAMBDA_INVOKE} tasks are supported locally, got {state['Resource']}")
        attempts: dict[int, int] = {}
        while True:
            try:
                params = resolve(state.get("Parameters", {}), data)
                try:
                    response = {"Payload": invoke(params["Payload"]), "StatusCode": 200}
                except TaskError:
                    raise
                except Exception as exc:
                    raise TaskError(type(exc).__name__, str(exc)[:1000]) from exc
                result = resolve(state["ResultSelector"], response) if "ResultSelector" in state else response
                data = set_path(data, state.get("ResultPath", "$"), result)
                entry["result"] = result
                break
            except TaskError as err:
                entry.setdefault("errors", []).append(f"{err.error}: {err.cause[:300]}")
                retry = next(
                    (
                        i
                        for i, r in enumerate(state.get("Retry", []))
                        if _matches(err.error, r["ErrorEquals"])
                    ),
                    None,
                )
                if retry is not None and attempts.get(retry, 0) < state["Retry"][retry].get("MaxAttempts", 3):
                    attempts[retry] = attempts.get(retry, 0) + 1
                    continue
                catch = next(
                    (c for c in state.get("Catch", []) if _matches(err.error, c["ErrorEquals"])), None
                )
                if catch is None:
                    ex.status, ex.error, ex.cause = "FAILED", err.error, err.cause
                    return ex
                data = set_path(data, catch.get("ResultPath", "$"), {"Error": err.error, "Cause": err.cause})
                entry["caught"] = err.error
                name = catch["Next"]
                break
        if "caught" in entry:
            continue
        if state.get("End"):
            ex.status, ex.output = "SUCCEEDED", data
            return ex
        name = state["Next"]
    raise RuntimeError("state machine exceeded the transition limit")
