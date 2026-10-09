"""Local-emulator performance shim for moto's DynamoDB ``TransactWriteItems``.

moto implements transactional rollback by deep-copying the *whole table* once per item in
the transaction (50 copies per consumer transaction here), which makes a replay of a few
thousand events take minutes. This shim evaluates every condition first and, only if all
of them pass, runs moto's implementation with the backup copy disabled (nothing can fail
afterwards, so nothing needs rolling back). If any condition fails it raises the same
``TransactionCanceledException`` with per-item ``CancellationReasons`` that moto and AWS
return. Semantics are unchanged; only the emulator gets faster.

Applied only in ``UBER_AWS_MODE=moto``. LocalStack and real DynamoDB are untouched.
"""

from __future__ import annotations

import contextlib
import types
from collections.abc import Iterator
from typing import Any

_PATCHED = False


@contextlib.contextmanager
def _no_deepcopy(module: types.ModuleType) -> Iterator[None]:
    original = module.copy
    module.copy = types.SimpleNamespace(deepcopy=lambda obj: obj)
    try:
        yield
    finally:
        module.copy = original


def install() -> None:
    global _PATCHED
    if _PATCHED:
        return
    from moto.dynamodb import models
    from moto.dynamodb.comparisons import get_filter_expression
    from moto.dynamodb.exceptions import TransactionCanceledException

    original = models.DynamoDBBackend.transact_write_items

    def transact_write_items(self: Any, transact_items: list[dict[str, Any]]) -> None:
        errors: list[tuple[Any, Any, Any]] = []
        failed = False
        for item in transact_items:
            op_type, op = next(iter(item.items()))
            condition = op.get("ConditionExpression")
            if condition is None or op_type not in {"Put", "Update", "Delete", "ConditionCheck"}:
                errors.append((None, None, None))
                continue
            table = self.get_table(op["TableName"])
            if op_type == "Put":
                attrs = op["Item"]
                key = {table.hash_key_attr: attrs[table.hash_key_attr]}
                if table.range_key_attr is not None:
                    key[table.range_key_attr] = attrs[table.range_key_attr]
            else:
                key = op["Key"]
            current = self.get_item(op["TableName"], key)
            expr = get_filter_expression(
                condition, op.get("ExpressionAttributeNames"), op.get("ExpressionAttributeValues")
            )
            if expr.expr(current):
                errors.append((None, None, None))
            else:
                failed = True
                errors.append(("ConditionalCheckFailed", "The conditional request failed", None))
        if failed:
            raise TransactionCanceledException(errors)
        with _no_deepcopy(models):
            return original(self, transact_items)

    models.DynamoDBBackend.transact_write_items = transact_write_items
    _PATCHED = True
