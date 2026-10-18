"""Contract test, consumer side: Notification must accept the agreed order.placed message."""
import json
import pathlib

import jsonschema
from fastapi.testclient import TestClient

from app.core import fetch
from app.main import app, handle_event

CONTRACTS = pathlib.Path(__file__).parents[3] / "contracts"
SCHEMA = json.loads((CONTRACTS / "order-placed.v1.schema.json").read_text())
EXAMPLE = json.loads((CONTRACTS / "order-placed.v1.example.json").read_text())


def test_example_is_valid_and_consumed_once():
    jsonschema.validate(EXAMPLE, SCHEMA)
    with TestClient(app):  # initialises the database
        handle_event("order.placed", EXAMPLE, "msg-1")
        handle_event("order.placed", EXAMPLE, "msg-1")  # redelivery of the same message
    rows = fetch("SELECT kind FROM notifications WHERE order_id=?", (EXAMPLE["order_id"],))
    assert [r["kind"] for r in rows] == ["order_received"]
