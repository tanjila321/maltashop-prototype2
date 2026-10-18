"""Contract test, provider side: Order's order.placed event must satisfy the shared schema."""
import json
import pathlib

import jsonschema
from fastapi.testclient import TestClient

from app.core import fetch
from app.main import app

SCHEMA = json.loads((pathlib.Path(__file__).parents[3] / "contracts" / "order-placed.v1.schema.json").read_text())


def test_order_placed_event_matches_contract():
    body = {"customer_id": "cust-9", "items": [{"sku": "SKU-PHONE", "quantity": 1, "unit_price_cents": 59900}]}
    with TestClient(app) as client:
        r = client.post("/orders", json=body, headers={"Idempotency-Key": "contract-key-1"})
    row = fetch("SELECT payload FROM outbox WHERE payload LIKE ?", (f"%{r.json()['order_id']}%",))[0]
    jsonschema.validate(json.loads(row["payload"]), SCHEMA)
