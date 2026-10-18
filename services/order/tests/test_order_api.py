"""Unit and API tests for the Order service (test pyramid: base and middle layers)."""
import pytest
from fastapi.testclient import TestClient

from app.core import fetch
from app.main import app

BODY = {"customer_id": "cust-001", "items": [{"sku": "SKU-KETTLE", "quantity": 2, "unit_price_cents": 3499}]}


@pytest.fixture(scope="module")
def client():
    with TestClient(app) as c:
        yield c


def test_place_order_returns_202_and_pending(client):
    r = client.post("/orders", json=BODY, headers={"Idempotency-Key": "key-0000001"})
    assert r.status_code == 202
    assert r.json()["status"] == "PENDING" and r.json()["total_cents"] == 6998


def test_same_idempotency_key_returns_same_order(client):
    h = {"Idempotency-Key": "key-0000002"}
    first = client.post("/orders", json=BODY, headers=h)
    second = client.post("/orders", json=BODY, headers=h)
    assert second.status_code == 200
    assert first.json()["order_id"] == second.json()["order_id"]


def test_invalid_quantity_rejected(client):
    bad = {"customer_id": "c", "items": [{"sku": "X", "quantity": 0, "unit_price_cents": 1}]}
    assert client.post("/orders", json=bad, headers={"Idempotency-Key": "key-0000003"}).status_code == 422


def test_correlation_id_is_echoed(client):
    r = client.post("/orders", json=BODY, headers={"Idempotency-Key": "key-0000004", "X-Correlation-ID": "abc-123"})
    assert r.headers["X-Correlation-ID"] == "abc-123"


def test_order_and_outbox_event_commit_together(client):
    r = client.post("/orders", json=BODY, headers={"Idempotency-Key": "key-0000005"})
    rows = fetch("SELECT routing_key, payload FROM outbox WHERE payload LIKE ?", (f"%{r.json()['order_id']}%",))
    assert len(rows) == 1 and rows[0]["routing_key"] == "order.placed"
