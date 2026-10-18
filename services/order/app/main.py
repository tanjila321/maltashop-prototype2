"""Order service: accepts orders (REST), owns order data, orchestrates nothing.

It publishes order.placed and reacts to the outcome events of Inventory and
Payment (choreography saga), moving the order to CONFIRMED or CANCELLED.
"""
import uuid

from fastapi import Header, HTTPException, Response
from pydantic import BaseModel, Field

from .core import claim, create_app, emit, fetch, log, now, tx

SCHEMA = """
CREATE TABLE IF NOT EXISTS orders (
    id TEXT PRIMARY KEY, customer_id TEXT NOT NULL, status TEXT NOT NULL,
    total_cents INTEGER NOT NULL, currency TEXT NOT NULL, reason TEXT,
    idempotency_key TEXT NOT NULL UNIQUE, created_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS order_items (
    order_id TEXT NOT NULL, sku TEXT NOT NULL, quantity INTEGER NOT NULL, unit_price_cents INTEGER NOT NULL);
"""


class Item(BaseModel):
    sku: str = Field(min_length=1)
    quantity: int = Field(gt=0, le=100)
    unit_price_cents: int = Field(gt=0)


class OrderRequest(BaseModel):
    customer_id: str = Field(min_length=1)
    items: list[Item] = Field(min_length=1)


def handle_event(routing_key: str, body: dict, message_id: str) -> None:
    """Saga step: apply the outcome of Inventory or Payment to the order, once only."""
    order_id = body["order_id"]
    with tx() as c:
        if not claim(c, message_id):  # inbox check: redelivered messages change nothing
            log("duplicate_ignored", message_id=message_id)
            return
        order = c.execute("SELECT customer_id FROM orders WHERE id=?", (order_id,)).fetchone()
        if order is None:
            raise ValueError(f"unknown order {order_id}")
        if routing_key == "payment.succeeded":
            changed = c.execute("UPDATE orders SET status='CONFIRMED' WHERE id=? AND status='PENDING'", (order_id,))
            if changed.rowcount:
                emit(c, "order.confirmed", {"order_id": order_id, "customer_id": order["customer_id"]})
        else:  # inventory.rejected or payment.failed: the saga ends in cancellation
            reason = body.get("reason", routing_key)
            changed = c.execute(
                "UPDATE orders SET status='CANCELLED', reason=? WHERE id=? AND status='PENDING'", (reason, order_id))
            if changed.rowcount:
                emit(c, "order.cancelled",
                     {"order_id": order_id, "customer_id": order["customer_id"], "reason": reason})
    log("event_handled", routing_key=routing_key, order_id=order_id)


app = create_app("Order service", SCHEMA, "order-service.q",
                 ["inventory.rejected", "payment.succeeded", "payment.failed"], handle_event)


def _view(order_id: str) -> dict:
    rows = fetch("SELECT * FROM orders WHERE id=?", (order_id,))
    if not rows:
        raise HTTPException(404, "order not found")
    o = rows[0]
    items = [dict(i) for i in fetch(
        "SELECT sku, quantity, unit_price_cents FROM order_items WHERE order_id=?", (order_id,))]
    return {"order_id": o["id"], "customer_id": o["customer_id"], "status": o["status"], "reason": o["reason"],
            "total_cents": o["total_cents"], "currency": o["currency"], "items": items}


@app.post("/orders", status_code=202)
def place_order(req: OrderRequest, response: Response, idempotency_key: str = Header(min_length=8)):
    """Place an order. Repeating the same Idempotency-Key returns the original order."""
    with tx() as c:
        existing = c.execute("SELECT id FROM orders WHERE idempotency_key=?", (idempotency_key,)).fetchone()
        if existing:
            response.status_code = 200
            log("order_replayed", order_id=existing["id"])
            order_id = existing["id"]
        else:
            order_id = str(uuid.uuid4())
            total = sum(i.quantity * i.unit_price_cents for i in req.items)
            c.execute("INSERT INTO orders VALUES (?,?,?,?,?,?,?,?)",
                      (order_id, req.customer_id, "PENDING", total, "EUR", None, idempotency_key, now()))
            for i in req.items:
                c.execute("INSERT INTO order_items VALUES (?,?,?,?)", (order_id, i.sku, i.quantity, i.unit_price_cents))
            emit(c, "order.placed", {
                "order_id": order_id, "customer_id": req.customer_id,
                "items": [i.model_dump() for i in req.items],
                "total_cents": total, "currency": "EUR", "placed_at": now()})
            log("order_placed", order_id=order_id, total_cents=total)
    return _view(order_id)


@app.get("/orders/{order_id}")
def get_order(order_id: str):
    return _view(order_id)
