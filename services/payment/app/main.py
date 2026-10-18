"""Payment service: owns payment records. The external provider is simulated.

Orders above PAYMENT_DECLINE_ABOVE_CENTS are declined so the compensation path
can be demonstrated. In production the provider call would carry the order ID
as the provider's own idempotency key.
"""
import os
import uuid

from fastapi import HTTPException

from .core import claim, create_app, emit, fetch, log, now, tx

DECLINE_ABOVE = int(os.environ.get("PAYMENT_DECLINE_ABOVE_CENTS", "500000"))

SCHEMA = """
CREATE TABLE IF NOT EXISTS payments (
    order_id TEXT PRIMARY KEY, status TEXT NOT NULL, amount_cents INTEGER NOT NULL,
    provider_ref TEXT, reason TEXT, created_at TEXT NOT NULL);
"""


def handle_event(routing_key: str, body: dict, message_id: str) -> None:
    order_id, amount = body["order_id"], body["total_cents"]
    with tx() as c:
        if not claim(c, message_id) or c.execute("SELECT 1 FROM payments WHERE order_id=?", (order_id,)).fetchone():
            log("duplicate_ignored", message_id=message_id)
            return
        if amount > DECLINE_ABOVE:
            reason = "card declined by provider (simulated limit)"
            c.execute("INSERT INTO payments VALUES (?,?,?,?,?,?)", (order_id, "FAILED", amount, None, reason, now()))
            emit(c, "payment.failed", {"order_id": order_id, "reason": reason})
            outcome = "payment_failed"
        else:
            ref = "psp_" + uuid.uuid4().hex[:12]
            c.execute("INSERT INTO payments VALUES (?,?,?,?,?,?)", (order_id, "SUCCEEDED", amount, ref, None, now()))
            emit(c, "payment.succeeded", {"order_id": order_id, "provider_ref": ref, "amount_cents": amount})
            outcome = "payment_succeeded"
    log(outcome, order_id=order_id, amount_cents=amount)


app = create_app("Payment service", SCHEMA, "payment-service.q", ["inventory.reserved"], handle_event)


@app.get("/payments/{order_id}")
def get_payment(order_id: str):
    rows = fetch("SELECT * FROM payments WHERE order_id=?", (order_id,))
    if not rows:
        raise HTTPException(404, "payment not found")
    return dict(rows[0])
