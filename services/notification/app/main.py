"""Notification service: sends customer messages (simulated). Owns its own store.

It is a pure consumer. If it crashes or leaks memory, checkout is unaffected:
events wait in its durable queue until it recovers.
"""
import uuid

from .core import claim, create_app, fetch, log, now, tx

KINDS = {"order.placed": "order_received", "order.confirmed": "order_confirmed", "order.cancelled": "order_cancelled"}

SCHEMA = """
CREATE TABLE IF NOT EXISTS notifications (
    id TEXT PRIMARY KEY, order_id TEXT NOT NULL, customer_id TEXT NOT NULL, kind TEXT NOT NULL,
    created_at TEXT NOT NULL, UNIQUE (order_id, kind));
"""


def handle_event(routing_key: str, body: dict, message_id: str) -> None:
    kind = KINDS[routing_key]
    with tx() as c:
        if not claim(c, message_id):
            log("duplicate_ignored", message_id=message_id)
            return
        c.execute("INSERT OR IGNORE INTO notifications VALUES (?,?,?,?,?)",
                  (str(uuid.uuid4()), body["order_id"], body["customer_id"], kind, now()))
    log("notification_sent", channel="email (simulated)", kind=kind, order_id=body["order_id"])


app = create_app("Notification service", SCHEMA, "notification-service.q", list(KINDS), handle_event)


@app.get("/notifications")
def notifications():
    rows = fetch("SELECT order_id, customer_id, kind, created_at FROM notifications ORDER BY created_at")
    return [dict(r) for r in rows]
