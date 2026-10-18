"""Inventory service: owns stock levels and reservations."""
from .core import claim, create_app, emit, fetch, log, tx

SCHEMA = """
CREATE TABLE IF NOT EXISTS stock (sku TEXT PRIMARY KEY, available INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS reservations (
    order_id TEXT NOT NULL, sku TEXT NOT NULL, quantity INTEGER NOT NULL, status TEXT NOT NULL,
    PRIMARY KEY (order_id, sku));
INSERT OR IGNORE INTO stock VALUES ('SKU-TV-55', 5), ('SKU-KETTLE', 20), ('SKU-PHONE', 10);
"""


def handle_event(routing_key: str, body: dict, message_id: str) -> None:
    order_id = body["order_id"]
    with tx() as c:
        if not claim(c, message_id):
            log("duplicate_ignored", message_id=message_id)
            return
        if routing_key == "order.placed":
            short = None
            for item in body["items"]:
                row = c.execute("SELECT available FROM stock WHERE sku=?", (item["sku"],)).fetchone()
                if row is None or row["available"] < item["quantity"]:
                    short = item["sku"]
                    break
            if short:
                emit(c, "inventory.rejected", {"order_id": order_id, "reason": f"insufficient stock for {short}"})
                outcome = "rejected"
            else:
                for item in body["items"]:
                    c.execute("UPDATE stock SET available=available-? WHERE sku=?", (item["quantity"], item["sku"]))
                    c.execute("INSERT INTO reservations VALUES (?,?,?,'RESERVED')",
                              (order_id, item["sku"], item["quantity"]))
                emit(c, "inventory.reserved", {
                    "order_id": order_id, "total_cents": body["total_cents"], "currency": body["currency"]})
                outcome = "reserved"
        else:  # payment.failed: compensating action releases the reserved stock
            for r in c.execute("SELECT sku, quantity FROM reservations WHERE order_id=? AND status='RESERVED'",
                               (order_id,)).fetchall():
                c.execute("UPDATE stock SET available=available+? WHERE sku=?", (r["quantity"], r["sku"]))
            c.execute("UPDATE reservations SET status='RELEASED' WHERE order_id=? AND status='RESERVED'", (order_id,))
            emit(c, "inventory.released", {"order_id": order_id})
            outcome = "released"
    log("inventory_" + outcome, order_id=order_id)


app = create_app("Inventory service", SCHEMA, "inventory-service.q", ["order.placed", "payment.failed"], handle_event)


@app.get("/stock")
def stock():
    return {r["sku"]: r["available"] for r in fetch("SELECT sku, available FROM stock ORDER BY sku")}
