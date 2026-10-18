"""End-to-end smoke test: place an order, wait for the saga, check the outcome.

Usage: python scripts/smoke_test.py [order_base_url] [notification_base_url]
"""
import json
import sys
import time
import urllib.request
import uuid

ORDER = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:8000"
NOTIFY = sys.argv[2] if len(sys.argv) > 2 else "http://localhost:8003"


def call(method, url, body=None, headers=None):
    req = urllib.request.Request(url, method=method, data=json.dumps(body).encode() if body else None,
                                 headers={"Content-Type": "application/json", **(headers or {})})
    with urllib.request.urlopen(req, timeout=10) as r:
        return r.status, dict(r.headers), json.loads(r.read())


def place(items, cid):
    return call("POST", f"{ORDER}/orders", {"customer_id": "cust-001", "items": items},
                {"Idempotency-Key": str(uuid.uuid4()), "X-Correlation-ID": cid})


def wait_final(order_id):
    for _ in range(40):
        _, _, order = call("GET", f"{ORDER}/orders/{order_id}")
        if order["status"] != "PENDING":
            return order
        time.sleep(0.5)
    raise SystemExit("FAIL: order stayed PENDING")


ok = place([{"sku": "SKU-KETTLE", "quantity": 2, "unit_price_cents": 3499}], "smoke-" + uuid.uuid4().hex[:8])
final = wait_final(ok[2]["order_id"])
assert final["status"] == "CONFIRMED", final
print("PASS happy path ->", final["status"])
bad = place([{"sku": "SKU-TV-55", "quantity": 1, "unit_price_cents": 600000}], "smoke-" + uuid.uuid4().hex[:8])
final = wait_final(bad[2]["order_id"])
assert final["status"] == "CANCELLED", final
print("PASS payment declined ->", final["status"], "-", final["reason"])
time.sleep(1)
kinds = {n["kind"] for n in call("GET", f"{NOTIFY}/notifications")[2]}
assert {"order_received", "order_confirmed", "order_cancelled"} <= kinds, kinds
print("PASS notifications ->", sorted(kinds))
