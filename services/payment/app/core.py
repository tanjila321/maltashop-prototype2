"""Shared infrastructure for MaltaShop services.

JSON logging with a correlation ID, SQLite helpers (one private database per
service), a transactional outbox and inbox, and RabbitMQ publish/consume.
This file is intentionally copied into each service: services share no code at
runtime. In production it would become a versioned internal library.
"""
import contextvars
import json
import logging
import os
import sqlite3
import sys
import threading
import time
import uuid
from contextlib import asynccontextmanager, contextmanager
from datetime import datetime, timezone

import pika
from fastapi import FastAPI, HTTPException, Request

SERVICE = os.environ.get("SERVICE_NAME", "service")
DB_PATH = os.environ.get("DB_PATH", f"/data/{SERVICE}.db")
AMQP_URL = os.environ.get("AMQP_URL", "amqp://guest:guest@rabbitmq:5672/%2F")
EXCHANGE = "maltashop.events"
DLX = "maltashop.dlx"

correlation_id: contextvars.ContextVar[str] = contextvars.ContextVar("correlation_id", default="-")


def now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


# ---------------------------------------------------------------- logging
class JsonFormatter(logging.Formatter):
    """Mandatory log schema: timestamp, level, service, correlation_id, event."""

    def format(self, record: logging.LogRecord) -> str:
        entry = {
            "timestamp": now(),
            "level": record.levelname,
            "service": SERVICE,
            "correlation_id": correlation_id.get(),
            "event": record.getMessage(),
        }
        entry.update(getattr(record, "fields", {}))
        return json.dumps(entry, separators=(",", ":"))


_logger = logging.getLogger("maltashop")
_handler = logging.StreamHandler(sys.stdout)
_handler.setFormatter(JsonFormatter())
_logger.addHandler(_handler)
_logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))
_logger.propagate = False
logging.getLogger("pika").setLevel(logging.WARNING)


def log(event: str, level: int = logging.INFO, **fields) -> None:
    _logger.log(level, event, extra={"fields": fields})


# --------------------------------------------------------------- database
BASE_SCHEMA = """
CREATE TABLE IF NOT EXISTS outbox (
    id TEXT PRIMARY KEY, routing_key TEXT NOT NULL, payload TEXT NOT NULL,
    correlation_id TEXT NOT NULL, created_at TEXT NOT NULL, sent_at TEXT);
CREATE TABLE IF NOT EXISTS inbox (
    message_id TEXT PRIMARY KEY, processed_at TEXT NOT NULL);
"""


def init_db(schema: str) -> None:
    os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.executescript(BASE_SCHEMA + schema)
    conn.close()


@contextmanager
def tx():
    """One local ACID transaction: business state and outbox/inbox rows commit together."""
    conn = sqlite3.connect(DB_PATH, timeout=15, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield conn
        conn.execute("COMMIT")
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    finally:
        conn.close()


def fetch(sql: str, params: tuple = ()) -> list:
    conn = sqlite3.connect(DB_PATH, timeout=15)
    conn.row_factory = sqlite3.Row
    try:
        return conn.execute(sql, params).fetchall()
    finally:
        conn.close()


def emit(conn, routing_key: str, payload: dict) -> None:
    """Outbox: record the event in the same transaction as the state change."""
    conn.execute(
        "INSERT INTO outbox(id, routing_key, payload, correlation_id, created_at) VALUES (?,?,?,?,?)",
        (str(uuid.uuid4()), routing_key, json.dumps(payload), correlation_id.get(), now()),
    )


def claim(conn, message_id: str) -> bool:
    """Inbox: True the first time a message ID is seen, False for any redelivery."""
    cur = conn.execute("INSERT OR IGNORE INTO inbox(message_id, processed_at) VALUES (?,?)", (message_id, now()))
    return cur.rowcount == 1


# ----------------------------------------------------------------- broker
def connect(retries: int = 60) -> pika.BlockingConnection:
    for attempt in range(1, retries + 1):
        try:
            return pika.BlockingConnection(pika.URLParameters(AMQP_URL))
        except pika.exceptions.AMQPError:
            log("broker_unavailable", logging.WARNING, attempt=attempt)
            time.sleep(2)
    raise ConnectionError("RabbitMQ not reachable")


_local = threading.local()


def publish(routing_key: str, body: str, message_id: str, corr_id: str) -> None:
    channel = getattr(_local, "channel", None)
    if channel is None or channel.is_closed:
        conn = connect()
        channel = conn.channel()
        channel.confirm_delivery()  # publisher confirms: broker acknowledges durable receipt
        channel.exchange_declare(EXCHANGE, "topic", durable=True)
        _local.conn, _local.channel = conn, channel
    props = pika.BasicProperties(
        delivery_mode=2, content_type="application/json", message_id=message_id,
        correlation_id=corr_id, type=routing_key,
    )
    try:
        channel.basic_publish(EXCHANGE, routing_key, body.encode(), props)
    except Exception:
        _local.channel = None
        raise


class OutboxRelay(threading.Thread):
    """Publishes committed outbox rows to the broker (at-least-once delivery)."""

    def __init__(self):
        super().__init__(daemon=True, name="outbox-relay")

    def run(self):
        while True:
            try:
                rows = fetch("SELECT * FROM outbox WHERE sent_at IS NULL ORDER BY created_at LIMIT 50")
                for row in rows:
                    token = correlation_id.set(row["correlation_id"])
                    try:
                        publish(row["routing_key"], row["payload"], row["id"], row["correlation_id"])
                        with tx() as c:
                            c.execute("UPDATE outbox SET sent_at=? WHERE id=?", (now(), row["id"]))
                        log("event_published", routing_key=row["routing_key"], message_id=row["id"])
                    finally:
                        correlation_id.reset(token)
                if not rows:
                    time.sleep(0.3)
            except Exception as exc:  # broker down: rows stay in the outbox and are retried
                log("outbox_relay_error", logging.ERROR, error=str(exc))
                time.sleep(2)


class Consumer(threading.Thread):
    """Durable queue with manual acks and a dead-letter queue for poison messages."""

    def __init__(self, queue: str, bindings: list, handler):
        super().__init__(daemon=True, name="consumer")
        self.queue, self.bindings, self.handler = queue, bindings, handler
        self.ready = threading.Event()

    def run(self):
        while True:
            try:
                conn = connect()
                ch = conn.channel()
                ch.exchange_declare(EXCHANGE, "topic", durable=True)
                ch.exchange_declare(DLX, "direct", durable=True)
                ch.queue_declare(f"{self.queue}.dlq", durable=True)
                ch.queue_bind(f"{self.queue}.dlq", DLX, self.queue)
                ch.queue_declare(self.queue, durable=True, arguments={
                    "x-dead-letter-exchange": DLX, "x-dead-letter-routing-key": self.queue})
                for key in self.bindings:
                    ch.queue_bind(self.queue, EXCHANGE, key)
                ch.basic_qos(prefetch_count=10)
                ch.basic_consume(self.queue, self._on_message)
                self.ready.set()
                log("consumer_started", queue=self.queue, bindings=self.bindings)
                ch.start_consuming()
            except Exception as exc:
                self.ready.clear()
                log("consumer_error", logging.ERROR, error=str(exc))
                time.sleep(2)

    def _on_message(self, ch, method, props, body):
        token = correlation_id.set(props.correlation_id or "-")
        try:
            log("event_received", routing_key=method.routing_key, message_id=props.message_id)
            self.handler(method.routing_key, json.loads(body), props.message_id)
            ch.basic_ack(method.delivery_tag)
        except Exception as exc:
            log("handler_failed", logging.ERROR, routing_key=method.routing_key, error=str(exc))
            ch.basic_nack(method.delivery_tag, requeue=False)  # to the dead-letter queue
        finally:
            correlation_id.reset(token)


# -------------------------------------------------------------------- app
def create_app(title: str, schema: str, queue: str, bindings: list, handler) -> FastAPI:
    enabled = os.environ.get("AMQP_DISABLED") != "1"
    consumer = Consumer(queue, bindings, handler) if enabled else None

    @asynccontextmanager
    async def lifespan(_app):
        init_db(schema)
        if enabled:
            OutboxRelay().start()
            consumer.start()
        log("service_started", queue=queue)
        yield

    app = FastAPI(title=title, lifespan=lifespan)

    @app.middleware("http")
    async def correlation_middleware(request: Request, call_next):
        cid = request.headers.get("X-Correlation-ID") or str(uuid.uuid4())
        token = correlation_id.set(cid)
        try:
            response = await call_next(request)
            response.headers["X-Correlation-ID"] = cid
            if not request.url.path.startswith("/health"):
                log("http_request", method=request.method, path=request.url.path, status=response.status_code)
            return response
        finally:
            correlation_id.reset(token)

    @app.get("/health/live")
    def live():
        return {"status": "alive"}

    @app.get("/health/ready")
    def ready():
        try:
            fetch("SELECT 1")
        except Exception:
            raise HTTPException(503, "database unavailable")
        if enabled and not consumer.ready.is_set():
            raise HTTPException(503, "broker unavailable")
        return {"status": "ready"}

    return app
