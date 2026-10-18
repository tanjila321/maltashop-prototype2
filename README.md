# MaltaShop "place an order" prototype

## What this is
A small event-driven prototype of the MaltaShop checkout flow, built for Unit 5 (Service-Oriented Architecture and Microservices Fundamentals). Four Python (FastAPI) services each own a private SQLite database and cooperate only through REST and RabbitMQ events:

| Service | Owns | Role |
|---|---|---|
| order | orders, order items | Accepts `POST /orders`, publishes `order.placed`, finishes the saga |
| inventory | stock, reservations | Reserves stock; releases it if payment fails (compensation) |
| payment | payments | Simulated card provider; declines orders above the configured limit |
| notification | notifications | Consumes order events and "sends" customer messages |

The order saga uses choreography: `order.placed` then `inventory.reserved` then `payment.succeeded` then `order.confirmed`. Failures publish `inventory.rejected` or `payment.failed`, ending in `order.cancelled`. Each service uses a transactional outbox (reliable publishing) and inbox (duplicate-safe consuming).

## Run it
Requirements: Docker with the Compose plugin.

    docker compose up --build

Then, in another terminal:

    python3 scripts/smoke_test.py            # happy path, declined payment, notifications
    curl -X POST localhost:8000/orders -H "Content-Type: application/json" \
         -H "Idempotency-Key: demo-key-0001" -H "X-Correlation-ID: demo-001" \
         -d '{"customer_id":"cust-001","items":[{"sku":"SKU-KETTLE","quantity":1,"unit_price_cents":3499}]}'

Ports: order 8000, inventory 8001, payment 8002, notification 8003, RabbitMQ management UI 15672. Seeded SKUs: `SKU-TV-55` (5), `SKU-KETTLE` (20), `SKU-PHONE` (10). An order above 5,000 EUR is declined to demonstrate the compensating action.

## Tests
    cd services/order && pip install -r requirements-dev.txt && ruff check . && pytest
    cd services/notification && pip install -r requirements-dev.txt && pytest

The shared `order.placed` contract lives in `contracts/`. Order tests that its event matches the schema; Notification tests that it consumes the agreed example.

## Environment variables
| Variable | Default | Used by | Meaning |
|---|---|---|---|
| `SERVICE_NAME` | service | all | Appears in every log line |
| `AMQP_URL` | built from `RABBITMQ_PASSWORD` in Compose | all | RabbitMQ connection string |
| `DB_PATH` | `/data/<SERVICE_NAME>.db` | all | Location of the service's private database |
| `LOG_LEVEL` | INFO | all | Log verbosity |
| `AMQP_DISABLED` | unset | tests | Set to `1` to run without a broker |
| `PAYMENT_DECLINE_ABOVE_CENTS` | 500000 | payment | Simulated provider decline threshold |
| `RABBITMQ_PASSWORD` | maltashop-dev | Compose | Broker password (development only) |

## Known limits
SQLite stands in for PostgreSQL. Secrets are plain environment variables. There is one broker node, no tracing, and no retry back-off beyond the dead-letter queue. See Section 6.3 of the report.
