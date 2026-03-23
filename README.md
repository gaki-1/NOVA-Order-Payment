# Order & Inventory Management Service

A Spring Boot service for managing items, inventory, and orders with strong stock consistency guarantees under concurrent load.

---

## Tech Stack

| Layer | Technology |
|---|---|
| Runtime | Java 17 |
| Framework | Spring Boot 4.0.4 |
| Persistence | PostgreSQL + Spring Data JPA |
| Schema migrations | Liquibase |
| Messaging | Apache Kafka |
| Distributed locking | Redis (Redisson 3.45.0) |
| API docs | SpringDoc OpenAPI (Swagger UI) |
| Build | Maven |

---

## Prerequisites

| Service | Default address |
|---|---|
| PostgreSQL | `localhost:5433` |
| Apache Kafka | `localhost:9092` |
| Redis | `localhost:6379` |

---

## Setup

### 1. Create the PostgreSQL database

```sql
CREATE DATABASE order_payment;
```

### 2. Configure `application.properties`

```properties
# DataSource
spring.datasource.url=jdbc:postgresql://localhost:5433/order_payment
spring.datasource.username=<your_username>
spring.datasource.password=<your_password>

# Kafka
spring.kafka.bootstrap-servers=localhost:9092

# Redis
redis.address=redis://127.0.0.1:6379
```

### 3. Create Kafka topics

```bash
kafka-topics.sh --create --topic order.updates    --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
kafka-topics.sh --create --topic inventory.updates --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

### 4. Run the application

```bash
./mvnw spring-boot:run
```

Liquibase will automatically apply all schema migrations on startup.

### 5. Swagger UI

```
http://localhost:8080/swagger-ui.html
```

---

## API Reference

### Inventory

| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/inventory/items` | Register a new item with initial stock |
| `GET` | `/inventory/items/{itemId}` | Get available stock for an item |
| `GET` | `/inventory/items?ids=...` | Bulk get available stock for multiple items |

#### Register item — request body
```json
{
  "name": "Milk",
  "description": "Fresh milk",
  "price": 2.50,
  "initialQuantity": 100,
  "expiryDate": "2026-12-31"
}
```

#### Register item — response
```json
{
  "itemId": "uuid",
  "name": "Milk",
  "price": 2.50,
  "availableQuantity": 100.0,
  "nearestExpiry": "2026-12-31"
}
```

---

### Orders

| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/orders` | Create a new order (status: PENDING) |
| `POST` | `/orders/{orderId}/place` | Place the order — validates and reserves stock |
| `POST` | `/orders/{orderId}/cancel` | Cancel the order — restores reserved stock if CONFIRMED |
| `GET` | `/orders/{orderId}` | Get order details |

#### Create order — request body
```json
{
  "items": [
    { "itemId": "uuid", "quantity": 5 },
    { "itemId": "uuid", "quantity": 2 }
  ]
}
```

#### Order response
```json
{
  "orderId": "uuid",
  "status": "CONFIRMED",
  "items": [
    { "itemId": "uuid", "itemName": "Milk", "quantity": 5, "priceSnapshot": 2.50 }
  ],
  "totalPrice": 12.50,
  "createdAt": "2026-03-23T10:00:00"
}
```

#### Order status lifecycle
```
PENDING  ──place──►  CONFIRMED  ──cancel──►  CANCELLED
   │                                              ▲
   └──────────────cancel──────────────────────────┘
```

---

### Alert Configuration

| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/inventory/alerts` | Create a low-stock alert threshold for an item |

#### Create alert — request body
```json
{
  "itemId": "uuid",
  "threshold": 10,
  "type": "LOW_STOCK"
}
```

After every `ORDER_PLACED` event, the service checks available stock against all configured thresholds for each ordered item and logs a warning if any threshold is breached.

---

## Inventory Model

Stock is tracked using a **SUPPLY / DEMAND ledger**:

```
available = SUM(SUPPLY where expiry > today) - SUM(DEMAND)
```

| Row type | Created when | Deleted when |
|---|---|---|
| `SUPPLY` | Item registered | Never (historical record) |
| `DEMAND` | Order placed (CONFIRMED) | Order cancelled |

This avoids in-place updates on quantity and gives a full audit trail of all stock movements.

---

## Stock Consistency & Concurrency

### The problem
Two concurrent `placeOrder` calls for the same item could both read the same available quantity and both succeed, causing **overbooking**.

### The solution — Redis distributed locking

```
placeOrder(orderId):
  ┌─ outside transaction ──────────────────────────────────┐
  │  1. Load order items → extract + sort itemIds          │
  │  2. Acquire Redis lock per itemId (tryLock 2s wait,    │
  │     10s lease) — sorted order prevents deadlock        │
  │     └─ fail → 503 SystemBusyException                  │
  └────────────────────────────────────────────────────────┘
  ┌─ TransactionTemplate (READ_COMMITTED) ─────────────────┐
  │  3. Re-validate order is PENDING                       │
  │  4. Per item: sumSupply - sumDemand >= requested?      │
  │     └─ fail → 422 InsufficientStockException           │
  │  5. Insert DEMAND row per item                         │
  │  6. Save order → CONFIRMED                             │
  └────────────────────────────────────────────────────────┘
  ┌─ finally (always) ─────────────────────────────────────┐
  │  7. Unlock all Redis locks                             │
  └────────────────────────────────────────────────────────┘
  8. Publish Kafka events (after commit — no orphaned msgs)
```

**Why the lock is outside the transaction:** holding a DB lock inside a long transaction ties up connections. Redis locks fail fast (2s timeout), free DB connections immediately, and make contention visible as HTTP 503 rather than silent timeouts.

**Deadlock prevention:** item IDs are sorted alphabetically before acquisition, so all threads acquire locks in the same order regardless of item order in the request.

---

## Kafka Events

### Topics

| Topic | Published by | Consumed by |
|---|---|---|
| `order.updates` | `OrderEventProducer` | `OrderEventConsumer` (alert checks) |
| `inventory.updates` | `InventoryEventProducer` | `StockTransactionConsumer` (audit log) |

### Event types

**OrderEvent** (`order.updates`)
```json
{
  "orderId": "uuid",
  "type": "ORDER_PLACED | ORDER_CANCELLED",
  "orderLines": [
    { "itemId": "uuid", "quantity": 5 }
  ]
}
```

**InventoryEvent** (`inventory.updates`)
```json
{
  "type": "SUPPLY_ADDED | DEMAND_CREATED | DEMAND_REMOVED",
  "inventoryId": "uuid",
  "itemId": "uuid",
  "quantity": 5.0,
  "inventoryType": "SUPPLY | DEMAND",
  "expiryDate": "2026-12-31",
  "referenceKey": "uuid (orderId for DEMAND rows)"
}
```

---

## Database Schema

```
item
├── id (UUID PK)
├── name, description, price
└── created_at

inventory
├── id (UUID PK)
├── item_key → item(id)
├── quantity (DOUBLE PRECISION)
├── type (SUPPLY | DEMAND)
├── expiry_date
├── reference_key (orderId for DEMAND rows)
└── updated_at

orders
├── id (UUID PK)
├── status (PENDING | CONFIRMED | CANCELLED)
├── created_at, updated_at

order_item
├── id (UUID PK)
├── order_key → orders(id)
├── item_key → item(id)
├── quantity
└── price_snapshot

stock_transaction  (pure event-log, no FKs)
├── id (UUID PK)
├── event_type (VARCHAR)
├── payload (TEXT — full JSON of the InventoryEvent)
└── created_at

alert_config
├── id (UUID PK)
├── item_key → item(id)
├── threshold (INT)
├── type (VARCHAR)
└── created_at
```

Schema migrations are managed by Liquibase (`db/changelog/db.changelog-master.xml`, changesets 1–22).

---

## Running Tests

```bash
./mvnw test
```

30 unit tests covering:
- `InventoryService` — item registration, stock calculation, nearest expiry
- `OrderService` — create/place/cancel order flows, insufficient stock, invalid state transitions, Redis lock fail-fast, **1000-thread concurrency test** (verifies no overbooking under parallel load)
- `AlertConfigService` — alert creation and deduplication

The concurrency test (`placeOrder_noOverbooking_under1000ConcurrentRequests`) backs the Redisson mock with a real `ReentrantLock`, fires 1000 threads against 10 units of stock, and asserts that DEMAND rows created never exceed supply.

---

## Error Responses

| Exception | HTTP status | When |
|---|---|---|
| `ItemNotFoundException` | 404 | Item ID does not exist |
| `OrderNotFoundException` | 404 | Order ID does not exist |
| `InsufficientStockException` | 422 | Available stock < requested quantity |
| `InvalidOrderStateException` | 409 | e.g. placing an already-confirmed order |
| `SystemBusyException` | 503 | Redis lock could not be acquired within 2s |
