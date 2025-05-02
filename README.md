# Go Microservices Framework

A modular, extensible framework for building robust, event-driven Go microservices with CQRS, DDD, and reliable messaging patterns.

---

## Features

- **CQRS Buses:** Command, Query, and Event buses with flexible publisher/subscriber instantiation.
- **Integration Bus:** JetStream-based integration event bus, supporting publisher-only, subscriber-only, or both.
- **Inbox Pattern:** Deduplication of incoming messages to ensure idempotent processing.
- **Outbox Pattern:** Reliable, atomic event publishing with database-backed outbox and relay.
- **Middleware:** Clean middleware support for both publishing and subscribing.
- **Pluggable Storage:** Inbox and outbox repositories are interfaces—use SQL or your own implementation.

---

## Architecture Overview

```
+-------------------+         +-------------------+         +-------------------+
|   Command Bus     |         |   Query Bus       |         |   Event Bus       |
|-------------------|         |-------------------|         |-------------------|
| Publisher/Subscriber logic  | Publisher/Subscriber logic  | Publisher/Subscriber logic
+-------------------+         +-------------------+         +-------------------+

         | (Integration Events)
         v

+-----------------------------------------------------------+
|                Integration Bus (JetStream)                |
|  - Publisher, Subscriber, or Both                         |
|  - Middleware for Inbox (subscribe) and Outbox (publish)  |
+-----------------------------------------------------------+

         | (Persistence)
         v

+-------------------+         +-------------------+
|      Inbox        |         |      Outbox       |
|-------------------|         |-------------------|
| Deduplication     |         | Reliable Publish  |
| SQL or custom     |         | SQL or custom     |
+-------------------+         +-------------------+
```

---

## Example Usage

### 1. Setup Outbox and Inbox

```go
import (
    "database/sql"
    "your-module/internal/framework/inbox"
    "your-module/internal/framework/outbox"
)

db, _ := sql.Open("postgres", "...")
inboxRepo := inbox.NewSQLStore(db)
outboxRepo := outbox.NewSQLRepository(db)
```

---

### 2. Setup JetStream Integration Bus

```go
import (
    "your-module/internal/framework/integration"
)

bus := integration.NewJetStreamBus(jetstreamConfig, inboxRepo, outboxRepo)
```

---

### 3. Register Middleware

```go
bus.UseSubscribe(integration.InboxMiddleware(inboxRepo))   // Deduplicate incoming messages
bus.UsePublish(integration.OutboxMiddleware(outboxRepo))   // Store outgoing events in outbox
```

---

### 4. Subscribe to Events

```go
bus.Subscribe("user.created", func(ctx context.Context, event integration.Event) error {
    // Your business logic here
    fmt.Println("User created:", event.Payload())
    return nil
})
```

---

### 5. Publish Events

```go
event := integration.NewEvent("user.created", map[string]interface{}{
    "user_id": "123",
    "email":   "user@example.com",
})
event.SetMetadata("id", "event-uuid-123")
event.SetMetadata("aggregate_id", "user-123")
event.SetMetadata("aggregate_name", "User")

err := bus.Publish(ctx, event)
```

---

### 6. Outbox Relay (Background Publisher)

```go
import "your-module/internal/framework/outbox"

go outbox.Relay(ctx, outboxRepo, bus, time.Second, 100)
```
This will periodically publish pending outbox messages to JetStream and mark them as published.

---

## Flexible Bus Instantiation

You can instantiate any bus as:
- **Publisher only:** `NewIntegrationPublisher(pub)`
- **Subscriber only:** `NewIntegrationSubscriber(sub)`
- **Full bus:** `NewIntegrationBus(pub, sub)`

---

## Extending

- Implement your own `inbox.Repository` or `outbox.Repository` for custom storage.
- Add more middleware for logging, tracing, validation, etc.

---

## Table Schemas

**Inbox:**
```sql
CREATE TABLE inbox (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  aggregate_id TEXT NOT NULL,
  aggregate_name TEXT NOT NULL,
  subject TEXT NOT NULL,
  data BYTEA,
  metadata BYTEA NOT NULL,
  sent_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ,
  deleted_at TIMESTAMPTZ
);
```

**Outbox:**
```sql
CREATE TABLE outbox (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  aggregate_id TEXT NOT NULL,
  aggregate_name TEXT NOT NULL,
  subject TEXT NOT NULL,
  data BYTEA,
  metadata BYTEA NOT NULL,
  published_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ,
  deleted_at TIMESTAMPTZ
);
```

---

## Summary

- **Inbox**: Ensures idempotency for incoming events.
- **Outbox**: Guarantees reliable, atomic event publishing.
- **CQRS Buses**: Clean separation of command, query, and event flows.
- **Integration Bus**: JetStream-based, with middleware for inbox/outbox.
- **Middleware**: Add cross-cutting concerns easily.

---

**Ready to build robust, event-driven Go microservices!** 