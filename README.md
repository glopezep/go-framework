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
|  CQRS Command     |         |   CQRS Query      |         |   Event Bus       |
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

## Comprehensive Example: User Registration Flow

This example demonstrates a complete flow from command handling to domain events and integration events.

### 1. Define the User Aggregate

```go
// domain/user/aggregate.go
type User struct {
    ID        string
    Email     string
    Name      string
    CreatedAt time.Time
}

func NewUser(id, email, name string) (*User, error) {
    if email == "" {
        return nil, errors.New("email is required")
    }
    
    user := &User{
        ID:        id,
        Email:     email,
        Name:      name,
        CreatedAt: time.Now(),
    }
    
    // Raise domain event
    user.RaiseEvent(events.NewUserCreatedEvent(user))
    
    return user, nil
}
```

### 2. Create the Command

```go
// application/commands/create_user.go
type CreateUserCommand struct {
    ID    string
    Email string
    Name  string
}

type CreateUserHandler struct {
    userRepo domain.UserRepository
    eventBus event.Bus
}

func NewCreateUserHandler(userRepo domain.UserRepository, eventBus event.Bus) *CreateUserHandler {
    return &CreateUserHandler{
        userRepo: userRepo,
        eventBus: eventBus,
    }
}

func (h *CreateUserHandler) Handle(ctx context.Context, cmd CreateUserCommand) error {
    user, err := domain.NewUser(cmd.ID, cmd.Email, cmd.Name)
    if err != nil {
        return err
    }
    
    if err := h.userRepo.Save(ctx, user); err != nil {
        return err
    }
    
    return nil
}
```

### 3. Define Domain Events

```go
// domain/events/user_events.go
type UserCreatedEvent struct {
    UserID    string
    Email     string
    Name      string
    CreatedAt time.Time
}

func NewUserCreatedEvent(user *domain.User) *UserCreatedEvent {
    return &UserCreatedEvent{
        UserID:    user.ID,
        Email:     user.Email,
        Name:      user.Name,
        CreatedAt: user.CreatedAt,
    }
}
```

### 4. Create Domain Event Subscriber

```go
// application/subscribers/user_created_subscriber.go
type UserCreatedSubscriber struct {
    integrationBus integration.Bus
}

func NewUserCreatedSubscriber(integrationBus integration.Bus) *UserCreatedSubscriber {
    return &UserCreatedSubscriber{
        integrationBus: integrationBus,
    }
}

func (s *UserCreatedSubscriber) Handle(ctx context.Context, event *events.UserCreatedEvent) error {
    // Convert domain event to integration event
    integrationEvent := integration.NewEvent("user.created", map[string]interface{}{
        "user_id":    event.UserID,
        "email":      event.Email,
        "name":       event.Name,
        "created_at": event.CreatedAt,
    })
    
    // Set metadata for tracking
    integrationEvent.SetMetadata("id", uuid.New().String())
    integrationEvent.SetMetadata("aggregate_id", event.UserID)
    integrationEvent.SetMetadata("aggregate_name", "User")
    
    // Publish to integration bus
    return s.integrationBus.Publish(ctx, integrationEvent)
}
```

### 5. Wire Everything Together

```go
// main.go
func main() {
    // Setup database
    db, _ := sql.Open("postgres", "...")
    
    // Setup repositories
    inboxRepo := inbox.NewSQLStore(db)
    outboxRepo := outbox.NewSQLRepository(db)
    userRepo := persistence.NewUserRepository(db)
    
    // Setup buses
    integrationBus := integration.NewJetStreamBus(jetstreamConfig, inboxRepo, outboxRepo)
    eventBus := event.NewBus()
    
    // Register middleware
    integrationBus.UseSubscribe(integration.InboxMiddleware(inboxRepo))
    integrationBus.UsePublish(integration.OutboxMiddleware(outboxRepo))
    
    // Register command handler
    createUserHandler := commands.NewCreateUserHandler(userRepo, eventBus)
    commandBus.RegisterHandler(createUserHandler)
    
    // Register domain event subscriber
    userCreatedSubscriber := subscribers.NewUserCreatedSubscriber(integrationBus)
    eventBus.Subscribe(events.UserCreatedEventType, userCreatedSubscriber.Handle)
    
    // Start outbox relay
    go outbox.Relay(ctx, outboxRepo, integrationBus, time.Second, 100)
    
    // Example usage
    cmd := commands.CreateUserCommand{
        ID:    "user-123",
        Email: "user@example.com",
        Name:  "John Doe",
    }
    
    if err := commandBus.Send(ctx, cmd); err != nil {
        log.Fatal(err)
    }
}
```

This example demonstrates:
1. Creating a user aggregate with domain events
2. Handling a command to create a user
3. Persisting the user in the database
4. Publishing a domain event
5. Subscribing to the domain event
6. Converting the domain event to an integration event
7. Publishing the integration event through the outbox pattern
8. Relaying the outbox messages to the integration bus

The flow ensures:
- Atomic operations (user creation and event publishing)
- Reliable event delivery through the outbox pattern
- Idempotent processing through the inbox pattern
- Clear separation between domain and integration events

**Ready to build robust, event-driven Go microservices!** 