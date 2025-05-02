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
import (
    "your-module/internal/framework/domain"
    "your-module/internal/framework/events"
)

type User struct {
    domain.AggregateRoot
    Email     string
    Name      string
    CreatedAt time.Time
}

func NewUser(id, email, name string) (*User, error) {
    if email == "" {
        return nil, errors.New("email is required")
    }
    
    user := &User{
        AggregateRoot: domain.NewAggregateRoot(id),
        Email:        email,
        Name:         name,
        CreatedAt:    time.Now(),
    }
    
    // Add domain event using the framework's event system
    user.AddEvent(events.NewEvent(
        "user.created",
        map[string]interface{}{
            "user_id":    user.ID(),
            "email":      user.Email,
            "name":       user.Name,
            "created_at": user.CreatedAt,
        },
    ))
    
    return user, nil
}

// Implement the Aggregate interface
func (u *User) AggregateID() string {
    return u.ID()
}

func (u *User) AggregateName() string {
    return "User"
}

func (u *User) Version() int {
    return u.AggregateRoot.Version()
}

func (u *User) ApplyEvent(event events.Event) {
    switch event.Type() {
    case "user.created":
        // Handle event application if needed
        break
    }
}
```

### 2. Create the Command

```go
// application/commands/create_user.go
type CreateUserCommand struct {
    cqrs.BaseCommand
    ID    string
    Email string
    Name  string
}

func NewCreateUserCommand(id, email, name string) *CreateUserCommand {
    return &CreateUserCommand{
        BaseCommand: cqrs.BaseCommand{
            Type: "create_user",
            Meta: make(domain.Metadata),
        },
        ID:    id,
        Email: email,
        Name:  name,
    }
}

type CreateUserHandler struct {
    userRepo domain.UserRepository
    flusher  *cqrs.AggregateFlusher
}

func NewCreateUserHandler(userRepo domain.UserRepository, dispatcher *domain.Dispatcher) *CreateUserHandler {
    return &CreateUserHandler{
        userRepo: userRepo,
        flusher:  &cqrs.AggregateFlusher{Dispatcher: dispatcher},
    }
}

func (h *CreateUserHandler) Handle(ctx context.Context, cmd Command) error {
    createCmd, ok := cmd.(*CreateUserCommand)
    if !ok {
        return fmt.Errorf("invalid command type: %T", cmd)
    }

    user, err := domain.NewUser(createCmd.ID, createCmd.Email, createCmd.Name)
    if err != nil {
        return err
    }
    
    if err := h.userRepo.Save(ctx, user); err != nil {
        return err
    }
    
    // Add the aggregate to the context
    ctx = cqrs.WithAggregate(ctx, user)
    
    return nil
}

// Create a command handler function with middleware
func NewCreateUserCommandHandler(userRepo domain.UserRepository, dispatcher *domain.Dispatcher) cqrs.CommandHandlerFunc {
    handler := &CreateUserHandler{
        userRepo: userRepo,
        flusher:  &cqrs.AggregateFlusher{Dispatcher: dispatcher},
    }
    
    return cqrs.NewCommandHandler(
        handler.Handle,
        cqrs.LoggingCommandMiddleware,
        cqrs.FlushingCommandMiddleware(handler.flusher),
    )
}
```

### 3. Create the Query

```go
// application/queries/get_user.go
type GetUserQuery struct {
    cqrs.BaseQuery
    ID string
}

func NewGetUserQuery(id string) *GetUserQuery {
    return &GetUserQuery{
        BaseQuery: cqrs.BaseQuery{
            Type: "get_user",
            Meta: make(domain.Metadata),
        },
        ID: id,
    }
}

type GetUserHandler struct {
    userRepo domain.UserRepository
}

func NewGetUserHandler(userRepo domain.UserRepository) *GetUserHandler {
    return &GetUserHandler{
        userRepo: userRepo,
    }
}

func (h *GetUserHandler) Handle(ctx context.Context, q Query) (interface{}, error) {
    getQuery, ok := q.(*GetUserQuery)
    if !ok {
        return nil, fmt.Errorf("invalid query type: %T", q)
    }
    
    return h.userRepo.FindByID(ctx, getQuery.ID)
}

// Create a query handler function with middleware
func NewGetUserQueryHandler(userRepo domain.UserRepository) cqrs.QueryHandlerFunc {
    handler := &GetUserHandler{
        userRepo: userRepo,
    }
    
    return cqrs.NewQueryHandler(
        handler.Handle,
        cqrs.LoggingQueryMiddleware,
    )
}
```

### 4. Wire Everything Together

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
    dispatcher := domain.NewDispatcher(eventBus)
    
    // Create and register command handler
    createUserHandler := commands.NewCreateUserCommandHandler(userRepo, dispatcher)
    commandBus.RegisterHandler("create_user", createUserHandler)
    
    // Create and register query handler
    getUserHandler := queries.NewGetUserQueryHandler(userRepo)
    queryBus.RegisterHandler("get_user", getUserHandler)
    
    // Register domain event subscriber
    userCreatedSubscriber := subscribers.NewUserCreatedSubscriber(integrationBus)
    eventBus.Subscribe("user.created", userCreatedSubscriber.Handle)
    
    // Start outbox relay
    go outbox.Relay(ctx, outboxRepo, integrationBus, time.Second, 100)
    
    // Example usage
    cmd := commands.NewCreateUserCommand(
        "user-123",
        "user@example.com",
        "John Doe",
    )
    
    if err := commandBus.Send(ctx, cmd); err != nil {
        log.Fatal(err)
    }
    
    // Example query usage
    qry := queries.NewGetUserQuery("user-123")
    user, err := queryBus.Send(ctx, qry)
    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("Found user: %+v\n", user)
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
