# Drite Go Microservices Framework

A modular, extensible framework for building robust, event-driven Go microservices using DDD, CQRS, and integration patterns.

---

## Features

- **CQRS**: Command and Query separation with aggregate event flushing
- **Domain Events**: In-process event dispatcher
- **Inbox/Outbox**: Reliable message delivery and deduplication
- **Integration Events**: Optional NATS JetStream support
- **Extensible Application Struct**: Easily register, create, and execute handlers

---

## Getting Started

### 1. Install

Add to your Go project:

```bash
go get github.com/dritelabs/internal/framework
```

### 2. Initialize the Application

#### Core CQRS/DDD Only

```go
import "github.com/dritelabs/internal/framework"

func main() {
    inboxRepo := /* your inbox.Repository implementation */
    outboxRepo := /* your outbox.Repository implementation */

    app := framework.NewApplication(inboxRepo, outboxRepo)

    // Register domain event handlers
    app.DomainDispatcher.Subscribe("UserCreated", func(ctx context.Context, evt domain.DomainEvent) error {
        // handle event
        return nil
    })

    // Use app.Flusher.Flush(ctx, aggregate) after command handling
}
```

#### With Integration Events (NATS JetStream)

```go
import (
    "github.com/dritelabs/internal/framework"
    "github.com/dritelabs/internal/framework/integration"
)

func main() {
    inboxRepo := /* your inbox.Repository implementation */
    outboxRepo := /* your outbox.Repository implementation */
    jetStreamBus := integration.NewJetStreamBus(/* ... */)

    app := framework.NewApplication(
        inboxRepo,
        outboxRepo,
        framework.WithJetStreamBus(jetStreamBus),
    )

    // Register integration event handlers
    app.Bus.Subscribe("integration.topic", func(ctx context.Context, msg integration.Event) error {
        // handle integration event
        return nil
    })

    // Publish integration events
    app.Bus.Publish(ctx, "integration.topic", event)
}
```

---

## Application Struct

```go
type Application struct {
    Flusher          *cqrs.AggregateFlusher
    InboxRepo        inbox.Repository
    OutboxRepo       outbox.Repository
    DomainDispatcher *domain.Dispatcher
    Bus              *integration.JetStreamBus // Optional

    commandHandlers map[string]cqrs.CommandHandler
    queryHandlers   map[string]cqrs.QueryHandler
}
```

### Functional Options

- `framework.WithJetStreamBus(bus *integration.JetStreamBus)`

---

## Handler Registration, Creation, and Execution

### Registering Handlers

```go
app := framework.NewApplication(inboxRepo, outboxRepo)

// Register a command handler
app.RegisterCommandHandler("CreateUser", createUserHandler)

// Register a query handler
app.RegisterQueryHandler("GetUser", getUserHandler)
```

### Creating Handlers with Middleware

```go
// Create a command handler with default and custom middleware
handler := app.NewCommandHandler(myCommandHandlerFunc, myCustomMiddleware)
app.RegisterCommandHandler("MyCommand", handler)

// Create a query handler
qHandler := app.NewQueryHandler(myQueryHandlerFunc)
app.RegisterQueryHandler("MyQuery", qHandler)
```

### Executing Handlers

```go
err := app.ExecuteCommand(ctx, "CreateUser", &CreateUserCommand{Name: "Alice"})
result, err := app.ExecuteQuery(ctx, "GetUser", &GetUserQuery{Name: "Alice"})
```

---

## Full Example

```go
package main

import (
    "context"
    "fmt"
    "github.com/dritelabs/internal/framework"
    "github.com/dritelabs/internal/framework/cqrs"
    "github.com/dritelabs/internal/framework/domain"
    "github.com/dritelabs/internal/framework/inbox"
    "github.com/dritelabs/internal/framework/integration"
    "github.com/dritelabs/internal/framework/outbox"
    "time"
)

// --- Command and Handler ---
type CreateUserCommand struct {
    Name string
}

func CreateUserHandlerFunc(ctx context.Context, cmd cqrs.Command) error {
    c, ok := cmd.(*CreateUserCommand)
    if !ok {
        return fmt.Errorf("invalid command type")
    }
    fmt.Println("User created:", c.Name)
    // Publish a domain event
    domainEvent := domain.NewEvent("UserCreated", map[string]any{"name": c.Name})
    app.DomainDispatcher.Publish(ctx, domainEvent)
    return nil
}

// --- Query and Handler ---
type GetUserQuery struct {
    Name string
}

func GetUserHandlerFunc(ctx context.Context, qry cqrs.Query) (any, error) {
    q, ok := qry.(*GetUserQuery)
    if !ok {
        return nil, fmt.Errorf("invalid query type")
    }
    return "User: " + q.Name, nil
}

// --- Domain Event Handler ---
func userCreatedDomainHandler(ctx context.Context, evt domain.DomainEvent) error {
    fmt.Println("Domain event received:", evt.Name(), evt.Payload())
    return nil
}

// --- Integration Event Handler ---
func integrationEventHandler(ctx context.Context, evt integration.Event) error {
    fmt.Println("Integration event received:", evt.Name, string(evt.Data))
    return nil
}

var app *framework.Application // Make app accessible to handlers

func main() {
    // Initialize repositories (use your own implementations)
    inboxRepo := inbox.NewSQLRepository(nil)
    outboxRepo := outbox.NewSQLRepository(nil)

    // (Optional) Initialize JetStream bus for integration events
    var jetStreamBus *integration.JetStreamBus
    // jetStreamBus = integration.NewJetStreamBus(...)

    // Create the application (with or without integration bus)
    app = framework.NewApplication(
        inboxRepo,
        outboxRepo,
        // framework.WithJetStreamBus(jetStreamBus), // Uncomment if using integration events
    )

    // Create and register command and query handlers
    createUserHandler := app.NewCommandHandler(CreateUserHandlerFunc)
    getUserHandler := app.NewQueryHandler(GetUserHandlerFunc)

    app.RegisterCommandHandler("CreateUser", createUserHandler)
    app.RegisterQueryHandler("GetUser", getUserHandler)

    // Register a domain event handler
    app.DomainDispatcher.Subscribe("UserCreated", userCreatedDomainHandler)

    // Register an integration event handler (if using JetStream)
    if app.Bus != nil {
        app.Bus.Subscribe("user.created.integration", integrationEventHandler)
    }

    ctx := context.Background()

    // Execute a command (triggers domain event)
    err := app.ExecuteCommand(ctx, "CreateUser", &CreateUserCommand{Name: "Alice"})
    if err != nil {
        fmt.Println("Command error:", err)
    }

    // Execute a query
    result, err := app.ExecuteQuery(ctx, "GetUser", &GetUserQuery{Name: "Alice"})
    if err != nil {
        fmt.Println("Query error:", err)
    } else {
        fmt.Println("Query result:", result)
    }

    // (Optional) Publish an integration event
    if app.Bus != nil {
        evt := integration.Event{
            Name: "user.created.integration",
            Data: []byte(`{"name":"Alice"}`),
            Metadata: map[string]string{
                "timestamp": time.Now().Format(time.RFC3339),
            },
        }
        _ = app.Bus.Publish(ctx, evt.Name, evt)
    }
}
```

---

## Extending

You can add more optional components by following the same pattern:
```go
func WithMyComponent(c *MyComponent) ApplicationOption {
    return func(app *Application) {
        app.MyComponent = c
    }
}
```

---

## License

MIT 