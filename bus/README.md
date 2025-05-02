# Bus Package

This package provides reusable, generic message buses for Go microservices, supporting:
- Multiple handlers per topic
- Middleware for cross-cutting concerns (logging, tracing, validation, etc.)
- Error aggregation (collects all handler errors)
- Synchronous and asynchronous event publishing
- DRY, type-safe, and extensible design using Go generics

## Buses Provided

- **CommandBus**: For commands (actions that change state)
- **QueryBus**: For queries (read-only requests)
- **EventBus**: For events (notifications, pub/sub)

All buses use string topics for routing and support multiple handlers per topic.

---

## Usage

### 1. Import

```go
import "yourmodule/internal/framework/bus"
```

### 2. CommandBus Example

```go
cbus := bus.NewCommandBus()

// Middleware example
cbus.Use(func(next bus.CommandHandler) bus.CommandHandler {
    return func(ctx context.Context, cmd bus.Command) error {
        fmt.Println("Before command")
        err := next(ctx, cmd)
        fmt.Println("After command")
        return err
    }
})

// Subscribe handlers
cbus.Subscribe("user.create", func(ctx context.Context, cmd bus.Command) error {
    // handle command
    return nil
})

// Publish a command
err := cbus.Publish(ctx, "user.create", MyCommand{/*...*/})
```

---

### 3. QueryBus Example

```go
qbus := bus.NewQueryBus()

qbus.Subscribe("user.get", func(ctx context.Context, qry bus.Query) (any, error) {
    // handle query
    return User{/*...*/}, nil
})

results, err := qbus.Publish(ctx, "user.get", MyQuery{/*...*/})
```

---

### 4. EventBus Example

```go
ebus := bus.NewEventBus()

ebus.Subscribe("user.created", func(ctx context.Context, evt bus.Event) error {
    // handle event
    return nil
})

// Async (fire-and-forget)
ebus.PublishAsync(ctx, "user.created", UserCreatedEvent{/*...*/})

// Sync (waits for all handlers, aggregates errors)
err := ebus.PublishSync(ctx, "user.created", UserCreatedEvent{/*...*/})
```

---

## Features

- **Multiple Handlers:** All buses support multiple handlers per topic.
- **Middleware:** Add cross-cutting logic with `Use()`.
- **Error Aggregation:** All handler errors are collected and returned as a `MultiError`.
- **Type Safety:** Uses Go generics for handler signatures.
- **Extensible:** Add new buses or features easily.

---

## Extending

You can add:
- Wildcard topic support
- Handler priorities
- Message envelopes (for metadata, tracing, etc.)
- Integration with NATS, Kafka, etc.

---

## License

MIT (or your preferred license) 