package domain

import (
	"context"

	"github.com/dritelabs/internal/framework/bus"
)

// DomainEvent is the base interface for all domain events.
type DomainEvent interface {
	EventType() string
	EventID() string
	Payload() any
	Metadata() Metadata
}

// Dispatcher uses the EventBus for domain event delivery.
type Dispatcher struct {
	eventBus *bus.EventBus
}

// NewDispatcher creates a new domain event dispatcher using the provided EventBus.
func NewDispatcher(eventBus *bus.EventBus) *Dispatcher {
	return &Dispatcher{eventBus: eventBus}
}

// Subscribe registers a handler for a specific domain event type.
func (d *Dispatcher) Subscribe(eventType string, handler func(ctx context.Context, event DomainEvent) error) error {
	return d.eventBus.Subscribe(eventType, func(ctx context.Context, evt bus.Event) error {
		domainEvt, ok := evt.(DomainEvent)
		if !ok {
			return nil // or return an error if you want strictness
		}
		return handler(ctx, domainEvt)
	})
}

// Publish synchronously publishes the event to the EventBus and waits for all handlers.
func (d *Dispatcher) Publish(ctx context.Context, event DomainEvent) error {
	return d.eventBus.PublishSync(ctx, event.EventType(), event)
}
