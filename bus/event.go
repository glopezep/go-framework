package bus

import (
	"context"
	"sync"
)

// Event is the base interface for events.
type Event interface{}

// EventHandler and EventMiddleware types.
type EventHandler = HandlerFunc[Event]
type EventMiddleware = Middleware[Event]

// EventBus handles event subscription and publishing.
type EventBus struct {
	*baseBus[Event]
}

// NewEventBus creates a new EventBus.
func NewEventBus() *EventBus {
	return &EventBus{newBaseBus[Event]()}
}

// PublishAsync sends an event to all handlers for the topic asynchronously (fire-and-forget).
func (b *EventBus) PublishAsync(ctx context.Context, topic string, evt Event) {
	handlers := b.Handlers(topic)
	for _, handler := range handlers {
		go handler(ctx, evt)
	}
}

// PublishSync sends an event to all handlers for the topic and waits for all to finish, aggregating errors.
func (b *EventBus) PublishSync(ctx context.Context, topic string, evt Event) error {
	handlers := b.Handlers(topic)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var errs []error
	for _, handler := range handlers {
		wg.Add(1)
		go func(h EventHandler) {
			defer wg.Done()
			if err := h(ctx, evt); err != nil {
				mu.Lock()
				errs = append(errs, err)
				mu.Unlock()
			}
		}(handler)
	}
	wg.Wait()
	if len(errs) > 0 {
		return &MultiError{Errors: errs}
	}
	return nil
}
