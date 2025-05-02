package bus

import (
	"context"
	"errors"
	"sync"
)

// MultiError aggregates multiple errors from handlers.
type MultiError struct {
	Errors []error
}

func (m *MultiError) Error() string {
	msg := "multiple errors:"
	for _, err := range m.Errors {
		msg += "\n - " + err.Error()
	}
	return msg
}

// HandlerFunc and Middleware are generic for all bus types.
type HandlerFunc[M any] func(ctx context.Context, msg M) error
type Middleware[M any] func(HandlerFunc[M]) HandlerFunc[M]

// baseBus is the generic bus implementation.
type baseBus[M any] struct {
	handlers    map[string][]HandlerFunc[M]
	middlewares []Middleware[M]
	mu          sync.RWMutex
}

func newBaseBus[M any]() *baseBus[M] {
	return &baseBus[M]{handlers: make(map[string][]HandlerFunc[M])}
}

func (b *baseBus[M]) Use(mw Middleware[M]) {
	b.middlewares = append(b.middlewares, mw)
}

func (b *baseBus[M]) Subscribe(topic string, handler HandlerFunc[M]) error {
	if topic == "" {
		return errors.New("topic cannot be empty")
	}
	for i := len(b.middlewares) - 1; i >= 0; i-- {
		handler = b.middlewares[i](handler)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.handlers[topic] = append(b.handlers[topic], handler)
	return nil
}

func (b *baseBus[M]) Handlers(topic string) []HandlerFunc[M] {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.handlers[topic]
}
