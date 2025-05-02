package bus

import (
	"context"
	"errors"
)

// Query is the base interface for queries.
type Query interface{}

// QueryHandler and QueryMiddleware types.
type QueryHandler = HandlerFunc[Query]
type QueryMiddleware = Middleware[Query]

// QueryBus handles query subscription and publishing.
type QueryBus struct {
	*baseBus[Query]
}

// NewQueryBus creates a new QueryBus.
func NewQueryBus() *QueryBus {
	return &QueryBus{newBaseBus[Query]()}
}

// Publish sends a query to all subscribed handlers for the topic and collects results.
func (b *QueryBus) Publish(ctx context.Context, topic string, qry Query) []error {
	handlers := b.Handlers(topic)
	if len(handlers) == 0 {
		return []error{errors.New("no handler subscribed for query topic: " + topic)}
	}
	var errs []error
	for _, handler := range handlers {
		if err := handler(ctx, qry); err != nil {
			errs = append(errs, err)
		}
	}
	return errs
}
