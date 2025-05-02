package cqrs

import (
	"context"

	"github.com/dritelabs/internal/framework/domain"
)

// Query is the base interface for all queries.
type Query interface {
	QueryType() string
	Metadata() domain.Metadata
}

// QueryHandler handles a query and returns a result.
type QueryHandler interface {
	Handle(ctx context.Context, qry Query) (any, error)
}

// BaseQuery provides a default implementation for queries.
type BaseQuery struct {
	Type string
	Meta domain.Metadata
}

func (q *BaseQuery) QueryType() string { return q.Type }
func (q *BaseQuery) Metadata() domain.Metadata {
	if q.Meta == nil {
		q.Meta = make(domain.Metadata)
	}
	return q.Meta
}

type QueryHandlerFunc func(ctx context.Context, qry Query) (any, error)
type QueryMiddleware func(QueryHandlerFunc) QueryHandlerFunc

// NewQueryHandler creates a QueryHandlerFunc with middleware support.
func NewQueryHandler(handler QueryHandlerFunc, mws ...QueryMiddleware) QueryHandlerFunc {
	for i := len(mws) - 1; i >= 0; i-- {
		handler = mws[i](handler)
	}
	return handler
}
