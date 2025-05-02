package cqrs

import (
	"context"

	"github.com/dritelabs/internal/framework/domain"
)

type aggregateKey struct{}

func WithAggregate(ctx context.Context, agg domain.AggregateRoot) context.Context {
	aggs := AggregatesFromContext(ctx)
	aggs = append(aggs, agg)
	return context.WithValue(ctx, aggregateKey{}, aggs)
}

func AggregatesFromContext(ctx context.Context) []domain.AggregateRoot {
	val := ctx.Value(aggregateKey{})
	if val == nil {
		return nil
	}
	return val.([]domain.AggregateRoot)
}
