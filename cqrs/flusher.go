package cqrs

import (
	"context"

	"github.com/dritelabs/internal/framework/domain"
)

type AggregateFlusher struct {
	Dispatcher *domain.Dispatcher
}

func (f *AggregateFlusher) Flush(ctx context.Context, aggregates ...domain.AggregateRoot) error {
	for _, agg := range aggregates {
		for _, event := range agg.Events() {
			if err := f.Dispatcher.Publish(ctx, event); err != nil {
				return err // Optionally aggregate errors
			}
		}
		agg.ClearEvents()
	}
	return nil
}
