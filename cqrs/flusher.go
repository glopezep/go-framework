package cqrs

import (
	"context"

	"github.com/glopezep/framework/domain"
)

type AggregateFlusher struct {
	dispatcher *domain.Dispatcher
}

func NewAggregateFlusher(dispatcher *domain.Dispatcher) *AggregateFlusher {
	return &AggregateFlusher{dispatcher: dispatcher}
}

func (f *AggregateFlusher) Flush(ctx context.Context, aggregates ...domain.AggregateRoot) error {
	for _, agg := range aggregates {
		for _, event := range agg.Events() {
			if err := f.dispatcher.Publish(ctx, event); err != nil {
				return err // Optionally aggregate errors
			}
		}
		agg.ClearEvents()
	}
	return nil
}
