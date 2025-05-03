package integration

import (
	"context"
	"time"

	"github.com/glopezep/framework/outbox"
)

func OutboxMiddleware(repo outbox.Repository) PublishMiddleware {
	return func(next PublishFunc) PublishFunc {
		return func(ctx context.Context, event Event) error {
			// Store in outbox instead of publishing
			msg := &outbox.Message{
				ID:            event.Metadata()["id"].(string),
				Name:          event.Name(),
				AggregateID:   event.Metadata()["aggregate_id"].(string),
				AggregateName: event.Metadata()["aggregate_name"].(string),
				Subject:       event.Name(),
				Data:          nil, // serialize event.Payload() as needed
				Metadata:      nil, // serialize event.Metadata() as needed
				CreatedAt:     time.Now(),
			}
			return repo.Save(ctx, msg)
		}
	}
}
