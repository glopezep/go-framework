package outbox

import (
	"context"
	"time"
)

type Publisher interface {
	Publish(ctx context.Context, subject string, data []byte) error
}

func Relay(ctx context.Context, repo Repository, publisher Publisher, pollInterval time.Duration, batchSize int) {
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			msgs, err := repo.Pending(ctx, batchSize)
			if err != nil {
				// log error
				continue
			}
			for _, msg := range msgs {
				if err := publisher.Publish(ctx, msg.Subject, msg.Data); err == nil {
					_ = repo.MarkPublished(ctx, msg.ID, time.Now())
				}
			}
		}
	}
}
