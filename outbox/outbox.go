package outbox

import (
	"context"
	"time"
)

// Message matches your outbox table schema.
type Message struct {
	ID            string
	Name          string
	AggregateID   string
	AggregateName string
	Subject       string
	Data          []byte
	Metadata      []byte
	PublishedAt   *time.Time
	CreatedAt     time.Time
	UpdatedAt     *time.Time
	DeletedAt     *time.Time
}

// Repository defines the interface for outbox storage.
type Repository interface {
	Save(ctx context.Context, msg *Message) error
	MarkPublished(ctx context.Context, id string, publishedAt time.Time) error
	Pending(ctx context.Context, limit int) ([]*Message, error)
}
