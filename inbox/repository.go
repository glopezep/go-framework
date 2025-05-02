package inbox

import (
	"context"
	"time"
)

type Message struct {
	ID            string
	Name          string
	AggregateID   string
	AggregateName string
	Subject       string
	Data          []byte
	Metadata      []byte
	SentAt        time.Time
	CreatedAt     time.Time
	UpdatedAt     *time.Time
	DeletedAt     *time.Time
}

type Repository interface {
	// Returns true if the message with this ID already exists
	Exists(ctx context.Context, id string) (bool, error)
	// Saves the message to the inbox (insert if new, update if exists)
	Save(ctx context.Context, msg *Message) error
}
