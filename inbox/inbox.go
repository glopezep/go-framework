package inbox

import (
	"context"
	"database/sql"
)

// Store defines the interface for message deduplication
type Store interface {
	// HasProcessed checks if a message has been processed
	HasProcessed(ctx context.Context, messageID string) (bool, error)
	// MarkAsProcessed marks a message as processed
	MarkAsProcessed(ctx context.Context, messageID string) error
}

// SQLStore implements Store using SQL database
type SQLStore struct {
	db *sql.DB
}

// NewSQLStore creates a new SQL-backed inbox store
func NewSQLStore(db *sql.DB) *SQLStore {
	return &SQLStore{db: db}
}

func (s *SQLStore) HasProcessed(ctx context.Context, messageID string) (bool, error) {
	var exists bool
	err := s.db.QueryRowContext(ctx, `
        SELECT EXISTS (
            SELECT 1 FROM inbox_messages 
            WHERE message_id = $1
        )
    `, messageID).Scan(&exists)

	return exists, err
}

func (s *SQLStore) MarkAsProcessed(ctx context.Context, messageID string) error {
	_, err := s.db.ExecContext(ctx, `
        INSERT INTO inbox_messages (message_id, processed_at)
        VALUES ($1, NOW())
        ON CONFLICT (message_id) DO NOTHING
    `, messageID)
	return err
}
