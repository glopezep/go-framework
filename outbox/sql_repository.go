package outbox

import (
	"context"
	"database/sql"
	"time"
)

type SQLRepository struct {
	db *sql.DB
}

func NewSQLRepository(db *sql.DB) *SQLRepository {
	return &SQLRepository{db: db}
}

func (r *SQLRepository) Save(ctx context.Context, msg *Message) error {
	_, err := r.db.ExecContext(ctx, `
		INSERT INTO outbox (id, name, aggregate_id, aggregate_name, subject, data, metadata, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, now())
		ON CONFLICT (id) DO NOTHING
	`, msg.ID, msg.Name, msg.AggregateID, msg.AggregateName, msg.Subject, msg.Data, msg.Metadata)
	return err
}

func (r *SQLRepository) MarkPublished(ctx context.Context, id string, publishedAt time.Time) error {
	_, err := r.db.ExecContext(ctx, `
		UPDATE outbox SET published_at = $2, updated_at = now() WHERE id = $1
	`, id, publishedAt)
	return err
}

func (r *SQLRepository) Pending(ctx context.Context, limit int) ([]*Message, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT id, name, aggregate_id, aggregate_name, subject, data, metadata, published_at, created_at, updated_at, deleted_at
		FROM outbox
		WHERE published_at IS NULL AND deleted_at IS NULL
		ORDER BY created_at ASC
		LIMIT $1
	`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var msgs []*Message
	for rows.Next() {
		var m Message
		err := rows.Scan(
			&m.ID, &m.Name, &m.AggregateID, &m.AggregateName, &m.Subject,
			&m.Data, &m.Metadata, &m.PublishedAt, &m.CreatedAt, &m.UpdatedAt, &m.DeletedAt,
		)
		if err != nil {
			return nil, err
		}
		msgs = append(msgs, &m)
	}
	return msgs, nil
}
