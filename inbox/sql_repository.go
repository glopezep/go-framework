package inbox

import (
	"context"
	"database/sql"
)

type SQLRepository struct {
	db *sql.DB
}

func NewSQLRepository(db *sql.DB) *SQLRepository {
	return &SQLRepository{db: db}
}

func (r *SQLRepository) Exists(ctx context.Context, id string) (bool, error) {
	var exists bool
	err := r.db.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM inbox WHERE id = $1)`, id).Scan(&exists)
	return exists, err
}

func (r *SQLRepository) Save(ctx context.Context, msg *Message) error {
	// Upsert logic: insert if not exists, else update
	_, err := r.db.ExecContext(ctx, `
        INSERT INTO inbox (id, name, aggregate_id, aggregate_name, subject, data, metadata, sent_at, created_at, updated_at, deleted_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, COALESCE($9, now()), $10, $11)
        ON CONFLICT (id) DO UPDATE SET
            name = EXCLUDED.name,
            aggregate_id = EXCLUDED.aggregate_id,
            aggregate_name = EXCLUDED.aggregate_name,
            subject = EXCLUDED.subject,
            data = EXCLUDED.data,
            metadata = EXCLUDED.metadata,
            sent_at = EXCLUDED.sent_at,
            updated_at = now(),
            deleted_at = EXCLUDED.deleted_at
    `,
		msg.ID, msg.Name, msg.AggregateID, msg.AggregateName, msg.Subject, msg.Data, msg.Metadata, msg.SentAt, msg.CreatedAt, msg.UpdatedAt, msg.DeletedAt,
	)
	return err
}
