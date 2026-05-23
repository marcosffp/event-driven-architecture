package repository

import (
	"context"
	"database/sql"
	"fmt"
)

type PostgresProcessedEventRepository struct {
	db *sql.DB
}

func NewPostgresProcessedEventRepository(db *sql.DB) *PostgresProcessedEventRepository {
	return &PostgresProcessedEventRepository{db: db}
}

func (r *PostgresProcessedEventRepository) HasProcessed(ctx context.Context, eventID, consumerGroup string) (bool, error) {
	var exists bool
	err := r.db.QueryRowContext(ctx,
		"SELECT EXISTS(SELECT 1 FROM processed_events WHERE event_id = $1 AND consumer_group = $2)",
		eventID, consumerGroup,
	).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("hasProcessed: %w", err)
	}
	return exists, nil
}

func (r *PostgresProcessedEventRepository) MarkProcessed(ctx context.Context, eventID, consumerGroup string) error {
	_, err := r.db.ExecContext(ctx,
		"INSERT INTO processed_events (event_id, consumer_group) VALUES ($1, $2) ON CONFLICT DO NOTHING",
		eventID, consumerGroup,
	)
	if err != nil {
		return fmt.Errorf("markProcessed: %w", err)
	}
	return nil
}
