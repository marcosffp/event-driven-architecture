package postgres

import (
	"context"
	"database/sql"
	"fmt"
)

type ProcessedEventRepository struct {
	db *sql.DB
}

func NewProcessedEventRepository(db *sql.DB) *ProcessedEventRepository {
	return &ProcessedEventRepository{db: db}
}

func (r *ProcessedEventRepository) TryClaim(ctx context.Context, eventID, consumerGroup string) (bool, error) {
	result, err := r.db.ExecContext(ctx,
		"INSERT INTO processed_events (event_id, consumer_group) VALUES ($1, $2) ON CONFLICT DO NOTHING",
		eventID, consumerGroup,
	)
	if err != nil {
		return false, fmt.Errorf("TryClaim: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("TryClaim: rowsAffected: %w", err)
	}
	return rows == 1, nil
}

func (r *ProcessedEventRepository) ReleaseClaim(ctx context.Context, eventID, consumerGroup string) error {
	_, err := r.db.ExecContext(ctx,
		"DELETE FROM processed_events WHERE event_id = $1 AND consumer_group = $2",
		eventID, consumerGroup,
	)
	if err != nil {
		return fmt.Errorf("ReleaseClaim: %w", err)
	}
	return nil
}
