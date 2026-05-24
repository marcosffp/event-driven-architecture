package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	"github.com/marcosffp/event-driven-architecture/internal/domain"
	"github.com/marcosffp/event-driven-architecture/internal/domain/port"
)

type OutboxRelay struct {
	db        *sql.DB
	publisher port.EventPublisher
}

func NewOutboxRelay(db *sql.DB, publisher port.EventPublisher) *OutboxRelay {
	return &OutboxRelay{db: db, publisher: publisher}
}

func (r *OutboxRelay) Run(ctx context.Context) {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := r.publish(ctx); err != nil {
				log.Printf("[OutboxRelay] publish: %v", err)
			}
		}
	}
}

func (r *OutboxRelay) publish(ctx context.Context) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("publish: begin: %w", err)
	}
	defer tx.Rollback()

	rows, err := tx.QueryContext(ctx,
		`SELECT id, topic, payload FROM outbox_events
		 WHERE published = false
		 ORDER BY created_at
		 LIMIT 10
		 FOR UPDATE SKIP LOCKED`,
	)
	if err != nil {
		return fmt.Errorf("publish: query: %w", err)
	}

	var entries []domain.OutboxEntry
	for rows.Next() {
		var e domain.OutboxEntry
		if err := rows.Scan(&e.ID, &e.Topic, &e.Payload); err != nil {
			rows.Close()
			return fmt.Errorf("publish: scan: %w", err)
		}
		entries = append(entries, e)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return fmt.Errorf("publish: rows: %w", err)
	}

	for _, entry := range entries {
		if err := r.publisher.Publish(ctx, entry.Topic, entry.Payload); err != nil {
			return fmt.Errorf("publish: broker[%s]: %w", entry.Topic, err)
		}
		if _, err := tx.ExecContext(ctx,
			"UPDATE outbox_events SET published = true WHERE id = $1",
			entry.ID,
		); err != nil {
			return fmt.Errorf("publish: markPublished: %w", err)
		}
	}

	return tx.Commit()
}
