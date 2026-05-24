package port

import (
	"context"

	"github.com/marcosffp/event-driven-architecture/internal/domain"
)

type EventPublisher interface {
	Publish(ctx context.Context, topic string, payload []byte) error
	PublishWithRetryHeader(ctx context.Context, topic string, payload []byte, dlqRetryCount int) error
}

type IdempotencyRepository interface {
	TryClaim(ctx context.Context, eventID, consumerGroup string) (bool, error)
	ReleaseClaim(ctx context.Context, eventID, consumerGroup string) error
}

type OutboxRepository interface {
	FetchUnpublished(ctx context.Context, limit int) ([]domain.OutboxEntry, error)
	MarkPublished(ctx context.Context, id string) error
}
