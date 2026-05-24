package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/marcosffp/event-driven-architecture/internal/domain/port"
	"github.com/marcosffp/event-driven-architecture/internal/events"
)

const maxDLQRetries = 3

type DeadLetterProcessor struct {
	publisher port.EventPublisher
}

func NewDeadLetterProcessor(publisher port.EventPublisher) *DeadLetterProcessor {
	return &DeadLetterProcessor{publisher: publisher}
}

func (p *DeadLetterProcessor) Process(ctx context.Context, payload []byte) error {
	var dlqEvent events.DeadLetterEvent
	if err := json.Unmarshal(payload, &dlqEvent); err != nil {
		return fmt.Errorf("DeadLetterProcessor.Process: unmarshal: %w", err)
	}

	if dlqEvent.DLQRetryCount >= maxDLQRetries {
		log.Printf("[DLQ] EVENTO MORTO DEFINITIVAMENTE após %d tentativas | topic: %s | group: %s | event_id: %s | reason: %s",
			dlqEvent.DLQRetryCount, dlqEvent.OriginalTopic, dlqEvent.ConsumerGroup, dlqEvent.EventID, dlqEvent.FailureReason)
		return nil
	}

	nextRetry := dlqEvent.DLQRetryCount + 1
	backoff := time.Duration(nextRetry) * 10 * time.Second
	log.Printf("[DLQ] republicando para %s (tentativa %d/%d) aguardando %s | event_id: %s",
		dlqEvent.OriginalTopic, nextRetry, maxDLQRetries, backoff, dlqEvent.EventID)

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(backoff):
	}

	if err := p.publisher.PublishWithRetryHeader(ctx, dlqEvent.OriginalTopic, []byte(dlqEvent.OriginalPayload), nextRetry); err != nil {
		return fmt.Errorf("DeadLetterProcessor.Process: republish: %w", err)
	}
	return nil
}
