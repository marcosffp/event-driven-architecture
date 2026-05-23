package processor

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/marcosffp/event-driven-architecture/internal/domain"
	"github.com/marcosffp/event-driven-architecture/internal/kafka"
	"github.com/marcosffp/event-driven-architecture/internal/repository"
)

type dlqProcessor struct{}

func (p *dlqProcessor) Process(ctx context.Context, payload []byte) error {
	var dlqEvent domain.DeadLetterEvent
	if err := json.Unmarshal(payload, &dlqEvent); err != nil {
		return fmt.Errorf("dlqProcessor: unmarshal: %w", err)
	}

	log.Printf("[DLQ] ALERTA: evento morto | topic: %s | group: %s | reason: %s | event_id: %s",
		dlqEvent.OriginalTopic, dlqEvent.ConsumerGroup, dlqEvent.FailureReason, dlqEvent.EventID)
	return nil
}

func RunDeadLetter(ctx context.Context, broker, dbURL string) {
	db := mustOpenDB(dbURL)
	defer db.Close()

	kafka.RunConsumer(ctx, kafka.ConsumerConfig{
		Broker:          broker,
		Topics:          []string{domain.TopicDeadLetter},
		GroupID:         domain.GroupDLQ,
		Processor:       &dlqProcessor{},
		IdempotencyRepo: repository.NewPostgresProcessedEventRepository(db),
		Publisher:       kafka.NewKafkaPublisher(broker),
	})
}
