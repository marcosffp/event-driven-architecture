package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/marcosffp/event-driven-architecture/internal/domain"
	kafkago "github.com/segmentio/kafka-go"
)

type EventProcessor interface {
	Process(ctx context.Context, payload []byte) error
}

type IdempotencyRepository interface {
	HasProcessed(ctx context.Context, eventID, consumerGroup string) (bool, error)
	MarkProcessed(ctx context.Context, eventID, consumerGroup string) error
}

type ConsumerConfig struct {
	Broker          string
	Topics          []string
	GroupID         string
	Processor       EventProcessor
	IdempotencyRepo IdempotencyRepository
	Publisher       Publisher
}

func RunConsumer(ctx context.Context, config ConsumerConfig) {
	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:     []string{config.Broker},
		GroupID:     config.GroupID,
		GroupTopics: config.Topics,
		MinBytes:    10e3,
		MaxBytes:    10e6,
		StartOffset: kafkago.FirstOffset,
	})
	defer reader.Close()

	for {
		message, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("[%s] fetchMessage: %v", config.GroupID, err)
			continue
		}

		if err := handleMessage(ctx, message, config); err != nil {
			log.Printf("[%s] handleMessage: %v", config.GroupID, err)
		}

		if err := reader.CommitMessages(ctx, message); err != nil {
			log.Printf("[%s] commitMessage: %v", config.GroupID, err)
		}
	}
}

func handleMessage(ctx context.Context, msg kafkago.Message, config ConsumerConfig) error {
	var extractor struct {
		EventID string `json:"event_id"`
	}
	if err := json.Unmarshal(msg.Value, &extractor); err != nil {
		return fmt.Errorf("unmarshalEventID: %w", err)
	}

	already, err := config.IdempotencyRepo.HasProcessed(ctx, extractor.EventID, config.GroupID)
	if err != nil {
		return fmt.Errorf("hasProcessed: %w", err)
	}
	if already {
		log.Printf("[%s] evento já processado, pulando: %s", config.GroupID, extractor.EventID)
		return nil
	}

	if processErr := processWithRetry(ctx, msg.Value, config.Processor); processErr != nil {
		publishDLQ(ctx, msg, config, processErr)
		return nil
	}

	if err := config.IdempotencyRepo.MarkProcessed(ctx, extractor.EventID, config.GroupID); err != nil {
		return fmt.Errorf("markProcessed: %w", err)
	}
	return nil
}

func processWithRetry(ctx context.Context, payload []byte, processor EventProcessor) error {
	delays := []time.Duration{1 * time.Second, 2 * time.Second, 4 * time.Second}
	var err error
	for attempt, delay := range delays {
		err = processor.Process(ctx, payload)
		if err == nil {
			return nil
		}
		log.Printf("retry %d/%d: %v", attempt+1, len(delays), err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
	}
	return err
}

func publishDLQ(ctx context.Context, msg kafkago.Message, config ConsumerConfig, reason error) {
	dlqEvent := domain.DeadLetterEvent{
		EventID:         fmt.Sprintf("dlq-%s-%d-%d", msg.Topic, msg.Partition, msg.Offset),
		OriginalTopic:   msg.Topic,
		ConsumerGroup:   config.GroupID,
		OriginalPayload: string(msg.Value),
		FailureReason:   reason.Error(),
		FailedAt:        time.Now(),
	}
	if err := config.Publisher.Publish(ctx, domain.TopicDeadLetter, dlqEvent); err != nil {
		log.Printf("[%s] publishDLQ: %v", config.GroupID, err)
	}
}
