package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/marcosffp/event-driven-architecture/internal/domain/port"
	"github.com/marcosffp/event-driven-architecture/internal/events"
	kafkago "github.com/segmentio/kafka-go"
)

type EventProcessor interface {
	Process(ctx context.Context, payload []byte) error
}

type ConsumerConfig struct {
	Broker                string
	Topics                []string
	GroupID               string
	Processor             EventProcessor
	IdempotencyRepository port.IdempotencyRepository
	Publisher             port.EventPublisher
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
			log.Printf("[%s] handleMessage: %v — mensagem não commitada, será reprocessada", config.GroupID, err)
			continue
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
		return fmt.Errorf("handleMessage: unmarshalEventID: %w", err)
	}

	claimed, err := config.IdempotencyRepository.TryClaim(ctx, extractor.EventID, config.GroupID)
	if err != nil {
		return fmt.Errorf("handleMessage: tryClaim: %w", err)
	}
	if !claimed {
		log.Printf("[%s] evento já processado, pulando: %s", config.GroupID, extractor.EventID)
		return nil
	}

	if processErr := processWithRetry(ctx, msg.Value, config.Processor); processErr != nil {
		if releaseErr := config.IdempotencyRepository.ReleaseClaim(ctx, extractor.EventID, config.GroupID); releaseErr != nil {
			log.Printf("[%s] releaseClaim: %v", config.GroupID, releaseErr)
		}
		dlqRetryCount := readDLQRetryCount(msg.Headers)
		publishDeadLetter(ctx, msg, config, processErr, dlqRetryCount)
		return nil
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

func readDLQRetryCount(headers []kafkago.Header) int {
	for _, h := range headers {
		if h.Key == "dlq-retry-count" {
			n, _ := strconv.Atoi(string(h.Value))
			return n
		}
	}
	return 0
}

func publishDeadLetter(ctx context.Context, msg kafkago.Message, config ConsumerConfig, reason error, dlqRetryCount int) {
	event := events.DeadLetterEvent{
		EventID:         fmt.Sprintf("dlq-%s-%d-%d", msg.Topic, msg.Partition, msg.Offset),
		OriginalTopic:   msg.Topic,
		ConsumerGroup:   config.GroupID,
		OriginalPayload: string(msg.Value),
		FailureReason:   reason.Error(),
		FailedAt:        time.Now(),
		DLQRetryCount:   dlqRetryCount,
	}
	payload, err := json.Marshal(event)
	if err != nil {
		log.Printf("[%s] publishDeadLetter: marshal: %v", config.GroupID, err)
		return
	}
	if err := config.Publisher.Publish(ctx, events.TopicDeadLetter, payload); err != nil {
		log.Printf("[%s] publishDeadLetter: %v", config.GroupID, err)
	}
}
