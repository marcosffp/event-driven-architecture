package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
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

	checkConsumerLag(ctx, config)

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

func checkConsumerLag(ctx context.Context, config ConsumerConfig) {
	checkCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	conn, err := kafkago.DialContext(checkCtx, "tcp", config.Broker)
	if err != nil {
		log.Printf("[%s] lag check: não foi possível conectar: %v", config.GroupID, err)
		return
	}
	defer conn.Close()

	partitions, err := conn.ReadPartitions(config.Topics...)
	if err != nil {
		if isUnknownTopicError(err) {
			log.Printf("[%s] consumidor atualizado, sem mensagens pendentes", config.GroupID)
		} else {
			log.Printf("[%s] lag check: erro ao ler partições: %v", config.GroupID, err)
		}
		return
	}

	topicPartitions := make(map[string][]int)
	latestOffsets := make(map[string]map[int]int64)

	for _, p := range partitions {
		topicPartitions[p.Topic] = append(topicPartitions[p.Topic], p.ID)
		if latestOffsets[p.Topic] == nil {
			latestOffsets[p.Topic] = make(map[int]int64)
		}
		partConn, dialErr := kafkago.DialLeader(checkCtx, "tcp", config.Broker, p.Topic, p.ID)
		if dialErr != nil {
			continue
		}
		last, readErr := partConn.ReadLastOffset()
		partConn.Close()
		if readErr != nil {
			continue
		}
		latestOffsets[p.Topic][p.ID] = last
	}

	brokerAddr, resolveErr := net.ResolveTCPAddr("tcp", config.Broker)
	if resolveErr != nil {
		log.Printf("[%s] lag check: resolve addr: %v", config.GroupID, resolveErr)
		return
	}

	client := &kafkago.Client{
		Addr:    brokerAddr,
		Timeout: 10 * time.Second,
	}

	fetchResp, err := client.OffsetFetch(checkCtx, &kafkago.OffsetFetchRequest{
		GroupID: config.GroupID,
		Topics:  topicPartitions,
	})
	if err != nil {
		log.Printf("[%s] lag check: erro ao buscar offsets do grupo: %v", config.GroupID, err)
		return
	}

	totalLag := int64(0)
	for topicName, partFetches := range fetchResp.Topics {
		for _, pf := range partFetches {
			if pf.Error != nil {
				continue
			}
			committed := pf.CommittedOffset
			if committed < 0 {
				committed = 0
			}
			latest := latestOffsets[topicName][pf.Partition]
			lag := latest - committed
			if lag > 0 {
				totalLag += lag
			}
		}
	}

	if totalLag > 0 {
		log.Printf("[%s] *** RETOMANDO APÓS INDISPONIBILIDADE: %d mensagens acumuladas aguardando processamento ***", config.GroupID, totalLag)
	} else {
		log.Printf("[%s] consumidor atualizado, sem mensagens pendentes", config.GroupID)
	}
}

func isUnknownTopicError(err error) bool {
	return strings.Contains(err.Error(), "Unknown Topic Or Partition")
}
