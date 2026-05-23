package kafka

import (
	"context"
	"encoding/json"
	"fmt"

	kafkago "github.com/segmentio/kafka-go"
)

type Publisher interface {
	Publish(ctx context.Context, topic string, event any) error
}

type KafkaPublisher struct {
	broker string
}

func NewKafkaPublisher(broker string) *KafkaPublisher {
	return &KafkaPublisher{broker: broker}
}

func (p *KafkaPublisher) Publish(ctx context.Context, topic string, event any) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("publishEvent: marshal: %w", err)
	}

	writer := &kafkago.Writer{
		Addr:     kafkago.TCP(p.broker),
		Topic:    topic,
		Balancer: &kafkago.LeastBytes{},
	}
	defer writer.Close()

	if err := writer.WriteMessages(ctx, kafkago.Message{Value: payload}); err != nil {
		return fmt.Errorf("publishEvent: write: %w", err)
	}
	return nil
}
