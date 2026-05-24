package kafka

import (
	"context"
	"fmt"
	"strconv"
	"sync"

	kafkago "github.com/segmentio/kafka-go"
)

type Publisher struct {
	broker  string
	mu      sync.Mutex
	writers map[string]*kafkago.Writer
}

func NewPublisher(broker string) *Publisher {
	return &Publisher{
		broker:  broker,
		writers: make(map[string]*kafkago.Writer),
	}
}

func (p *Publisher) writerFor(topic string) *kafkago.Writer {
	p.mu.Lock()
	defer p.mu.Unlock()
	if w, ok := p.writers[topic]; ok {
		return w
	}
	w := &kafkago.Writer{
		Addr:     kafkago.TCP(p.broker),
		Topic:    topic,
		Balancer: &kafkago.LeastBytes{},
	}
	p.writers[topic] = w
	return w
}

func (p *Publisher) Publish(ctx context.Context, topic string, payload []byte) error {
	if err := p.writerFor(topic).WriteMessages(ctx, kafkago.Message{Value: payload}); err != nil {
		return fmt.Errorf("Publish: %w", err)
	}
	return nil
}

func (p *Publisher) PublishWithRetryHeader(ctx context.Context, topic string, payload []byte, dlqRetryCount int) error {
	msg := kafkago.Message{
		Value: payload,
		Headers: []kafkago.Header{
			{Key: "dlq-retry-count", Value: []byte(strconv.Itoa(dlqRetryCount))},
		},
	}
	if err := p.writerFor(topic).WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("PublishWithRetryHeader: %w", err)
	}
	return nil
}

func (p *Publisher) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, w := range p.writers {
		w.Close()
	}
}
