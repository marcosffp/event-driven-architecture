package processor

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/marcosffp/event-driven-architecture/internal/domain"
	"github.com/marcosffp/event-driven-architecture/internal/kafka"
	"github.com/marcosffp/event-driven-architecture/internal/repository"
)

type reportProcessor struct{}

func (p *reportProcessor) Process(ctx context.Context, payload []byte) error {
	var enrollmentEvent domain.EnrollmentCreatedEvent
	if err := json.Unmarshal(payload, &enrollmentEvent); err != nil {
		return fmt.Errorf("reportProcessor: unmarshal: %w", err)
	}
	if enrollmentEvent.EnrollmentID == "" {
		return fmt.Errorf("reportProcessor: enrollment_id ausente")
	}

	log.Printf("[report] enrollment.created | id: %s | student: %s | course: %s | latência: %s",
		enrollmentEvent.EnrollmentID, enrollmentEvent.StudentID, enrollmentEvent.CourseID,
		time.Since(enrollmentEvent.PublishedAt))
	return nil
}

func RunReport(ctx context.Context, broker, dbURL string) {
	db := mustOpenDB(dbURL)
	defer db.Close()

	kafka.RunConsumer(ctx, kafka.ConsumerConfig{
		Broker:          broker,
		Topics:          []string{domain.TopicEnrollmentCreated},
		GroupID:         domain.GroupReport,
		Processor:       &reportProcessor{},
		IdempotencyRepo: repository.NewPostgresProcessedEventRepository(db),
		Publisher:       kafka.NewKafkaPublisher(broker),
	})
}
