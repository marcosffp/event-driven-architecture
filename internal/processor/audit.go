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

type auditProcessor struct{}

func (p *auditProcessor) Process(ctx context.Context, payload []byte) error {
	var studentEvent domain.StudentRegisteredEvent
	if err := json.Unmarshal(payload, &studentEvent); err == nil && studentEvent.Email != "" {
		log.Printf("[audit] student.registered | id: %s | latência: %s",
			studentEvent.StudentID, time.Since(studentEvent.PublishedAt))
		return nil
	}

	var enrollmentEvent domain.EnrollmentCreatedEvent
	if err := json.Unmarshal(payload, &enrollmentEvent); err == nil && enrollmentEvent.EnrollmentID != "" {
		log.Printf("[audit] enrollment.created | id: %s | student: %s | latência: %s",
			enrollmentEvent.EnrollmentID, enrollmentEvent.StudentID, time.Since(enrollmentEvent.PublishedAt))
		return nil
	}

	return fmt.Errorf("auditProcessor: evento não reconhecido")
}

func RunAudit(ctx context.Context, broker, dbURL string) {
	db := mustOpenDB(dbURL)
	defer db.Close()

	kafka.RunConsumer(ctx, kafka.ConsumerConfig{
		Broker:          broker,
		Topics:          []string{domain.TopicStudentRegistered, domain.TopicEnrollmentCreated},
		GroupID:         domain.GroupAudit,
		Processor:       &auditProcessor{},
		IdempotencyRepo: repository.NewPostgresProcessedEventRepository(db),
		Publisher:       kafka.NewKafkaPublisher(broker),
	})
}
