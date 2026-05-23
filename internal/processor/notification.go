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

type notificationProcessor struct{}

func (p *notificationProcessor) Process(ctx context.Context, payload []byte) error {
	var studentEvent domain.StudentRegisteredEvent
	if err := json.Unmarshal(payload, &studentEvent); err == nil && studentEvent.Email != "" {
		log.Printf("[notification] student.registered | email: %s | latência: %s",
			studentEvent.Email, time.Since(studentEvent.PublishedAt))
		return nil
	}

	var enrollmentEvent domain.EnrollmentCreatedEvent
	if err := json.Unmarshal(payload, &enrollmentEvent); err == nil && enrollmentEvent.EnrollmentID != "" {
		log.Printf("[notification] enrollment.created | student: %s | course: %s | latência: %s",
			enrollmentEvent.StudentID, enrollmentEvent.CourseID, time.Since(enrollmentEvent.PublishedAt))
		return nil
	}

	return fmt.Errorf("notificationProcessor: evento não reconhecido")
}

func RunNotification(ctx context.Context, broker, dbURL string) {
	db := mustOpenDB(dbURL)
	defer db.Close()

	kafka.RunConsumer(ctx, kafka.ConsumerConfig{
		Broker:          broker,
		Topics:          []string{domain.TopicStudentRegistered, domain.TopicEnrollmentCreated},
		GroupID:         domain.GroupNotification,
		Processor:       &notificationProcessor{},
		IdempotencyRepo: repository.NewPostgresProcessedEventRepository(db),
		Publisher:       kafka.NewKafkaPublisher(broker),
	})
}
