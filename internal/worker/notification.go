package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/marcosffp/event-driven-architecture/internal/events"
)

type NotificationProcessor struct{}

func NewNotificationProcessor() *NotificationProcessor {
	return &NotificationProcessor{}
}

func (p *NotificationProcessor) Process(ctx context.Context, payload []byte) error {
	if err := simulatedFailure("NotificationProcessor"); err != nil {
		return err
	}

	var studentEvent events.StudentRegisteredEvent
	if err := json.Unmarshal(payload, &studentEvent); err == nil && studentEvent.Email != "" {
		log.Printf("[notification] student.registered | email: %s | latência: %s",
			studentEvent.Email, time.Since(studentEvent.PublishedAt))
		return nil
	}

	var enrollmentEvent events.EnrollmentCreatedEvent
	if err := json.Unmarshal(payload, &enrollmentEvent); err == nil && enrollmentEvent.EnrollmentID != "" {
		log.Printf("[notification] enrollment.created | student: %s | course: %s | latência: %s",
			enrollmentEvent.StudentID, enrollmentEvent.CourseID, time.Since(enrollmentEvent.PublishedAt))
		return nil
	}

	return fmt.Errorf("NotificationProcessor.Process: evento não reconhecido")
}
