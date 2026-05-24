package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/marcosffp/event-driven-architecture/internal/events"
)

type AuditProcessor struct{}

func NewAuditProcessor() *AuditProcessor {
	return &AuditProcessor{}
}

func (p *AuditProcessor) Process(ctx context.Context, payload []byte) error {
	var studentEvent events.StudentRegisteredEvent
	if err := json.Unmarshal(payload, &studentEvent); err == nil && studentEvent.Email != "" {
		log.Printf("[audit] student.registered | id: %s | latência: %s",
			studentEvent.StudentID, time.Since(studentEvent.PublishedAt))
		return nil
	}

	var enrollmentEvent events.EnrollmentCreatedEvent
	if err := json.Unmarshal(payload, &enrollmentEvent); err == nil && enrollmentEvent.EnrollmentID != "" {
		log.Printf("[audit] enrollment.created | id: %s | student: %s | latência: %s",
			enrollmentEvent.EnrollmentID, enrollmentEvent.StudentID, time.Since(enrollmentEvent.PublishedAt))
		return nil
	}

	return fmt.Errorf("AuditProcessor.Process: evento não reconhecido")
}
