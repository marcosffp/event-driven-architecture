package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/marcosffp/event-driven-architecture/internal/events"
)

type ReportProcessor struct{}

func NewReportProcessor() *ReportProcessor {
	return &ReportProcessor{}
}

func (p *ReportProcessor) Process(ctx context.Context, payload []byte) error {
	var enrollmentEvent events.EnrollmentCreatedEvent
	if err := json.Unmarshal(payload, &enrollmentEvent); err != nil {
		return fmt.Errorf("ReportProcessor.Process: unmarshal: %w", err)
	}
	if enrollmentEvent.EnrollmentID == "" {
		return fmt.Errorf("ReportProcessor.Process: enrollment_id ausente")
	}

	log.Printf("[report] enrollment.created | id: %s | student: %s | course: %s | latência: %s",
		enrollmentEvent.EnrollmentID, enrollmentEvent.StudentID, enrollmentEvent.CourseID,
		time.Since(enrollmentEvent.PublishedAt))
	return nil
}
