package domain

import "time"

const (
	TopicStudentRegistered = "academic.student.registered"
	TopicEnrollmentCreated = "academic.enrollment.created"
	TopicDeadLetter        = "academic.events.dlq"

	GroupNotification = "academic-notification"
	GroupAudit        = "academic-audit"
	GroupReport       = "academic-report"
	GroupDLQ          = "academic-dlq"
)

type StudentRegisteredEvent struct {
	EventID     string    `json:"event_id"`
	StudentID   string    `json:"student_id"`
	Name        string    `json:"name"`
	Email       string    `json:"email"`
	PublishedAt time.Time `json:"published_at"`
}

type EnrollmentCreatedEvent struct {
	EventID      string    `json:"event_id"`
	EnrollmentID string    `json:"enrollment_id"`
	StudentID    string    `json:"student_id"`
	CourseID     string    `json:"course_id"`
	PublishedAt  time.Time `json:"published_at"`
}

type DeadLetterEvent struct {
	EventID         string    `json:"event_id"`
	OriginalTopic   string    `json:"original_topic"`
	ConsumerGroup   string    `json:"consumer_group"`
	OriginalPayload string    `json:"original_payload"`
	FailureReason   string    `json:"failure_reason"`
	FailedAt        time.Time `json:"failed_at"`
}
