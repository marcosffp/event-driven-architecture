package usecase

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/marcosffp/event-driven-architecture/internal/domain"
	"github.com/marcosffp/event-driven-architecture/internal/domain/port"
	"github.com/marcosffp/event-driven-architecture/internal/events"
)

type CreateEnrollmentInput struct {
	StudentID string
	CourseID  string
}

type EnrollmentUseCase struct {
	enrollmentRepository port.EnrollmentRepository
}

func NewEnrollmentUseCase(enrollmentRepository port.EnrollmentRepository) *EnrollmentUseCase {
	return &EnrollmentUseCase{enrollmentRepository: enrollmentRepository}
}

func (uc *EnrollmentUseCase) Create(ctx context.Context, input CreateEnrollmentInput) (domain.Enrollment, error) {
	enrollment := domain.Enrollment{
		ID:        domain.EnrollmentID(uuid.New().String()),
		StudentID: domain.StudentID(input.StudentID),
		CourseID:  domain.CourseID(input.CourseID),
		CreatedAt: time.Now(),
	}

	event := events.EnrollmentCreatedEvent{
		EventID:      uuid.New().String(),
		EnrollmentID: string(enrollment.ID),
		StudentID:    string(enrollment.StudentID),
		CourseID:     string(enrollment.CourseID),
		PublishedAt:  time.Now(),
	}

	payload, err := json.Marshal(event)
	if err != nil {
		return domain.Enrollment{}, fmt.Errorf("Create: marshalEvent: %w", err)
	}

	outboxEntry := domain.OutboxEntry{
		ID:      uuid.New().String(),
		Topic:   events.TopicEnrollmentCreated,
		Payload: payload,
	}

	if err := uc.enrollmentRepository.Save(ctx, enrollment, outboxEntry); err != nil {
		return domain.Enrollment{}, fmt.Errorf("Create: %w", err)
	}

	return enrollment, nil
}
