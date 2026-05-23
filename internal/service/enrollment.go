package service

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/marcosffp/event-driven-architecture/internal/domain"
	"github.com/marcosffp/event-driven-architecture/internal/kafka"
)

type EnrollmentRepository interface {
	Save(ctx context.Context, enrollment domain.Enrollment) error
	FindByID(ctx context.Context, id domain.EnrollmentID) (domain.Enrollment, error)
}

type EnrollmentService struct {
	enrollmentRepo EnrollmentRepository
	publisher      kafka.Publisher
}

func NewEnrollmentService(enrollmentRepo EnrollmentRepository, publisher kafka.Publisher) *EnrollmentService {
	return &EnrollmentService{enrollmentRepo: enrollmentRepo, publisher: publisher}
}

func (s *EnrollmentService) Create(ctx context.Context, enrollment domain.Enrollment) error {
	if err := s.enrollmentRepo.Save(ctx, enrollment); err != nil {
		return fmt.Errorf("createEnrollment: %w", err)
	}

	event := domain.EnrollmentCreatedEvent{
		EventID:      uuid.New().String(),
		EnrollmentID: string(enrollment.ID),
		StudentID:    string(enrollment.StudentID),
		CourseID:     string(enrollment.CourseID),
		PublishedAt:  time.Now(),
	}
	if err := s.publisher.Publish(ctx, domain.TopicEnrollmentCreated, event); err != nil {
		return fmt.Errorf("createEnrollment: publishEvent: %w", err)
	}
	return nil
}
