package service

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/marcosffp/event-driven-architecture/internal/domain"
	"github.com/marcosffp/event-driven-architecture/internal/kafka"
)

type StudentRepository interface {
	Save(ctx context.Context, student domain.Student) error
	FindByID(ctx context.Context, id domain.StudentID) (domain.Student, error)
}

type StudentService struct {
	studentRepo StudentRepository
	publisher   kafka.Publisher
}

func NewStudentService(studentRepo StudentRepository, publisher kafka.Publisher) *StudentService {
	return &StudentService{studentRepo: studentRepo, publisher: publisher}
}

func (s *StudentService) Register(ctx context.Context, student domain.Student) error {
	if err := s.studentRepo.Save(ctx, student); err != nil {
		return fmt.Errorf("registerStudent: %w", err)
	}

	event := domain.StudentRegisteredEvent{
		EventID:     uuid.New().String(),
		StudentID:   string(student.ID),
		Name:        student.Name,
		Email:       student.Email,
		PublishedAt: time.Now(),
	}
	if err := s.publisher.Publish(ctx, domain.TopicStudentRegistered, event); err != nil {
		return fmt.Errorf("registerStudent: publishEvent: %w", err)
	}
	return nil
}
