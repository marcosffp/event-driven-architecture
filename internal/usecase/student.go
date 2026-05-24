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

type RegisterStudentInput struct {
	Name  string
	Email string
}

type StudentUseCase struct {
	studentRepository port.StudentRepository
}

func NewStudentUseCase(studentRepository port.StudentRepository) *StudentUseCase {
	return &StudentUseCase{studentRepository: studentRepository}
}

func (uc *StudentUseCase) Register(ctx context.Context, input RegisterStudentInput) (domain.Student, error) {
	student := domain.Student{
		ID:        domain.StudentID(uuid.New().String()),
		Name:      input.Name,
		Email:     input.Email,
		CreatedAt: time.Now(),
	}

	event := events.StudentRegisteredEvent{
		EventID:     uuid.New().String(),
		StudentID:   string(student.ID),
		Name:        student.Name,
		Email:       student.Email,
		PublishedAt: time.Now(),
	}

	payload, err := json.Marshal(event)
	if err != nil {
		return domain.Student{}, fmt.Errorf("Register: marshalEvent: %w", err)
	}

	outboxEntry := domain.OutboxEntry{
		ID:      uuid.New().String(),
		Topic:   events.TopicStudentRegistered,
		Payload: payload,
	}

	if err := uc.studentRepository.Save(ctx, student, outboxEntry); err != nil {
		return domain.Student{}, fmt.Errorf("Register: %w", err)
	}

	return student, nil
}
