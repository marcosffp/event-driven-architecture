package port

import (
	"context"

	"github.com/marcosffp/event-driven-architecture/internal/domain"
)

type StudentRepository interface {
	Save(ctx context.Context, student domain.Student, event domain.OutboxEntry) error
	FindByID(ctx context.Context, id domain.StudentID) (domain.Student, error)
}

type EnrollmentRepository interface {
	Save(ctx context.Context, enrollment domain.Enrollment, event domain.OutboxEntry) error
	FindByID(ctx context.Context, id domain.EnrollmentID) (domain.Enrollment, error)
}
