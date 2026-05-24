package postgres

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/marcosffp/event-driven-architecture/internal/domain"
)

type EnrollmentRepository struct {
	db *sql.DB
}

func NewEnrollmentRepository(db *sql.DB) *EnrollmentRepository {
	return &EnrollmentRepository{db: db}
}

func (r *EnrollmentRepository) Save(ctx context.Context, enrollment domain.Enrollment, event domain.OutboxEntry) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("Save: begin: %w", err)
	}
	defer tx.Rollback()

	if _, err = tx.ExecContext(ctx,
		"INSERT INTO enrollments (id, student_id, course_id, created_at) VALUES ($1, $2, $3, $4)",
		string(enrollment.ID), string(enrollment.StudentID), string(enrollment.CourseID), enrollment.CreatedAt,
	); err != nil {
		return fmt.Errorf("Save: insertEnrollment: %w", err)
	}

	if _, err = tx.ExecContext(ctx,
		"INSERT INTO outbox_events (id, topic, payload) VALUES ($1, $2, $3)",
		event.ID, event.Topic, event.Payload,
	); err != nil {
		return fmt.Errorf("Save: insertOutbox: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("Save: commit: %w", err)
	}
	return nil
}

func (r *EnrollmentRepository) FindByID(ctx context.Context, id domain.EnrollmentID) (domain.Enrollment, error) {
	var enrollment domain.Enrollment
	var enrollmentID, studentID, courseID string
	err := r.db.QueryRowContext(ctx,
		"SELECT id, student_id, course_id, created_at FROM enrollments WHERE id = $1",
		string(id),
	).Scan(&enrollmentID, &studentID, &courseID, &enrollment.CreatedAt)
	if err != nil {
		return domain.Enrollment{}, fmt.Errorf("FindByID: %w", err)
	}
	enrollment.ID = domain.EnrollmentID(enrollmentID)
	enrollment.StudentID = domain.StudentID(studentID)
	enrollment.CourseID = domain.CourseID(courseID)
	return enrollment, nil
}
