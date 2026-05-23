package repository

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/marcosffp/event-driven-architecture/internal/domain"
)

type PostgresEnrollmentRepository struct {
	db *sql.DB
}

func NewPostgresEnrollmentRepository(db *sql.DB) *PostgresEnrollmentRepository {
	return &PostgresEnrollmentRepository{db: db}
}

func (r *PostgresEnrollmentRepository) Save(ctx context.Context, enrollment domain.Enrollment) error {
	_, err := r.db.ExecContext(ctx,
		"INSERT INTO enrollments (id, student_id, course_id, created_at) VALUES ($1, $2, $3, $4)",
		string(enrollment.ID), string(enrollment.StudentID), string(enrollment.CourseID), enrollment.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("saveEnrollment: %w", err)
	}
	return nil
}

func (r *PostgresEnrollmentRepository) FindByID(ctx context.Context, id domain.EnrollmentID) (domain.Enrollment, error) {
	var enrollment domain.Enrollment
	var enrollmentID, studentID, courseID string
	err := r.db.QueryRowContext(ctx,
		"SELECT id, student_id, course_id, created_at FROM enrollments WHERE id = $1",
		string(id),
	).Scan(&enrollmentID, &studentID, &courseID, &enrollment.CreatedAt)
	if err != nil {
		return domain.Enrollment{}, fmt.Errorf("findEnrollmentByID: %w", err)
	}
	enrollment.ID = domain.EnrollmentID(enrollmentID)
	enrollment.StudentID = domain.StudentID(studentID)
	enrollment.CourseID = domain.CourseID(courseID)
	return enrollment, nil
}
