package repository

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/marcosffp/event-driven-architecture/internal/domain"
)

type PostgresStudentRepository struct {
	db *sql.DB
}

func NewPostgresStudentRepository(db *sql.DB) *PostgresStudentRepository {
	return &PostgresStudentRepository{db: db}
}

func (r *PostgresStudentRepository) Save(ctx context.Context, student domain.Student) error {
	_, err := r.db.ExecContext(ctx,
		"INSERT INTO students (id, name, email, created_at) VALUES ($1, $2, $3, $4)",
		string(student.ID), student.Name, student.Email, student.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("saveStudent: %w", err)
	}
	return nil
}

func (r *PostgresStudentRepository) FindByID(ctx context.Context, id domain.StudentID) (domain.Student, error) {
	var student domain.Student
	var studentID string
	err := r.db.QueryRowContext(ctx,
		"SELECT id, name, email, created_at FROM students WHERE id = $1",
		string(id),
	).Scan(&studentID, &student.Name, &student.Email, &student.CreatedAt)
	if err != nil {
		return domain.Student{}, fmt.Errorf("findStudentByID: %w", err)
	}
	student.ID = domain.StudentID(studentID)
	return student, nil
}
