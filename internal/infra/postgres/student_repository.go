package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/lib/pq"
	"github.com/marcosffp/event-driven-architecture/internal/domain"
)

type StudentRepository struct {
	db *sql.DB
}

func NewStudentRepository(db *sql.DB) *StudentRepository {
	return &StudentRepository{db: db}
}

func (r *StudentRepository) Save(ctx context.Context, student domain.Student, event domain.OutboxEntry) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("Save: begin: %w", err)
	}
	defer tx.Rollback()

	if _, err = tx.ExecContext(ctx,
		"INSERT INTO students (id, name, email, created_at) VALUES ($1, $2, $3, $4)",
		string(student.ID), student.Name, student.Email, student.CreatedAt,
	); err != nil {
		var pqErr *pq.Error
		if errors.As(err, &pqErr) && pqErr.Code == "23505" {
			return fmt.Errorf("Save: %w", domain.ErrEmailAlreadyTaken)
		}
		return fmt.Errorf("Save: insertStudent: %w", err)
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

func (r *StudentRepository) FindByID(ctx context.Context, id domain.StudentID) (domain.Student, error) {
	var student domain.Student
	var studentID string
	err := r.db.QueryRowContext(ctx,
		"SELECT id, name, email, created_at FROM students WHERE id = $1",
		string(id),
	).Scan(&studentID, &student.Name, &student.Email, &student.CreatedAt)
	if err != nil {
		return domain.Student{}, fmt.Errorf("FindByID: %w", err)
	}
	student.ID = domain.StudentID(studentID)
	return student, nil
}
