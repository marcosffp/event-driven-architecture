package domain

import (
	"errors"
	"time"
)

var ErrEmailAlreadyTaken = errors.New("email already registered")

type StudentID string

type Student struct {
	ID        StudentID `json:"id"`
	Name      string    `json:"name"`
	Email     string    `json:"email"`
	CreatedAt time.Time `json:"created_at"`
}
