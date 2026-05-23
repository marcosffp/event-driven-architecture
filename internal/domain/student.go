package domain

import "time"

type StudentID string

type Student struct {
	ID        StudentID `json:"id"`
	Name      string    `json:"name"`
	Email     string    `json:"email"`
	CreatedAt time.Time `json:"created_at"`
}
