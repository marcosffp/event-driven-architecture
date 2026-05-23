package domain

import "time"

type EnrollmentID string
type CourseID string

type Enrollment struct {
	ID        EnrollmentID `json:"id"`
	StudentID StudentID    `json:"student_id"`
	CourseID  CourseID     `json:"course_id"`
	CreatedAt time.Time    `json:"created_at"`
}
