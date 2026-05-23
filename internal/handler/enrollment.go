package handler

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/marcosffp/event-driven-architecture/internal/domain"
	"github.com/marcosffp/event-driven-architecture/internal/service"
)

type createEnrollmentRequest struct {
	StudentID string `json:"student_id" binding:"required"`
	CourseID  string `json:"course_id"  binding:"required"`
}

type EnrollmentHandler struct {
	enrollmentService *service.EnrollmentService
}

func NewEnrollmentHandler(enrollmentService *service.EnrollmentService) *EnrollmentHandler {
	return &EnrollmentHandler{enrollmentService: enrollmentService}
}

// @Summary  Criar matrícula
// @Tags     enrollments
// @Accept   json
// @Produce  json
// @Param    body body createEnrollmentRequest true "Dados da matrícula"
// @Success  201 {object} domain.Enrollment
// @Failure  400 {object} errorResponse
// @Failure  500 {object} errorResponse
// @Router   /enrollments [post]
func (h *EnrollmentHandler) CreateEnrollment(c *gin.Context) {
	var body createEnrollmentRequest
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, errorResponse{Error: "invalid request body"})
		return
	}

	enrollment := domain.Enrollment{
		ID:        domain.EnrollmentID(uuid.New().String()),
		StudentID: domain.StudentID(body.StudentID),
		CourseID:  domain.CourseID(body.CourseID),
		CreatedAt: time.Now(),
	}

	if err := h.enrollmentService.Create(c.Request.Context(), enrollment); err != nil {
		c.JSON(http.StatusInternalServerError, errorResponse{Error: err.Error()})
		return
	}

	c.JSON(http.StatusCreated, enrollment)
}
