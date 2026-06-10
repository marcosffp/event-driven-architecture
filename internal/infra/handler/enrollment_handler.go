package handler

import (
	"context"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/marcosffp/event-driven-architecture/internal/domain"
	"github.com/marcosffp/event-driven-architecture/internal/usecase"
)

type enrollmentCreator interface {
	Create(ctx context.Context, input usecase.CreateEnrollmentInput) (domain.Enrollment, error)
}

type EnrollmentHandler struct {
	enrollmentUseCase enrollmentCreator
}

func NewEnrollmentHandler(enrollmentUseCase enrollmentCreator) *EnrollmentHandler {
	return &EnrollmentHandler{enrollmentUseCase: enrollmentUseCase}
}

type createEnrollmentRequest struct {
	StudentID string `json:"student_id" binding:"required"`
	CourseID  string `json:"course_id"  binding:"required"`
}

// Create godoc
// @Summary      Criar matrícula
// @Tags         enrollments
// @Accept       json
// @Produce      json
// @Param        body body createEnrollmentRequest true "Dados da matrícula"
// @Success      201 {object} domain.Enrollment
// @Failure      400 {object} errorResponse
// @Failure      500 {object} errorResponse
// @Router       /enrollments [post]
func (h *EnrollmentHandler) Create(c *gin.Context) {
	var body createEnrollmentRequest
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, errorResponse{Error: "invalid request body"})
		return
	}

	enrollment, err := h.enrollmentUseCase.Create(c.Request.Context(), usecase.CreateEnrollmentInput{
		StudentID: body.StudentID,
		CourseID:  body.CourseID,
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, errorResponse{Error: err.Error()})
		return
	}

	c.JSON(http.StatusCreated, enrollment)
}
