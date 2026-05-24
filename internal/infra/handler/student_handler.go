package handler

import (
	"context"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/marcosffp/event-driven-architecture/internal/domain"
	"github.com/marcosffp/event-driven-architecture/internal/usecase"
)

type studentRegistrar interface {
	Register(ctx context.Context, input usecase.RegisterStudentInput) (domain.Student, error)
}

type StudentHandler struct {
	studentUseCase studentRegistrar
}

func NewStudentHandler(studentUseCase studentRegistrar) *StudentHandler {
	return &StudentHandler{studentUseCase: studentUseCase}
}

type registerStudentRequest struct {
	Name  string `json:"name"  binding:"required"`
	Email string `json:"email" binding:"required"`
}

func (h *StudentHandler) Register(c *gin.Context) {
	var body registerStudentRequest
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, errorResponse{Error: "invalid request body"})
		return
	}

	student, err := h.studentUseCase.Register(c.Request.Context(), usecase.RegisterStudentInput{
		Name:  body.Name,
		Email: body.Email,
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, errorResponse{Error: err.Error()})
		return
	}

	c.JSON(http.StatusCreated, student)
}
