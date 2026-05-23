package handler

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/marcosffp/event-driven-architecture/internal/domain"
	"github.com/marcosffp/event-driven-architecture/internal/service"
)

type registerStudentRequest struct {
	Name  string `json:"name"  binding:"required"`
	Email string `json:"email" binding:"required"`
}

type StudentHandler struct {
	studentService *service.StudentService
}

func NewStudentHandler(studentService *service.StudentService) *StudentHandler {
	return &StudentHandler{studentService: studentService}
}

// @Summary  Cadastrar aluno
// @Tags     students
// @Accept   json
// @Produce  json
// @Param    body body registerStudentRequest true "Dados do aluno"
// @Success  201 {object} domain.Student
// @Failure  400 {object} errorResponse
// @Failure  500 {object} errorResponse
// @Router   /students [post]
func (h *StudentHandler) RegisterStudent(c *gin.Context) {
	var body registerStudentRequest
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, errorResponse{Error: "invalid request body"})
		return
	}

	student := domain.Student{
		ID:        domain.StudentID(uuid.New().String()),
		Name:      body.Name,
		Email:     body.Email,
		CreatedAt: time.Now(),
	}

	if err := h.studentService.Register(c.Request.Context(), student); err != nil {
		c.JSON(http.StatusInternalServerError, errorResponse{Error: err.Error()})
		return
	}

	c.JSON(http.StatusCreated, student)
}
