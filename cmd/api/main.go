package main

import (
	"database/sql"
	"log"
	"os"

	"github.com/gin-gonic/gin"
	_ "github.com/lib/pq"
	_ "github.com/marcosffp/event-driven-architecture/docs"
	"github.com/marcosffp/event-driven-architecture/internal/handler"
	"github.com/marcosffp/event-driven-architecture/internal/kafka"
	"github.com/marcosffp/event-driven-architecture/internal/repository"
	"github.com/marcosffp/event-driven-architecture/internal/service"
	swaggerFiles "github.com/swaggo/files"
	ginSwagger "github.com/swaggo/gin-swagger"
)

// @title       Academic API
// @version     1.0
// @description Plataforma acadêmica orientada a eventos
// @host        localhost:8080
// @BasePath    /
func main() {
	db, err := sql.Open("postgres", os.Getenv("DATABASE_URL"))
	if err != nil {
		log.Fatalf("openDB: %v", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		log.Fatalf("pingDB: %v", err)
	}

	publisher := kafka.NewKafkaPublisher(os.Getenv("KAFKA_BROKER"))

	studentService := service.NewStudentService(repository.NewPostgresStudentRepository(db), publisher)
	enrollmentService := service.NewEnrollmentService(repository.NewPostgresEnrollmentRepository(db), publisher)

	router := gin.Default()
	router.POST("/students", handler.NewStudentHandler(studentService).RegisterStudent)
	router.POST("/enrollments", handler.NewEnrollmentHandler(enrollmentService).CreateEnrollment)
	router.GET("/swagger/*any", ginSwagger.WrapHandler(swaggerFiles.Handler))

	log.Println("API listening on :8080")
	if err := router.Run(":8080"); err != nil {
		log.Fatalf("runServer: %v", err)
	}
}
