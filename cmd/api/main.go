package main

import (
	"context"
	"log"
	"os"
	"os/signal"

	"github.com/gin-gonic/gin"
	_ "github.com/marcosffp/event-driven-architecture/docs"
	"github.com/marcosffp/event-driven-architecture/internal/infra/handler"
	"github.com/marcosffp/event-driven-architecture/internal/infra/kafka"
	"github.com/marcosffp/event-driven-architecture/internal/infra/postgres"
	"github.com/marcosffp/event-driven-architecture/internal/usecase"
	swaggerFiles "github.com/swaggo/files"
	ginSwagger "github.com/swaggo/gin-swagger"
)

// @title       Academic API
// @version     1.0
// @description Plataforma acadêmica orientada a eventos
// @host        localhost:8080
// @BasePath    /
func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	db, err := postgres.Open(os.Getenv("DATABASE_URL"))
	if err != nil {
		log.Fatalf("main: %v", err)
	}
	defer db.Close()

	publisher := kafka.NewPublisher(os.Getenv("KAFKA_BROKER"))
	defer publisher.Close()

	studentUseCase := usecase.NewStudentUseCase(postgres.NewStudentRepository(db))
	enrollmentUseCase := usecase.NewEnrollmentUseCase(postgres.NewEnrollmentRepository(db))

	relay := postgres.NewOutboxRelay(db, publisher)
	go relay.Run(ctx)

	router := gin.Default()
	router.POST("/students", handler.NewStudentHandler(studentUseCase).Register)
	router.POST("/enrollments", handler.NewEnrollmentHandler(enrollmentUseCase).Create)
	router.GET("/swagger/*any", ginSwagger.WrapHandler(swaggerFiles.Handler))

	log.Println("API listening on :8080")
	if err := router.Run(":8080"); err != nil {
		log.Fatalf("main: %v", err)
	}
}
