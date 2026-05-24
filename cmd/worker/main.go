package main

import (
	"context"
	"log"
	"os"
	"os/signal"

	"github.com/marcosffp/event-driven-architecture/internal/events"
	"github.com/marcosffp/event-driven-architecture/internal/infra/kafka"
	"github.com/marcosffp/event-driven-architecture/internal/infra/postgres"
	"github.com/marcosffp/event-driven-architecture/internal/worker"
)

const (
	groupNotification = "academic-notification"
	groupAudit        = "academic-audit"
	groupReport       = "academic-report"
	groupDLQ          = "academic-dlq"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	broker := os.Getenv("KAFKA_BROKER")

	db, err := postgres.Open(os.Getenv("DATABASE_URL"))
	if err != nil {
		log.Fatalf("main: %v", err)
	}
	defer db.Close()

	idempotencyRepository := postgres.NewProcessedEventRepository(db)
	publisher := kafka.NewPublisher(broker)

	switch os.Getenv("PROCESSOR") {
	case "notification":
		kafka.RunConsumer(ctx, kafka.ConsumerConfig{
			Broker:                broker,
			Topics:                []string{events.TopicStudentRegistered, events.TopicEnrollmentCreated},
			GroupID:               groupNotification,
			Processor:             worker.NewNotificationProcessor(),
			IdempotencyRepository: idempotencyRepository,
			Publisher:             publisher,
		})
	case "audit":
		kafka.RunConsumer(ctx, kafka.ConsumerConfig{
			Broker:                broker,
			Topics:                []string{events.TopicStudentRegistered, events.TopicEnrollmentCreated},
			GroupID:               groupAudit,
			Processor:             worker.NewAuditProcessor(),
			IdempotencyRepository: idempotencyRepository,
			Publisher:             publisher,
		})
	case "report":
		kafka.RunConsumer(ctx, kafka.ConsumerConfig{
			Broker:                broker,
			Topics:                []string{events.TopicEnrollmentCreated},
			GroupID:               groupReport,
			Processor:             worker.NewReportProcessor(),
			IdempotencyRepository: idempotencyRepository,
			Publisher:             publisher,
		})
	case "dlq":
		kafka.RunConsumer(ctx, kafka.ConsumerConfig{
			Broker:                broker,
			Topics:                []string{events.TopicDeadLetter},
			GroupID:               groupDLQ,
			Processor:             worker.NewDeadLetterProcessor(publisher),
			IdempotencyRepository: idempotencyRepository,
			Publisher:             publisher,
		})
	default:
		log.Fatalf("PROCESSOR obrigatório: notification | audit | report | dlq")
	}
}
