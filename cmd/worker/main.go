package main

import (
	"context"
	"log"
	"os"
	"os/signal"

	"github.com/marcosffp/event-driven-architecture/internal/processor"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	broker := os.Getenv("KAFKA_BROKER")
	dbURL := os.Getenv("DATABASE_URL")

	switch os.Getenv("PROCESSOR") {
	case "notification":
		processor.RunNotification(ctx, broker, dbURL)
	case "audit":
		processor.RunAudit(ctx, broker, dbURL)
	case "report":
		processor.RunReport(ctx, broker, dbURL)
	case "dlq":
		processor.RunDeadLetter(ctx, broker, dbURL)
	default:
		log.Fatalf("PROCESSOR obrigatório: notification | audit | report | dlq")
	}
}
