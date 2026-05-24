# Arquitetura do Projeto

Fonte autoritativa sobre estrutura, componentes e organização do código. Use `/fluxo` para comportamento em runtime, garantias de entrega, demonstrações e trade-offs.

---

## Visão Geral

Plataforma acadêmica orientada a eventos. Ação principal (cadastro/matrícula) é síncrona. Tarefas secundárias (notificação, auditoria, relatório) são processadas de forma assíncrona por consumer groups independentes via Kafka.

```
Cliente HTTP
    │
    ▼
┌──────────────────┐
│       API        │  POST /students
│   (Go + HTTP)    │  POST /enrollments
└────────┬─────────┘
         │ 1. salva entidade + outbox_events (mesma transação)
         ▼
    ┌──────────┐
    │ PostgreSQL│ students, enrollments, outbox_events, processed_events
    └──────────┘
         │ 2. OutboxRelay goroutine (200ms) lê outbox e publica
         ▼
┌─────────────────────────────────────────────────────┐
│                       Kafka                         │
│  academic.student.registered    (3 partições)       │
│  academic.enrollment.created    (3 partições)       │
│  academic.events.dlq            (3 partições)       │
└───┬──────────────┬──────────────┬───────────────────┘
    │              │              │
    ▼              ▼              ▼
┌────────────┐ ┌────────────┐ ┌────────────┐
│ worker-    │ │ worker-    │ │ worker-    │
│notification│ │  audit     │ │  report    │
└────────────┘ └────────────┘ └────────────┘
      falha após 3 retries → worker-dlq (até 3 ciclos DLQ)
```

---

## Serviços (containers Docker)

| Container | Responsabilidade | Porta host |
|---|---|---|
| `api` | Recebe HTTP, salva no banco, roda OutboxRelay | `8080` |
| `worker-notification` | Consumer group `academic-notification` | — |
| `worker-audit` | Consumer group `academic-audit` | — |
| `worker-report` | Consumer group `academic-report` | — |
| `worker-dlq` | Consumer group `academic-dlq` | — |
| `kafka` | Broker em modo KRaft (sem Zookeeper) | — |
| `postgres` | Banco relacional | — |
| `kafka-ui` | Interface visual de tópicos e consumer lag | `8090` |

Todos os `worker-*` usam o **mesmo binário** (`cmd/worker`), diferenciados pela variável `PROCESSOR`. Escalar independentemente:

```bash
docker compose up --scale worker-notification=3 --scale worker-audit=1
```

---

## Tópicos Kafka

| Tópico | Partições | Publicado por | Consumer groups |
|---|---|---|---|
| `academic.student.registered` | 3 | `OutboxRelay` (via `api`) | `academic-notification`, `academic-audit` |
| `academic.enrollment.created` | 3 | `OutboxRelay` (via `api`) | `academic-notification`, `academic-audit`, `academic-report` |
| `academic.events.dlq` | 3 | qualquer worker após esgotar retries | `academic-dlq` |

---

## Consumer Groups

| Group | Container | Tópicos | Função |
|---|---|---|---|
| `academic-notification` | `worker-notification` | `student.registered`, `enrollment.created` | Simula envio de e-mail ao aluno |
| `academic-audit` | `worker-audit` | `student.registered`, `enrollment.created` | Registra auditoria de todos os eventos |
| `academic-report` | `worker-report` | `enrollment.created` | Gera relatório de matrícula |
| `academic-dlq` | `worker-dlq` | `events.dlq` | Reprocessa ou descarta eventos mortos |

---

## Estrutura de Diretórios

```
.
├── cmd/
│   ├── api/
│   │   └── main.go              # injeta dependências, inicia HTTP + OutboxRelay goroutine
│   └── worker/
│       └── main.go              # group constants, switch PROCESSOR, chama kafka.RunConsumer
│
├── internal/
│   ├── domain/
│   │   ├── student.go           # Student, StudentID
│   │   ├── enrollment.go        # Enrollment, EnrollmentID, CourseID
│   │   ├── outbox.go            # OutboxEntry (envelope para o outbox pattern)
│   │   └── port/
│   │       ├── repository.go    # StudentRepository, EnrollmentRepository
│   │       └── port.go          # EventPublisher, IdempotencyRepository, OutboxRepository
│   │
│   ├── events/
│   │   └── event.go             # StudentRegisteredEvent, EnrollmentCreatedEvent, DeadLetterEvent
│   │                            # + TopicStudentRegistered, TopicEnrollmentCreated, TopicDeadLetter
│   │
│   ├── usecase/
│   │   ├── student.go           # RegisterStudent: monta Student + OutboxEntry → salva em transação
│   │   └── enrollment.go        # CreateEnrollment: monta Enrollment + OutboxEntry → salva em transação
│   │
│   ├── worker/
│   │   ├── notification.go      # NotificationProcessor
│   │   ├── audit.go             # AuditProcessor
│   │   ├── report.go            # ReportProcessor
│   │   └── dead_letter.go       # DeadLetterProcessor (maxDLQRetries = 3)
│   │
│   └── infra/
│       ├── handler/
│       │   ├── student.go       # POST /students → chama usecase via interface local
│       │   └── enrollment.go    # POST /enrollments → chama usecase via interface local
│       ├── kafka/
│       │   ├── publisher.go     # Publisher: writers persistentes por tópico (sync.Mutex + map)
│       │   └── consumer.go      # RunConsumer + ConsumerConfig: at-least-once, TryClaim, retry, DLQ
│       └── postgres/
│           ├── student_repository.go          # Save (transação: student + outbox_events), FindByID
│           ├── enrollment_repository.go       # Save (transação: enrollment + outbox_events), FindByID
│           ├── processed_event_repository.go  # TryClaim, ReleaseClaim
│           ├── outbox_relay.go                # OutboxRelay: ticker 200ms, FOR UPDATE SKIP LOCKED
│           └── db.go                          # postgres.Open()
│
├── migrations/
│   └── 001_init.sql
│
├── docker/
│   ├── api.Dockerfile
│   └── worker.Dockerfile
│
├── docker-compose.yml
├── go.mod
└── go.sum
```

### Regra de dependência entre camadas

```
infra/handler → usecase (via interface local)
usecase       → domain + domain/port + events
worker        → events (só deserializa payload)
infra/kafka   → domain/port (EventPublisher, IdempotencyRepository) + events
infra/postgres → domain + domain/port
cmd/worker    → events + infra/kafka + infra/postgres + worker
cmd/api       → usecase + infra/handler + infra/kafka + infra/postgres
```

- `domain/` não importa nada externo — zero dependências externas
- `usecase/` não conhece HTTP nem Kafka
- `worker/` não importa `infra/kafka` nem `infra/postgres` — só recebe `[]byte`
- `infra/handler/` define interfaces locais (`studentRegistrar`, `enrollmentCreator`) — não importa tipos concretos de usecase

---

## Contratos de Eventos (`internal/events/event.go`)

```go
const (
    TopicStudentRegistered = "academic.student.registered"
    TopicEnrollmentCreated = "academic.enrollment.created"
    TopicDeadLetter        = "academic.events.dlq"
)

type StudentRegisteredEvent struct {
    EventID     string    `json:"event_id"`
    StudentID   string    `json:"student_id"`
    Name        string    `json:"name"`
    Email       string    `json:"email"`
    PublishedAt time.Time `json:"published_at"`
}

type EnrollmentCreatedEvent struct {
    EventID      string    `json:"event_id"`
    EnrollmentID string    `json:"enrollment_id"`
    StudentID    string    `json:"student_id"`
    CourseID     string    `json:"course_id"`
    PublishedAt  time.Time `json:"published_at"`
}

type DeadLetterEvent struct {
    EventID         string    `json:"event_id"`         // "dlq-{topic}-{partition}-{offset}"
    OriginalTopic   string    `json:"original_topic"`
    ConsumerGroup   string    `json:"consumer_group"`
    OriginalPayload string    `json:"original_payload"`
    FailureReason   string    `json:"failure_reason"`
    FailedAt        time.Time `json:"failed_at"`
    DLQRetryCount   int       `json:"dlq_retry_count"`
}
```

As constantes de consumer group (`groupNotification`, `groupAudit`, etc.) vivem em `cmd/worker/main.go` como constantes não exportadas — o único lugar que as usa.

---

## Schema (`migrations/001_init.sql`)

```sql
CREATE TABLE students (
    id         VARCHAR(36)  PRIMARY KEY,
    name       VARCHAR(255) NOT NULL,
    email      VARCHAR(255) NOT NULL UNIQUE,
    created_at TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE TABLE enrollments (
    id         VARCHAR(36)  PRIMARY KEY,
    student_id VARCHAR(36)  NOT NULL REFERENCES students(id),
    course_id  VARCHAR(36)  NOT NULL,
    created_at TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE TABLE processed_events (
    event_id       VARCHAR(36)  NOT NULL,
    consumer_group VARCHAR(64)  NOT NULL,
    processed_at   TIMESTAMPTZ  NOT NULL DEFAULT now(),
    PRIMARY KEY (event_id, consumer_group)
);

CREATE TABLE outbox_events (
    id         VARCHAR(36)  PRIMARY KEY,
    topic      VARCHAR(255) NOT NULL,
    payload    TEXT         NOT NULL,
    published  BOOLEAN      NOT NULL DEFAULT false,
    created_at TIMESTAMPTZ  NOT NULL DEFAULT now()
);
```

A PK composta `(event_id, consumer_group)` em `processed_events` permite que diferentes groups processem o mesmo evento de forma independente.

---

## cmd/worker/main.go

```go
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
```

---

## Docker Compose

```yaml
name: academic

services:
  postgres:
    image: postgres:16-alpine
    environment:
      POSTGRES_DB: ${POSTGRES_DB}
      POSTGRES_USER: ${POSTGRES_USER}
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
    volumes:
      - postgres-data:/var/lib/postgresql/data
      - ./migrations/001_init.sql:/docker-entrypoint-initdb.d/001_init.sql
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U ${POSTGRES_USER}"]
      interval: 5s
      retries: 10
    networks:
      - academic-net

  kafka:
    image: apache/kafka:latest
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
      KAFKA_NUM_PARTITIONS: 3
    healthcheck:
      test: ["CMD-SHELL", "/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list || exit 1"]
      interval: 10s
      retries: 10
    networks:
      - academic-net

  kafka-ui:
    image: provectuslabs/kafka-ui:latest
    ports:
      - "8090:8080"
    environment:
      KAFKA_CLUSTERS_0_NAME: academic
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka:9092
    depends_on:
      kafka:
        condition: service_healthy
    networks:
      - academic-net

  api:
    build:
      context: .
      dockerfile: docker/api.Dockerfile
    ports:
      - "${API_PORT:-8080}:8080"
    environment:
      KAFKA_BROKER: kafka:9092
      DATABASE_URL: ${DATABASE_URL}
    depends_on:
      kafka:
        condition: service_healthy
      postgres:
        condition: service_healthy
    networks:
      - academic-net

  worker-notification:
    build:
      context: .
      dockerfile: docker/worker.Dockerfile
    environment:
      PROCESSOR: notification
      KAFKA_BROKER: kafka:9092
      DATABASE_URL: ${DATABASE_URL}
    depends_on:
      kafka:
        condition: service_healthy
      postgres:
        condition: service_healthy
    networks:
      - academic-net

  # worker-audit, worker-report, worker-dlq: mesma estrutura, PROCESSOR diferente

volumes:
  postgres-data:
  go-mod-cache:
  go-build-cache:

networks:
  academic-net:
    driver: bridge
```

---

## Como usar este comando

- **Onde criar um arquivo?** Consulte a estrutura de diretórios
- **Como conectar componentes?** Consulte a regra de dependência entre camadas
- **Como nomear um evento ou tópico?** Consulte `internal/events/event.go`
- **Como escalar um processor?** `docker compose up --scale worker-<tipo>=N`

Use `/fluxo` para fluxos de dados, garantias de entrega, retry/DLQ, idempotência e demonstrações.
Use `/stack` para padrões de código Go e `/tp` para requisitos do trabalho.
