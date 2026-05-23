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
         │ 1. salva no banco
         ▼
    ┌──────────┐
    │ PostgreSQL│ students, enrollments, processed_events
    └──────────┘
         │ 2. publica evento (com EventID único)
         ▼
┌─────────────────────────────────────────────────────┐
│                       Kafka                         │
│  academic.student.registered    (3 partições)       │
│  academic.enrollment.created    (3 partições)       │
│  academic.events.dlq            (1 partição)        │
└───┬──────────────┬──────────────┬───────────────────┘
    │              │              │
    ▼              ▼              ▼
┌────────────┐ ┌────────────┐ ┌────────────┐
│ worker-    │ │ worker-    │ │ worker-    │
│notification│ │  audit     │ │  report    │
└────────────┘ └────────────┘ └────────────┘
      falha após 3 retries → worker-dlq
```

---

## Serviços (containers Docker)

| Container | Responsabilidade | Porta host |
|---|---|---|
| `api` | Recebe HTTP, salva no banco, publica no Kafka | `8080` |
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
| `academic.student.registered` | 3 | `api` | `academic-notification`, `academic-audit` |
| `academic.enrollment.created` | 3 | `api` | `academic-notification`, `academic-audit`, `academic-report` |
| `academic.events.dlq` | 1 | qualquer worker após esgotar retries | `academic-dlq` |

---

## Consumer Groups

| Group | Container | Tópicos | Função |
|---|---|---|---|
| `academic-notification` | `worker-notification` | `student.registered`, `enrollment.created` | Simula envio de e-mail ao aluno |
| `academic-audit` | `worker-audit` | `student.registered`, `enrollment.created` | Registra auditoria de todos os eventos |
| `academic-report` | `worker-report` | `enrollment.created` | Gera relatório de matrícula |
| `academic-dlq` | `worker-dlq` | `events.dlq` | Alerta de eventos mortos |

---

## Estrutura de Diretórios

```
.
├── cmd/
│   ├── api/
│   │   └── main.go              # injeta dependências, inicia HTTP server
│   └── worker/
│       └── main.go              # lê PROCESSOR env var, inicia o consumer group correspondente
│
├── internal/
│   ├── domain/
│   │   ├── student.go           # Student, StudentID
│   │   ├── enrollment.go        # Enrollment, EnrollmentID, CourseID
│   │   └── event.go             # eventos + constantes de tópicos e groups
│   │
│   ├── handler/
│   │   ├── student.go           # POST /students
│   │   └── enrollment.go        # POST /enrollments
│   │
│   ├── service/
│   │   ├── student.go           # RegisterStudent: salva → publica
│   │   └── enrollment.go        # CreateEnrollment: salva → publica
│   │
│   ├── kafka/
│   │   ├── topics.go            # constantes de tópicos e groups
│   │   ├── publisher.go         # serializa e escreve no Kafka
│   │   └── consumer.go          # base consumer: at-least-once + idempotência + retry + DLQ
│   │
│   ├── processor/
│   │   ├── notification.go      # RunNotification
│   │   ├── audit.go             # RunAudit
│   │   ├── report.go            # RunReport
│   │   └── dlq.go               # RunDeadLetter
│   │
│   └── repository/
│       ├── student.go           # Save, FindByID
│       ├── enrollment.go        # Save, FindByID
│       └── processed_event.go   # HasProcessed, MarkProcessed
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

### Regra de dependência entre pacotes

```
handler → service → repository/student
                  → repository/enrollment
                  → kafka/publisher

worker/main → processor/* → kafka/consumer (base)
                                  ├─ repository/processed_event
                                  └─ kafka/publisher (DLQ)
```

- `handler` nunca acessa `repository` diretamente
- `service` não conhece HTTP
- `processor` recebe `[]byte` — não importa `kafka` diretamente
- `kafka/consumer.go` é o único ponto que conhece Kafka, idempotência e DLQ

---

## Domínio (`internal/domain/event.go`)

```go
const (
    TopicStudentRegistered = "academic.student.registered"
    TopicEnrollmentCreated = "academic.enrollment.created"
    TopicDeadLetter        = "academic.events.dlq"

    GroupNotification = "academic-notification"
    GroupAudit        = "academic-audit"
    GroupReport       = "academic-report"
    GroupDLQ          = "academic-dlq"
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
    EventID         string    `json:"event_id"`
    OriginalTopic   string    `json:"original_topic"`
    ConsumerGroup   string    `json:"consumer_group"`
    OriginalPayload string    `json:"original_payload"`
    FailureReason   string    `json:"failure_reason"`
    FailedAt        time.Time `json:"failed_at"`
}
```

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
```

---

## cmd/worker/main.go

```go
func main() {
    ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
    defer cancel()

    broker := os.Getenv("KAFKA_BROKER")
    dbURL  := os.Getenv("DATABASE_URL")

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
```

---

## Docker Compose

```yaml
services:
  postgres:
    image: postgres:16-alpine
    environment:
      POSTGRES_DB: academic
      POSTGRES_USER: academic
      POSTGRES_PASSWORD: academic
    volumes:
      - ./migrations/001_init.sql:/docker-entrypoint-initdb.d/001_init.sql
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U academic"]
      interval: 5s
      retries: 10

  kafka:
    image: bitnami/kafka:3.7
    environment:
      KAFKA_CFG_NODE_ID: 1
      KAFKA_CFG_PROCESS_ROLES: broker,controller
      KAFKA_CFG_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093
      KAFKA_CFG_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092
      KAFKA_CFG_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
      KAFKA_CFG_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_CFG_AUTO_CREATE_TOPICS_ENABLE: "true"
      KAFKA_CFG_NUM_PARTITIONS: 3
    healthcheck:
      test: ["CMD-SHELL", "kafka-topics.sh --bootstrap-server localhost:9092 --list"]
      interval: 10s
      retries: 10

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

  api:
    build:
      context: .
      dockerfile: docker/api.Dockerfile
    ports:
      - "8080:8080"
    environment:
      KAFKA_BROKER: kafka:9092
      DATABASE_URL: postgres://academic:academic@postgres:5432/academic?sslmode=disable
    depends_on:
      kafka:
        condition: service_healthy
      postgres:
        condition: service_healthy

  worker-notification:
    build:
      context: .
      dockerfile: docker/worker.Dockerfile
    environment:
      PROCESSOR: notification
      KAFKA_BROKER: kafka:9092
      DATABASE_URL: postgres://academic:academic@postgres:5432/academic?sslmode=disable
    depends_on:
      kafka:
        condition: service_healthy
      postgres:
        condition: service_healthy

  worker-audit:
    build:
      context: .
      dockerfile: docker/worker.Dockerfile
    environment:
      PROCESSOR: audit
      KAFKA_BROKER: kafka:9092
      DATABASE_URL: postgres://academic:academic@postgres:5432/academic?sslmode=disable
    depends_on:
      kafka:
        condition: service_healthy
      postgres:
        condition: service_healthy

  worker-report:
    build:
      context: .
      dockerfile: docker/worker.Dockerfile
    environment:
      PROCESSOR: report
      KAFKA_BROKER: kafka:9092
      DATABASE_URL: postgres://academic:academic@postgres:5432/academic?sslmode=disable
    depends_on:
      kafka:
        condition: service_healthy
      postgres:
        condition: service_healthy

  worker-dlq:
    build:
      context: .
      dockerfile: docker/worker.Dockerfile
    environment:
      PROCESSOR: dlq
      KAFKA_BROKER: kafka:9092
      DATABASE_URL: postgres://academic:academic@postgres:5432/academic?sslmode=disable
    depends_on:
      kafka:
        condition: service_healthy
      postgres:
        condition: service_healthy
```

---

## Como usar este comando

- **Onde criar um arquivo?** Consulte a estrutura de diretórios
- **Como conectar componentes?** Consulte a regra de dependência entre pacotes
- **Como nomear um evento ou tópico?** Consulte `internal/domain/event.go`
- **Como escalar um processor?** `docker compose up --scale worker-<tipo>=N`

Use `/fluxo` para fluxos de dados, garantias de entrega, retry/DLQ, idempotência e demonstrações.
Use `/stack` para padrões de código Go e `/tp` para requisitos do trabalho.
