<!--
<div align="center" style="background:#1a1a2e;padding:32px 0;border-radius:12px">
<div align="center" style="background:#3fc3a7;padding:32px 80px;border-radius:12px;width:85%">
-->
<img width="1600" style="height:auto; border-radius: 12px;" alt="banner" src="docs/images/banner.png" />

# Academic — Plataforma Acadêmica Orientada a Eventos

> Trabalho Prático — Arquitetura de Software (5º período) · PUC Minas  
> Professor: Filipe Tório Lopes Ruas Nhimi

---

## Stack Principal

![Go](https://img.shields.io/badge/Go-1.23-00ADD8?style=for-the-badge&logo=go&logoColor=white)
![Kafka](https://img.shields.io/badge/Apache_Kafka-latest-231F20?style=for-the-badge&logo=apachekafka&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-4169E1?style=for-the-badge&logo=postgresql&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=for-the-badge&logo=docker&logoColor=white)
![Swagger](https://img.shields.io/badge/Swagger-UI-85EA2D?style=for-the-badge&logo=swagger&logoColor=black)

---

## Sumário

- [Sobre o projeto](#sobre-o-projeto)
- [Mensageria](#mensageria)
- [Arquitetura](#arquitetura)
- [Estrutura de pastas](#estrutura-de-pastas)
- [APIs e endpoints](#apis-e-endpoints)
- [Infraestrutura Docker](#infraestrutura-docker)
- [Variáveis de ambiente](#variáveis-de-ambiente)
- [Instalação e execução](#instalação-e-execução)

---

## Sobre o projeto

O **Academic** é uma plataforma acadêmica que demonstra, na prática, os conceitos de **Arquitetura Orientada a Eventos**. Toda ação principal — cadastrar um aluno ou criar uma matrícula — dispara eventos assíncronos no Kafka que são consumidos por múltiplos workers independentes, cada um com responsabilidade própria: notificação, auditoria, relatório e Dead Letter Queue.

O objetivo central não é apenas enviar mensagens, mas evidenciar como a **troca assíncrona de eventos reduz acoplamento, melhora escalabilidade e permite maior flexibilidade entre componentes** — produtor e consumidor nunca se conhecem diretamente.

---

## Mensageria

> A parte mais importante deste projeto é **como a mensageria foi projetada**: Outbox Pattern, idempotência atômica, retry com backoff exponencial, Dead Letter Queue com auto-reprocessamento e escalabilidade horizontal por partições Kafka.

**A documentação completa está em [`MESSAGING.md`](docs/MESSAGING.md)**, cobrindo:

- Fluxo completo passo a passo de cada requisição (com referência direta a cada arquivo)
- Os 5 cenários de falha e como o sistema se recupera de cada um
- Por que o Kafka entrega duplicatas e como o `TryClaim` atômico resolve
- Como o ciclo DLQ auto-reprocessa eventos até 12 vezes antes do descarte
- Escalabilidade horizontal e o limite de paralelismo por partições
- Tabela de trade-offs de cada decisão arquitetural

---

## Arquitetura

A aplicação segue **Clean Architecture** com separação estrita em camadas. A regra fundamental: a dependência sempre aponta para dentro — infra depende de usecase, usecase depende de domain, domain não depende de ninguém.

<img alt="Diagrama Arquitetural" src="docs/images/Arquitetura.jpeg">

**Regras de dependência por camada:**

| Camada | Pacote | Pode importar |
|---|---|---|
| Domain | `internal/domain` | Apenas stdlib |
| Events | `internal/events` | Apenas stdlib |
| Use Case | `internal/usecase` | `domain`, `events` |
| Worker | `internal/worker` | `events`, `domain/port` |
| Infra | `internal/infra/postgres` | `domain`, `domain/port` |
| Infra | `internal/infra/kafka` | `domain/port`, `events` |
| Infra | `internal/infra/handler` | `domain`, `usecase` |
| Composition Root | `cmd/` | Tudo |

---

## Estrutura de pastas

```
.
├── cmd/
│   ├── api/
│   │   └── main.go              ← wiring: DB + publisher + usecases + relay + router
│   └── worker/
│       └── main.go              ← switch em $PROCESSOR: notification|audit|report|dlq
├── docker/
│   ├── api.Dockerfile
│   └── worker.Dockerfile
├── docs/
│   ├── docs.go
│   ├── swagger.json
│   └── swagger.yaml
├── internal/
│   ├── domain/
│   │   ├── student.go           ← Student, StudentID
│   │   ├── enrollment.go        ← Enrollment, EnrollmentID, CourseID
│   │   ├── outbox.go            ← OutboxEntry (envelope do Outbox Pattern)
│   │   └── port/
│   │       ├── port.go          ← EventPublisher, IdempotencyRepository, OutboxRepository
│   │       └── repository.go    ← StudentRepository, EnrollmentRepository
│   ├── events/
│   │   └── event.go             ← StudentRegisteredEvent, EnrollmentCreatedEvent, DeadLetterEvent + constantes de tópicos
│   ├── usecase/
│   │   ├── student.go           ← StudentUseCase.Register
│   │   └── enrollment.go        ← EnrollmentUseCase.Create
│   ├── worker/
│   │   ├── notification.go      ← NotificationProcessor
│   │   ├── audit.go             ← AuditProcessor
│   │   ├── report.go            ← ReportProcessor
│   │   ├── dead_letter.go       ← DeadLetterProcessor (auto-reprocessamento DLQ)
│   │   └── demo.go              ← simulatedFailure (FAIL_RATE=1 para demo)
│   └── infra/
│       ├── handler/
│       │   ├── student_handler.go
│       │   ├── enrollment_handler.go
│       │   └── response.go
│       ├── kafka/
│       │   ├── publisher.go     ← Publisher com writers persistentes por tópico (sync.Mutex + map)
│       │   └── consumer.go      ← RunConsumer, handleMessage, processWithRetry, publishDeadLetter
│       └── postgres/
│           ├── connection.go
│           ├── student_repository.go      ← Save: transação (student + outbox_events)
│           ├── enrollment_repository.go   ← Save: transação (enrollment + outbox_events)
│           ├── outbox_relay.go            ← OutboxRelay.Run: polling 200ms → Kafka
│           └── processed_event_repository.go ← TryClaim + ReleaseClaim (idempotência)
├── migrations/
│   └── 001_init.sql
├── MESSAGING.md
├── .env.example
├── docker-compose.yml
├── go.mod
├── go.sum
└── Makefile
```

---

### `cmd/` — Entrypoints da aplicação

Cada binário em `cmd/` é um **composition root**: o único lugar do sistema que sabe quais implementações concretas existem. As camadas internas só enxergam interfaces.

#### `cmd/api/main.go`

1. Abre conexão com o Postgres via `postgres.Open()`
2. Instancia o `kafka.Publisher` (persistente, com `defer publisher.Close()`)
3. Cria `StudentUseCase` e `EnrollmentUseCase` injetando os repositórios Postgres
4. Instancia o `OutboxRelay` e o inicia como goroutine: `go relay.Run(ctx)`
5. Registra as rotas no Gin (`POST /students`, `POST /enrollments`, `GET /swagger/*any`)
6. Inicia o servidor na porta `8080`

#### `cmd/worker/main.go`

1. Abre conexão com o Postgres
2. Instancia `ProcessedEventRepository` (idempotência) e `kafka.Publisher` (necessário para DLQ)
3. Seleciona o processor via `switch os.Getenv("PROCESSOR")`
4. Chama `kafka.RunConsumer()` com a configuração montada

O mesmo binário `worker` serve os quatro tipos. O Docker Compose sobe quatro containers, cada um com um valor diferente de `PROCESSOR`.

---

### `internal/domain/` — Núcleo do sistema

Contém apenas structs e interfaces. **Zero importações externas** — apenas `time` e `context` da stdlib. Esta camada nunca sabe que Kafka ou Postgres existem.

#### `domain/student.go` e `domain/enrollment.go`

Definem `Student`, `StudentID`, `Enrollment`, `EnrollmentID` e `CourseID`. Os tipos de ID próprios evitam confusão de IDs em chamadas de função.

`student.go` também exporta `ErrEmailAlreadyTaken` — o repositório retorna esse erro ao detectar violação de `UNIQUE` no banco, e o handler o transforma em `409 Conflict`.

#### `domain/outbox.go`

```go
type OutboxEntry struct {
    ID      string
    Topic   string
    Payload []byte
}
```

Envelope do Outbox Pattern. O usecase cria um `OutboxEntry` e o passa junto com a entidade para o repositório — que os salva na mesma transação.

#### `domain/port/port.go` — Contratos de infraestrutura

```go
type EventPublisher interface {
    Publish(ctx context.Context, topic string, payload []byte) error
    PublishWithRetryHeader(ctx context.Context, topic string, payload []byte, dlqRetryCount int) error
}

type IdempotencyRepository interface {
    TryClaim(ctx context.Context, eventID, consumerGroup string) (bool, error)
    ReleaseClaim(ctx context.Context, eventID, consumerGroup string) error
}

type OutboxRepository interface {
    FetchUnpublished(ctx context.Context, limit int) ([]domain.OutboxEntry, error)
    MarkPublished(ctx context.Context, id string) error
}
```

`TryClaim` é a operação atômica de idempotência. `ReleaseClaim` é chamado antes de enviar para a DLQ, para que o ciclo de reprocessamento consiga reivindicar o evento de volta. `OutboxRepository` é usado pelo `OutboxRelay` para buscar e marcar eventos pendentes.

#### `domain/port/repository.go` — Contratos de persistência

```go
type StudentRepository interface {
    Save(ctx context.Context, student domain.Student, event domain.OutboxEntry) error
    FindByID(ctx context.Context, id domain.StudentID) (domain.Student, error)
}

type EnrollmentRepository interface {
    Save(ctx context.Context, enrollment domain.Enrollment, event domain.OutboxEntry) error
    FindByID(ctx context.Context, id domain.EnrollmentID) (domain.Enrollment, error)
}
```

O `Save` recebe tanto a entidade quanto o `OutboxEntry` — a implementação concreta salva os dois na mesma transação de banco.

---

### `internal/events/` — Contratos de mensagem

#### `events/event.go`

Define os structs publicados no Kafka e as constantes de tópico:

```go
const (
    TopicStudentRegistered = "academic.student.registered"
    TopicEnrollmentCreated = "academic.enrollment.created"
    TopicDeadLetter        = "academic.events.dlq"
)
```

Cada evento carrega um `event_id` único (UUID gerado no usecase) usado pelos workers para idempotência via `TryClaim`.

| Struct | Campos principais | Tópico |
|---|---|---|
| `StudentRegisteredEvent` | `event_id`, `student_id`, `name`, `email`, `published_at` | `student.registered` |
| `EnrollmentCreatedEvent` | `event_id`, `enrollment_id`, `student_id`, `course_id`, `published_at` | `enrollment.created` |
| `DeadLetterEvent` | `event_id`, `original_topic`, `consumer_group`, `original_payload`, `failure_reason`, `dlq_retry_count` | `events.dlq` |

---

### `internal/usecase/` — Lógica de negócio

Contém os casos de uso da API. **Não importa nenhum pacote de infra** — depende apenas de `domain` e `events`.

#### `usecase/student.go` — Cadastro de aluno

`StudentUseCase.Register` gera o `student.ID` (UUID), monta o `StudentRegisteredEvent`, serializa em JSON e cria um `OutboxEntry`. Chama `studentRepository.Save(ctx, student, outboxEntry)` — o repositório salva os dois numa transação atômica. Retorna o student para o handler serializar na resposta `201`.

#### `usecase/enrollment.go` — Criação de matrícula

Mesmo padrão: gera ID, monta `EnrollmentCreatedEvent`, cria `OutboxEntry` com tópico `enrollment.created`, chama `enrollmentRepository.Save`.

---

### `internal/worker/` — Processadores de eventos Kafka

Cada processor implementa `Process(ctx context.Context, payload []byte) error`. Não importa nenhum tipo Kafka — recebe apenas bytes e retorna erro. O `RunConsumer` do `infra/kafka` é quem lida com fetch, retry, DLQ e commit.

| Arquivo | Processor | Tópicos consumidos | O que faz |
|---|---|---|---|
| `notification.go` | `NotificationProcessor` | `student.registered`, `enrollment.created` | Detecta o tipo do evento pelo payload e loga email/course |
| `audit.go` | `AuditProcessor` | `student.registered`, `enrollment.created` | Loga o ID da entidade e a latência (`time.Since(published_at)`) |
| `report.go` | `ReportProcessor` | `enrollment.created` apenas | Loga dados completos da matrícula com latência |
| `dead_letter.go` | `DeadLetterProcessor` | `events.dlq` | Se `dlq_retry_count < 3`: aguarda backoff (10s/20s/30s) e republica no tópico original; senão, descarta |
| `demo.go` | — | — | Exporta `simulatedFailure`: retorna erro em todos os processadores quando `FAIL_RATE=1`, permitindo demonstrar retry e DLQ sem precisar de falha real |

---

### `internal/infra/handler/` — Camada HTTP

Handlers Gin. Cada handler define uma **interface local** para o use case que precisa — nunca dependendo do tipo concreto. Isso permite testar o handler sem precisar do usecase real.

`StudentHandler.Register` e `EnrollmentHandler.Create` seguem o mesmo padrão: validam o body com `ShouldBindJSON`, chamam o usecase e retornam `201` com a entidade criada.

---

### `internal/infra/kafka/` — Mensageria

#### `kafka/publisher.go`

O `Publisher` mantém um `map[string]*kafkago.Writer` protegido por `sync.Mutex`. Na primeira publicação para um tópico, cria um writer TCP persistente; nas seguintes, reutiliza. Isso evita o overhead de handshake TCP a cada mensagem.

Expõe dois métodos:
- `Publish` — publica sem headers extras
- `PublishWithRetryHeader` — adiciona o header `dlq-retry-count` (usado pelo ciclo de reprocessamento DLQ)

`Close()` é chamado via `defer` no `cmd/api/main.go` e `cmd/worker/main.go` para fechar todos os writers ao encerrar.

#### `kafka/consumer.go`

`RunConsumer` inicia com `checkConsumerLag`: conecta ao broker, compara os últimos offsets das partições com o offset commitado pelo consumer group e loga quantas mensagens se acumularam durante a indisponibilidade. Depois entra em loop infinito: `FetchMessage → handleMessage → CommitMessages`. O commit só acontece se `handleMessage` retornar `nil` — erros de infra não commitam o offset, garantindo reentrega pelo Kafka.

`handleMessage` executa o protocolo completo:

1. Extrai o `event_id` do payload JSON
2. Chama `TryClaim(eventID, groupID)` — se `rows=0` (já processado), pula silenciosamente
3. Chama `processWithRetry` com backoff exponencial (1s → 2s → 4s, 3 tentativas)
4. Se ainda falhar: chama `ReleaseClaim`, lê o header `dlq-retry-count` e publica em `events.dlq`
5. Se sucesso: retorna `nil` → offset é commitado

---

### `internal/infra/postgres/` — Persistência

#### `postgres/student_repository.go` e `enrollment_repository.go`

O `Save` de cada repositório abre uma transação, insere a entidade na sua tabela e insere o `OutboxEntry` em `outbox_events`, e faz COMMIT. Se qualquer INSERT falhar, tudo reverte — entidade e evento são sempre criados juntos ou nunca.

#### `postgres/outbox_relay.go`

`OutboxRelay.Run` roda como goroutine dentro da API e dispara a cada 200ms. A cada tick, abre uma transação, busca até 10 linhas com `published = false` usando `FOR UPDATE SKIP LOCKED` (múltiplas réplicas da API não processam a mesma linha), publica cada uma no Kafka via `Publisher.Publish`, e marca como `published = true`. Se o Kafka estiver fora, a transação reverte e as linhas são tentadas no próximo tick.

#### `postgres/processed_event_repository.go`

Implementa `port.IdempotencyRepository`:
- `TryClaim` — `INSERT INTO processed_events ... ON CONFLICT DO NOTHING`. Retorna `true` se inseriu (este worker foi o primeiro) ou `false` se já existia. Operação atômica — elimina race condition entre instâncias.
- `ReleaseClaim` — `DELETE FROM processed_events WHERE event_id = $1 AND consumer_group = $2`. Chamado antes de enviar para a DLQ.

---

### `migrations/001_init.sql` — Schema do banco

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

-- Registro atômico de idempotência por (event_id, consumer_group)
CREATE TABLE processed_events (
    event_id       VARCHAR(36) NOT NULL,
    consumer_group VARCHAR(64) NOT NULL,
    processed_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (event_id, consumer_group)
);

-- Fila transacional do Outbox Pattern
CREATE TABLE outbox_events (
    id          VARCHAR(36)  PRIMARY KEY,
    topic       VARCHAR(255) NOT NULL,
    payload     TEXT         NOT NULL,
    published   BOOLEAN      NOT NULL DEFAULT false,
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT now()
);
```

A chave composta em `processed_events` permite que grupos diferentes processem o mesmo evento, mas impede que o mesmo grupo processe duas vezes. A tabela `outbox_events` é a garantia de durabilidade do Outbox Pattern — o evento fica aqui até ser publicado no Kafka.

---

### `docker/` — Dockerfiles

Ambos usam build em duas etapas: **builder** (`golang:1.23-alpine`) compila o binário; **runtime** (`alpine:latest`) copia apenas o binário. A imagem final não tem Go instalado.

`api.Dockerfile` também executa `swag init` para gerar a documentação Swagger antes de compilar. `worker.Dockerfile` compila apenas o binário `worker` — o comportamento em runtime é controlado por `PROCESSOR`.

---

### `Makefile` — Atalhos de desenvolvimento

| Comando | O que faz |
|---|---|
| `make up` | `docker compose up --build` — sobe tudo |
| `make down` | Para e remove os containers |
| `make logs` | Segue os logs de todos os serviços em tempo real |
| `make tidy` | Roda `go mod tidy` no container de ferramentas |
| `make scale-notification n=3` | Escala `worker-notification` para `n` réplicas sem recriar os outros |

---

## APIs e endpoints

Documentação interativa disponível em `http://localhost:8080/swagger/index.html` após subir a aplicação.

### Alunos

| Método | Rota | Descrição | Body |
|---|---|---|---|
| `POST` | `/students` | Cadastra um novo aluno | `{ "name": "string", "email": "string" }` |

**Resposta 201:**
```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "name": "João Silva",
  "email": "joao@exemplo.com",
  "created_at": "2025-01-15T10:30:00Z"
}
```

### Matrículas

| Método | Rota | Descrição | Body |
|---|---|---|---|
| `POST` | `/enrollments` | Cria uma matrícula | `{ "student_id": "uuid", "course_id": "string" }` |

**Resposta 201:**
```json
{
  "id": "660e8400-e29b-41d4-a716-446655440001",
  "student_id": "550e8400-e29b-41d4-a716-446655440000",
  "course_id": "curso-eng-software",
  "created_at": "2025-01-15T10:31:00Z"
}
```

| Código | Quando |
|---|---|
| `400` | Body inválido ou campos obrigatórios ausentes |
| `409` | Email já cadastrado (`ErrEmailAlreadyTaken`) |
| `500` | Falha ao persistir no banco |

---

## Infraestrutura Docker

O `docker-compose.yml` sobe toda a infraestrutura. Nenhuma dependência além do Docker é necessária localmente.

| Serviço | Imagem | Porta | Função |
|---|---|---|---|
| `postgres` | `postgres:16-alpine` | interno | Banco de dados |
| `kafka` | `apache/kafka:latest` | interno | Broker de mensagens (KRaft, sem ZooKeeper) |
| `kafka-ui` | `provectuslabs/kafka-ui` | `8090` | Interface web para inspecionar tópicos, offsets e lag |
| `api` | build local | `8080` | Servidor HTTP + OutboxRelay |
| `worker-notification` | build local | — | `PROCESSOR=notification` |
| `worker-audit` | build local | — | `PROCESSOR=audit` |
| `worker-report` | build local | — | `PROCESSOR=report` |
| `worker-dlq` | build local | — | `PROCESSOR=dlq` |
| `go` | `golang:1.23-alpine` | — | Container de ferramentas (profile `tools`) |

Kafka configurado com `KAFKA_NUM_PARTITIONS: 3` e `KAFKA_AUTO_CREATE_TOPICS_ENABLE: true`. Todos os serviços aguardam os healthchecks de Postgres e Kafka antes de iniciar.

---

## Variáveis de ambiente

```bash
cp .env.example .env
```

| Variável | Exemplo | Descrição |
|---|---|---|
| `POSTGRES_DB` | `academic` | Nome do banco |
| `POSTGRES_USER` | `academic` | Usuário Postgres |
| `POSTGRES_PASSWORD` | `academic` | Senha Postgres |
| `DATABASE_URL` | `postgres://academic:academic@postgres:5432/academic?sslmode=disable` | URL de conexão completa |
| `API_PORT` | `8080` | Porta exposta pela API |
| `KAFKA_BROKER` | `kafka:9092` | Endereço do broker |
| `PROCESSOR` | `notification` | Tipo do worker: `notification`, `audit`, `report` ou `dlq` |
| `FAIL_RATE` | `` (vazio) | `1` faz todos os workers falharem 100% das mensagens — para demonstrar retry e DLQ em apresentações |

---

## Instalação e execução

**Pré-requisito:** Docker + Docker Compose.

```bash
# 1. Clone o repositório
git clone https://github.com/marcosffp/event-driven-architecture.git
cd event-driven-architecture

# 2. Configure o ambiente
cp .env.example .env

# 3. Suba toda a infraestrutura
make up
```

| Serviço | URL |
|---|---|
| API REST | `http://localhost:8080` |
| Swagger UI | `http://localhost:8080/swagger/index.html` |
| Kafka UI | `http://localhost:8090` |

**Testando o fluxo completo:**

```bash
# Cadastrar um aluno — dispara StudentRegisteredEvent
curl -s -X POST http://localhost:8080/students \
  -H "Content-Type: application/json" \
  -d '{"name": "João Silva", "email": "joao@exemplo.com"}' | jq

# Criar uma matrícula — dispara EnrollmentCreatedEvent
curl -s -X POST http://localhost:8080/enrollments \
  -H "Content-Type: application/json" \
  -d '{"student_id": "<id-retornado-acima>", "course_id": "curso-eng-software"}' | jq

# Acompanhar os workers em tempo real
make logs
```

```bash
# Escalar o worker de notificação para 3 instâncias
make scale-notification n=3
```

---

## Mensageria — documentação completa

Todo o design de mensageria deste projeto — Outbox Pattern, idempotência com `TryClaim` atômico, retry com backoff exponencial, Dead Letter Queue com auto-reprocessamento em ciclos, escalabilidade horizontal e trade-offs de cada decisão — está documentado em detalhe em:

### [`MESSAGING.md`](docs/MESSAGING.md)

---

<div align="center">
  <img width="70%" alt="pucminas" src="docs/images/banner-institucional.svg"/>
</div>
<p align="center">Fonte do banner: <a href="https://github.com/joaopauloaramuni">João Paulo Carneiro Aramuni</a></p>
