# Stack e Padrões de Código

Este comando define as tecnologias e padrões de código do projeto. Para estrutura de diretórios, componentes e fluxo de dados consulte `/arquitetura`. Toda implementação deve seguir estas diretrizes sem exceção.

---

## Stack Tecnológica

| Camada | Tecnologia | Justificativa |
|---|---|---|
| **Back-end** | Go (Golang) | Concorrência nativa com goroutines, tipagem estática, binário único, sem runtime pesado |
| **Mensageria** | Apache Kafka | Persistência por offset, múltiplos consumer groups independentes, reprocessamento — cobre todos os requisitos do trabalho |
| **Banco de dados** | PostgreSQL | Relacional, transações ACID, ampla adoção |
| **Containerização** | Docker + Docker Compose | Ambiente 100% isolado — o único pré-requisito na máquina é o **Docker** |
| **Observabilidade** | Kafka UI (`provectuslabs/kafka-ui`) | Visualização de tópicos, mensagens acumuladas e consumer lag — essencial para a demonstração |

> Toda a stack roda com `docker compose up --build`. Nenhum desenvolvedor instala Go, Kafka ou PostgreSQL localmente.

### Biblioteca Kafka para Go

**`segmentio/kafka-go`** — pure Go, sem CGO, funciona em Alpine sem dependências extras.

```
go get github.com/segmentio/kafka-go
```

---

## Containerização — Regras

- **Nenhum serviço roda fora do Docker.** Go, Kafka, PostgreSQL — tudo em container.
- `docker compose up --build` sobe o ambiente completo sem etapas manuais.
- Cada serviço tem seu próprio container no Compose.
- Usar **multi-stage build** para manter imagens Go mínimas.
- Configurações via variáveis de ambiente — sem hardcode no código.
- Expor apenas as portas necessárias para o host (`8080` para API, `8090` para Kafka UI).

### Dockerfile padrão para serviços Go

```dockerfile
FROM golang:1.23-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o bin/service ./cmd/api

FROM alpine:latest
WORKDIR /app
COPY --from=builder /app/bin/service .
ENTRYPOINT ["./service"]
```

---

## Padrões de Código Go

### Regra absoluta: zero comentários

**Nenhum comentário no código.** Sem `//`, sem `/* */`, sem godoc, sem nada.

Se sentiu necessidade de escrever um comentário, o código está errado. Refatore: renomeie a função, quebre em funções menores, extraia um tipo com nome mais claro.

> Comentário é sintoma de código mal nomeado ou mal estruturado.

### Nomenclatura

- **Funções e métodos:** verbos que descrevem a ação — `RegisterStudent`, `PublishStudentRegistered`, `RunNotification`, `RunAudit`
- **Tipos e structs:** substantivos no singular — `Student`, `Enrollment`, `StudentRegisteredEvent`, `DeadLetterEvent`
- **Variáveis:** descritivas, sem abreviações — `studentID` não `sid`, `enrollmentEvent` não `ev`
- **Packages:** lowercase, sem underline, singular — `handler`, `kafka`, `repository`, `domain`, `processor`
- **Constantes:** PascalCase quando exportadas — `TopicStudentRegistered`, `TopicEnrollmentCreated`, `TopicDeadLetter`
- **Interfaces:** sufixo `er` — `Publisher`, `StudentRepository`, `EnrollmentRepository`

### Funções

- Uma função faz uma única coisa.
- Mais de 25 linhas é sinal de responsabilidade demais — divida.
- Retorne `error` explicitamente — nunca ignore com `_` em código de produção.
- `context.Context` como primeiro parâmetro em qualquer função que faz I/O.

### Tratamento de erros

Sempre propague contexto:

```go
student, err := r.db.FindByID(ctx, studentID)
if err != nil {
    return fmt.Errorf("registerStudent: %w", err)
}
```

- Nunca use `panic` para fluxos normais.
- Nunca silencie erros.

### Tipos de domínio

Evite `string` e `int` soltos. Defina tipos explícitos:

```go
type StudentID string
type CourseID string
type EnrollmentID string

type Student struct {
    ID        StudentID
    Name      string
    Email     string
    CreatedAt time.Time
}
```

### Interfaces

Defina no pacote que **consome**, não no que implementa. Mantenha interfaces pequenas:

```go
type Publisher interface {
    Publish(ctx context.Context, topic string, event any) error
}

type StudentRepository interface {
    Save(ctx context.Context, student domain.Student) error
    FindByID(ctx context.Context, id domain.StudentID) (domain.Student, error)
}
```

Os processors (`notification`, `audit`, `report`, `dlq`) recebem o evento já desserializado — eles não dependem de Kafka diretamente. A interface de cada processor é definida em `kafka/consumer.go`:

```go
type EventProcessor interface {
    Process(ctx context.Context, payload []byte) error
}
```

---

## Kafka no projeto

O projeto usa **quatro consumer groups independentes**, cada um em seu próprio container, escalável individualmente:

| Consumer group | Container | Tópicos consumidos | Função |
|---|---|---|---|
| `academic-notification` | `worker-notification` | `student.registered`, `enrollment.created` | Notificação ao aluno |
| `academic-audit` | `worker-audit` | `student.registered`, `enrollment.created` | Auditoria de todos os eventos |
| `academic-report` | `worker-report` | `enrollment.created` | Geração de relatório de matrícula |
| `academic-dlq` | `worker-dlq` | `events.dlq` | Alerta de eventos mortos |

Todos os containers usam o mesmo binário `cmd/worker`, diferenciados pela variável `PROCESSOR`.

### At-least-once — regra obrigatória

`auto-commit` é **desabilitado**. O offset só é commitado após o processamento ser concluído com sucesso. Nunca commite antes de processar.

Ordem obrigatória em `kafka/consumer.go`:
1. Recebe mensagem
2. Verifica idempotência
3. Processa
4. Registra em `processed_events`
5. Commita offset

### Idempotência — regra obrigatória

Todo evento tem `EventID string` (UUID). Antes de processar, verificar `processed_events(event_id, consumer_group)`. Se já existe, commitar offset e pular. Isso garante segurança em reprocessamentos por crash, retry ou rebalanceamento de partição.

### Retry e DLQ

A lógica vive em `kafka/consumer.go` e é compartilhada por todos os groups:

- 3 retries com backoff exponencial: 1s → 2s → 4s
- Após esgotar retries: publica `DeadLetterEvent` em `academic.events.dlq` e commita offset
- A partição nunca trava por uma mensagem problemática

### Latência

Todo evento carrega `PublishedAt time.Time`. Cada processor loga `ProcessedAt - PublishedAt` para demonstrar o tempo de ponta a ponta na apresentação.

---

## O que nunca fazer

- Qualquer comentário no código
- Hardcode de endereços, senhas ou portas
- Ignorar erros retornados por funções
- Nomes de variável de uma letra (exceto `i` em loops simples)
- Lógica de negócio dentro de handlers HTTP
- Dependência direta entre `api` e `worker` — comunicação exclusivamente via Kafka
- Um único consumer fazendo tudo — cada responsabilidade tem seu próprio consumer group
- Commitar offset antes de processar — isso quebra a garantia at-least-once
- Criar eventos sem `EventID` — sem ele idempotência é impossível
- Usar `auto-commit` do Kafka — o commit deve ser manual e explícito após sucesso

---

## Como usar este comando

Ao invocar `/stack`, aplique estas diretrizes como lei para toda implementação:

1. Há algum comentário? Remova e renomeie o que causou a necessidade.
2. O ambiente sobe com `docker compose up --build` sem etapas manuais?
3. A nomenclatura comunica intenção sem precisar de explicação?
4. Erros são tratados e propagados com contexto?
5. Cada consumer group tem uma única responsabilidade?

Use em conjunto com `/tp` (requisitos), `/fundamentos` (teoria da aula) e `/arquitetura` (estrutura e fluxo do projeto).
