# Mensageria — Fluxos, Falhas e Escalabilidade

Este documento explica em detalhe como a mensageria funciona neste projeto: arquitetura de camadas, contratos entre componentes, fluxos completos passo a passo (com referência direta aos arquivos reais), cenários de falha, idempotência e escalabilidade. O objetivo é que qualquer pessoa consiga entender **o que acontece, por que, e onde no código**.

---

## Sumário

- [Estrutura geral do projeto](#estrutura-geral-do-projeto)
- [Camadas da arquitetura limpa](#camadas-da-arquitetura-limpa)
- [Contratos — interfaces e eventos](#contratos--interfaces-e-eventos)
- [Tópicos e consumer groups](#tópicos-e-consumer-groups)
- [Fluxo feliz — POST /students](#fluxo-feliz--post-students)
- [Fluxo feliz — POST /enrollments](#fluxo-feliz--post-enrollments)
- [OutboxRelay — por dentro](#outboxrelay--por-dentro)
- [Cenários de falha](#cenários-de-falha)
  - [Worker fora do ar](#1-worker-fora-do-ar)
  - [Falha no processamento — retry com backoff](#2-falha-no-processamento--retry-com-backoff)
  - [Falha persistente — Dead Letter Queue](#3-falha-persistente--dead-letter-queue)
  - [Banco indisponível no worker](#4-banco-indisponível-no-worker)
  - [API morre após salvar — Outbox Pattern](#5-api-morre-após-salvar--outbox-pattern)
- [Idempotência — TryClaim atômico](#idempotência--tryclaim-atômico)
  - [Por que o Kafka entrega duplicatas](#por-que-o-kafka-entrega-duplicatas)
  - [Como o TryClaim resolve](#como-o-tryclaim-resolve)
  - [Race condition entre instâncias do mesmo worker](#race-condition-entre-instâncias-do-mesmo-worker)
- [Escalabilidade horizontal](#escalabilidade-horizontal)
- [Consistência eventual](#consistência-eventual)
- [Observando o sistema](#observando-o-sistema)
- [Resumo dos trade-offs](#resumo-dos-trade-offs)

---

## Estrutura geral do projeto

```
cmd/
  api/main.go          ← ponto de entrada da API (HTTP + OutboxRelay)
  worker/main.go       ← ponto de entrada dos workers (branching por PROCESSOR env)

internal/
  domain/              ← modelos de negócio e interfaces (sem dependência de infra)
    student.go
    enrollment.go
    outbox.go
    port/
      port.go          ← interfaces EventPublisher, IdempotencyRepository, OutboxRepository
      repository.go    ← interfaces StudentRepository, EnrollmentRepository

  events/
    event.go           ← structs dos eventos Kafka + constantes de tópicos

  usecase/
    student.go         ← StudentUseCase.Register: cria student + OutboxEntry na transação
    enrollment.go      ← EnrollmentUseCase.Create: cria enrollment + OutboxEntry na transação

  infra/
    handler/
      student_handler.go    ← HTTP POST /students
      enrollment_handler.go ← HTTP POST /enrollments
      response.go           ← errorResponse struct
    kafka/
      publisher.go     ← Publisher: escreve mensagens no Kafka (writers persistentes por tópico)
      consumer.go      ← RunConsumer, handleMessage, processWithRetry, publishDeadLetter
    postgres/
      connection.go           ← postgres.Open (sql.DB)
      student_repository.go   ← StudentRepository.Save (transação: student + outbox)
      enrollment_repository.go← EnrollmentRepository.Save (transação: enrollment + outbox)
      outbox_relay.go         ← OutboxRelay.Run: polling a cada 200ms, publica outbox no Kafka
      processed_event_repository.go ← TryClaim + ReleaseClaim (idempotência)

  worker/
    notification.go    ← NotificationProcessor
    audit.go           ← AuditProcessor
    report.go          ← ReportProcessor
    dead_letter.go     ← DeadLetterProcessor (auto-reprocessamento da DLQ)

migrations/
  001_init.sql         ← tabelas: students, enrollments, outbox_events, processed_events

docker/
  api.Dockerfile
  worker.Dockerfile

docker-compose.yml     ← orquestra postgres, kafka, kafka-ui, api, worker-*
```

---

## Camadas da arquitetura limpa

A regra de dependência vai de fora para dentro: infra depende de usecase, usecase depende de domain. **Domain não importa nada de infra.**

```
┌─────────────────────────────────────────────────────────────────┐
│  cmd/api/main.go  |  cmd/worker/main.go                         │ ← wiring (DI manual)
└─────────────────────────────────────────────────────────────────┘
          │ instancia e conecta
          ▼
┌─────────────────────────────────────────────────────────────────┐
│  internal/infra/                                                  │ ← adaptadores concretos
│    handler/  (Gin HTTP)                                           │
│    kafka/    (segmentio/kafka-go)                                 │
│    postgres/ (database/sql + lib/pq)                             │
└─────────────────────────────────────────────────────────────────┘
          │ chama via interface
          ▼
┌─────────────────────────────────────────────────────────────────┐
│  internal/usecase/                                                │ ← regras de negócio
│    StudentUseCase, EnrollmentUseCase                             │
└─────────────────────────────────────────────────────────────────┘
          │ importa apenas
          ▼
┌─────────────────────────────────────────────────────────────────┐
│  internal/domain/                                                 │ ← modelos + interfaces
│    Student, Enrollment, OutboxEntry                               │
│    port.StudentRepository, port.EnrollmentRepository             │
│    port.EventPublisher, port.IdempotencyRepository               │
└─────────────────────────────────────────────────────────────────┘
```

**Por que isso importa para a mensageria:** o `StudentUseCase` não conhece Kafka, Postgres ou Gin. Ele só chama `studentRepository.Save(ctx, student, outboxEntry)` — a interface `port.StudentRepository`. Quem implementa essa interface é `postgres.StudentRepository`, que executa a transação de banco. Isso significa que trocar o broker de Kafka por RabbitMQ não tocaria em nada no usecase.

---

## Contratos — interfaces e eventos

### Interfaces (`internal/domain/port/`)

**`port.go`** define os contratos de infraestrutura que o usecase e o consumer precisam:

```go
// port.EventPublisher — implementado por infra/kafka/Publisher
type EventPublisher interface {
    Publish(ctx context.Context, topic string, payload []byte) error
    PublishWithRetryHeader(ctx context.Context, topic string, payload []byte, dlqRetryCount int) error
}

// port.IdempotencyRepository — implementado por infra/postgres/ProcessedEventRepository
type IdempotencyRepository interface {
    TryClaim(ctx context.Context, eventID, consumerGroup string) (bool, error)
    ReleaseClaim(ctx context.Context, eventID, consumerGroup string) error
}
```

**`repository.go`** define os contratos de persistência das entidades:

```go
// port.StudentRepository — implementado por infra/postgres/StudentRepository
type StudentRepository interface {
    Save(ctx context.Context, student domain.Student, event domain.OutboxEntry) error
    FindByID(ctx context.Context, id domain.StudentID) (domain.Student, error)
}

// port.EnrollmentRepository — implementado por infra/postgres/EnrollmentRepository
type EnrollmentRepository interface {
    Save(ctx context.Context, enrollment domain.Enrollment, event domain.OutboxEntry) error
    FindByID(ctx context.Context, id domain.EnrollmentID) (domain.Enrollment, error)
}
```

### Eventos Kafka (`internal/events/event.go`)

Cada evento tem um `event_id` único (UUID gerado no usecase) que o consumer usa para idempotência:

```go
const (
    TopicStudentRegistered = "academic.student.registered"
    TopicEnrollmentCreated = "academic.enrollment.created"
    TopicDeadLetter        = "academic.events.dlq"
)

// Publicado em academic.student.registered quando POST /students tem sucesso
type StudentRegisteredEvent struct {
    EventID     string    `json:"event_id"`
    StudentID   string    `json:"student_id"`
    Name        string    `json:"name"`
    Email       string    `json:"email"`
    PublishedAt time.Time `json:"published_at"`
}

// Publicado em academic.enrollment.created quando POST /enrollments tem sucesso
type EnrollmentCreatedEvent struct {
    EventID      string    `json:"event_id"`
    EnrollmentID string    `json:"enrollment_id"`
    StudentID    string    `json:"student_id"`
    CourseID     string    `json:"course_id"`
    PublishedAt  time.Time `json:"published_at"`
}

// Publicado em academic.events.dlq quando um evento falha após 3 retries
type DeadLetterEvent struct {
    EventID         string    `json:"event_id"`         // "dlq-{topic}-{partition}-{offset}"
    OriginalTopic   string    `json:"original_topic"`
    ConsumerGroup   string    `json:"consumer_group"`
    OriginalPayload string    `json:"original_payload"` // payload original intacto (string JSON)
    FailureReason   string    `json:"failure_reason"`
    FailedAt        time.Time `json:"failed_at"`
    DLQRetryCount   int       `json:"dlq_retry_count"`  // quantas vezes já passou pela DLQ
}
```

**Contrato entre API e workers:** apenas o formato JSON acima. A API não sabe quantos workers existem. Adicionar um novo worker não requer nenhuma mudança na API.

---

## Tópicos e consumer groups

| Tópico | Quando é publicado | Partições |
|---|---|---|
| `academic.student.registered` | `POST /students` com sucesso | 3 |
| `academic.enrollment.created` | `POST /enrollments` com sucesso | 3 |
| `academic.events.dlq` | Um evento falha após 3 retries no worker | 3 |

| Consumer Group | Tópicos consumidos | Arquivo do processador |
|---|---|---|
| `academic-notification` | `student.registered`, `enrollment.created` | `internal/worker/notification.go` |
| `academic-audit` | `student.registered`, `enrollment.created` | `internal/worker/audit.go` |
| `academic-report` | `enrollment.created` | `internal/worker/report.go` |
| `academic-dlq` | `events.dlq` | `internal/worker/dead_letter.go` |

**Como o `cmd/worker/main.go` decide qual worker rodar:** a variável de ambiente `PROCESSOR` determina o branch:

```go
switch os.Getenv("PROCESSOR") {
case "notification":
    kafka.RunConsumer(ctx, kafka.ConsumerConfig{
        Topics:    []string{events.TopicStudentRegistered, events.TopicEnrollmentCreated},
        GroupID:   "academic-notification",
        Processor: worker.NewNotificationProcessor(),
        // ...
    })
case "audit":   // topics: student.registered + enrollment.created
case "report":  // topics: enrollment.created apenas
case "dlq":     // topics: events.dlq
}
```

No `docker-compose.yml`, cada serviço (`worker-notification`, `worker-audit`, etc.) usa a mesma imagem `worker.Dockerfile` e passa `PROCESSOR=<valor>` como variável de ambiente.

**Offsets independentes por grupo:** um mesmo evento publicado uma única vez em `student.registered` é entregue tanto para `academic-notification` quanto para `academic-audit` — cada grupo tem seu próprio ponteiro de leitura (offset). Se o worker de auditoria estiver lento ou offline, o de notificação não é afetado.

---

## Fluxo feliz — POST /students

Passo a passo de um `POST /students` quando tudo funciona:

**1. O cliente envia a requisição** com `{"name": "João Silva", "email": "joao@example.com"}`.

**2. O handler valida os campos obrigatórios.** `StudentHandler.Register` (`internal/infra/handler/student_handler.go`) usa o `ShouldBindJSON` do Gin, que rejeita a requisição com 400 se `name` ou `email` estiverem ausentes.

**3. O usecase monta o student e o evento.** `StudentUseCase.Register` (`internal/usecase/student.go`) gera dois UUIDs — um para o `student.ID` e outro para o `event_id`. Monta o `StudentRegisteredEvent` com todos os dados, incluindo `published_at` (usado depois para medir latência). Serializa o evento em JSON e cria um `OutboxEntry` apontando para o tópico `academic.student.registered`.

**4. Student e evento são salvos numa única transação.** `StudentRepository.Save` (`internal/infra/postgres/student_repository.go`) abre uma transação, insere o student em `students` e o evento em `outbox_events` com `published = false`, e faz COMMIT. Se qualquer INSERT falhar, tudo reverte — nunca existe um student sem seu evento, nem um evento sem seu student.

**5. A API responde 201.** O cliente recebe o JSON do student criado. A partir daqui, tudo é assíncrono — o cliente não precisa aguardar o Kafka.

**6. O OutboxRelay publica o evento no Kafka.** Uma goroutine chamada `OutboxRelay.Run` (`internal/infra/postgres/outbox_relay.go`) roda dentro do processo da API e dispara a cada 200ms. Ela busca linhas com `published = false` usando `FOR UPDATE SKIP LOCKED` — se houver múltiplas réplicas da API, cada linha é processada por apenas uma delas. Para cada linha encontrada, o `Publisher.Publish` escreve a mensagem no Kafka e a linha é marcada como `published = true`, tudo dentro de uma transação. Se o Kafka estiver indisponível, a transação reverte e a linha é tentada no próximo tick.

**7. Os workers consomem o evento em paralelo.** O `worker-notification` e o `worker-audit` são processos separados no mesmo tópico, mas em consumer groups diferentes — cada um recebe o evento de forma independente. Antes de processar, cada worker chama `TryClaim` (`internal/infra/postgres/processed_event_repository.go`) para "reivindicar" o evento: apenas quem consegue inserir a linha em `processed_events` prossegue; os demais pulam silenciosamente. O processador é então chamado e, ao retornar `nil`, o offset é confirmado no Kafka.

**Resultado:** API respondeu em ~5ms. O evento foi salvo atomicamente com o student. O relay publicou no Kafka em até 200ms. Os workers processaram em paralelo sem interferir um no outro.

---

## Fluxo feliz — POST /enrollments

O fluxo é estruturalmente idêntico ao de students, com três diferenças importantes:

**1. O handler recebe campos diferentes.** `EnrollmentHandler.Create` (`internal/infra/handler/enrollment_handler.go`) lê `student_id` e `course_id` do corpo da requisição.

**2. O usecase cria um tipo de evento diferente.** `EnrollmentUseCase.Create` (`internal/usecase/enrollment.go`) monta um `EnrollmentCreatedEvent` com `enrollment_id`, `student_id` e `course_id`, salva o enrollment na tabela `enrollments` (que tem foreign key para `students`) e grava o OutboxEntry com o tópico `academic.enrollment.created`.

**3. Três workers consomem este evento, não dois.** O `academic-notification` e o `academic-audit` processam tanto `student.registered` quanto `enrollment.created`. O `academic-report` processa **exclusivamente** `enrollment.created` — ele não consome `student.registered`. Isso está definido em `cmd/worker/main.go` no `case "report"`, que passa apenas `events.TopicEnrollmentCreated` na lista de tópicos do consumer.

---

## OutboxRelay — por dentro

O `OutboxRelay` (`internal/infra/postgres/outbox_relay.go`) é iniciado como goroutine no processo da API (`cmd/api/main.go`):

```go
relay := postgres.NewOutboxRelay(db, publisher)
go relay.Run(ctx)  // ctx é cancelado quando o processo recebe SIGINT
```

A cada 200ms ele executa `publish(ctx)`:

```go
func (r *OutboxRelay) publish(ctx context.Context) error {
    tx, _ := r.db.BeginTx(ctx, nil)
    defer tx.Rollback()  // rollback se não chegar ao Commit

    rows, _ := tx.QueryContext(ctx,
        `SELECT id, topic, payload FROM outbox_events
         WHERE published = false
         ORDER BY created_at
         LIMIT 10
         FOR UPDATE SKIP LOCKED`,  // outras réplicas da API pulam essas linhas
    )

    // Para cada linha:
    //   1. publisher.Publish(ctx, entry.Topic, entry.Payload) → Kafka
    //   2. UPDATE outbox_events SET published = true WHERE id = $1

    return tx.Commit()
}
```

**`FOR UPDATE SKIP LOCKED` — o detalhe crítico:** quando você tem múltiplas réplicas da API (ex: 3 pods), cada uma roda seu próprio `OutboxRelay`. Sem `SKIP LOCKED`, todas tentariam pegar as mesmas linhas → deadlock ou publicação dupla. Com `SKIP LOCKED`, a primeira réplica que pega uma linha a bloqueia para escrita; as outras a pulam e vão para as próximas. Sem deadlock, sem duplicata.

**E se o Kafka estiver indisponível?** O `publish` retorna erro (logado como `[OutboxRelay] publish: ...`). A transação faz rollback. As linhas ficam com `published = false` e serão reprocessadas no próximo tick de 200ms. O evento não é perdido.

**`Publisher` persistente por tópico** (`internal/infra/kafka/publisher.go`): em vez de criar uma conexão TCP a cada `Publish`, o `Publisher` mantém um `map[string]*kafkago.Writer` protegido por `sync.Mutex`. Na primeira chamada para um tópico, cria o writer; nas seguintes, reutiliza. Isso economiza o overhead de handshake TCP e reduz a latência de publicação.

---

## Cenários de falha

### 1. Worker fora do ar

Imagine que o `worker-notification` cai enquanto havia mensagens não processadas no Kafka. O Kafka não descarta essas mensagens — ele as retém pelo período de retenção configurado (padrão: 7 dias). Cada consumer group mantém um ponteiro de leitura (offset) por partição, e o Kafka sabe qual foi o último offset que o grupo `academic-notification` confirmou.

Quando o worker volta, o leitor (`segmentio/kafka-go`) retoma automaticamente do próximo offset não confirmado. Todas as mensagens pendentes são reentregues e processadas normalmente.

Isso funciona porque o commit do offset só acontece **depois** que `handleMessage` retorna `nil` (`internal/infra/kafka/consumer.go`). Se o worker cai antes de commitar, a mensagem não foi confirmada — o Kafka vai reentregá-la. O `TryClaim` garante que, se o evento já havia sido parcialmente processado antes da queda, ele não será processado novamente.

```go
if err := handleMessage(ctx, message, config); err != nil {
    log.Printf("[%s] handleMessage: %v — mensagem não commitada, será reprocessada", ...)
    continue  // offset NÃO commitado
}
reader.CommitMessages(ctx, message)  // só chega aqui se não houve erro
```

---

### 2. Falha no processamento — retry com backoff

Quando o processador retorna erro (timeout em serviço externo, banco sobrecarregado), o `processWithRetry` (`internal/infra/kafka/consumer.go`) não desiste na primeira falha. Ele tenta até 3 vezes, esperando progressivamente mais entre cada tentativa: 1 segundo, depois 2 segundos, depois 4 segundos. Se ainda assim falhar, retorna o erro para `handleMessage`, que encaminha o evento para a DLQ.

```go
delays := []time.Duration{1 * time.Second, 2 * time.Second, 4 * time.Second}
```

Por que o backoff é crescente e não um delay fixo? Se dez instâncias do worker falham ao mesmo tempo por sobrecarga no banco e todas retryan a cada 1s fixo, elas disparam dez requisições simultâneas a cada segundo — piorando exatamente a sobrecarga que causou o problema. Com delays crescentes, as tentativas ficam espaçadas e dão ao sistema espaço para se recuperar.

O `select` com `ctx.Done()` dentro do backoff garante que, se o processo receber um sinal de shutdown enquanto espera, o worker encerra limpo em vez de ficar travado esperando o delay terminar.

```go
select {
case <-ctx.Done():
    return ctx.Err()  // shutdown gracioso durante o backoff
case <-time.After(delay):
}
```

---

### 3. Falha persistente — Dead Letter Queue

Quando as três tentativas de `processWithRetry` se esgotam, o evento não pode ser simplesmente descartado. O `handleMessage` (`internal/infra/kafka/consumer.go`) executa três ações em sequência:

**Primeiro**, chama `ReleaseClaim` para remover a linha de `processed_events`. Isso é necessário para que, quando o evento for republicado pelo ciclo da DLQ, o `TryClaim` consiga reivindicá-lo novamente. Se a linha ficasse na tabela, o `TryClaim` retornaria `rows=0` e o evento seria pulado silenciosamente — nunca seria reprocessado.

**Segundo**, lê o header `dlq-retry-count` da mensagem Kafka. Esse header registra quantas vezes este evento já passou pela DLQ. Na primeira falha ele não existe, então é tratado como zero.

**Terceiro**, publica uma mensagem em `academic.events.dlq` com o `DeadLetterEvent`, que contém o tópico original, o group que falhou, o payload original intacto (para poder ser republicado depois) e o `DLQRetryCount`. Depois retorna `nil` — o offset é commitado, pois o evento está seguro na DLQ.

O `worker-dlq` consome essa mensagem com o `DeadLetterProcessor` (`internal/worker/dead_letter.go`). Se `DLQRetryCount >= 3`, loga que o evento morreu definitivamente e descarta. Caso contrário, aguarda um backoff crescente (`nextRetry × 10s`, então 10s na primeira vez, 20s na segunda, 30s na terceira) e republica o payload original no tópico original com o header `dlq-retry-count` incrementado. O worker original vai tentar processar novamente, e o ciclo pode se repetir até 3 vezes.

O ciclo completo resulta em até 12 tentativas antes do descarte final:

| Ciclo | Retries internos | Backoff DLQ | Header publicado |
|---|---|---|---|
| 1ª vez (count=0) | 3 tentativas (1s + 2s + 4s) | — | vai para DLQ com count=0 |
| DLQ ciclo 1 | — | aguarda 10s | republica com count=1 |
| 2ª vez (count=1) | 3 tentativas | — | vai para DLQ com count=1 |
| DLQ ciclo 2 | — | aguarda 20s | republica com count=2 |
| 3ª vez (count=2) | 3 tentativas | — | vai para DLQ com count=2 |
| DLQ ciclo 3 | — | aguarda 30s | republica com count=3 |
| 4ª vez (count=3) | 3 tentativas | — | vai para DLQ com count=3 |
| DLQ ciclo 4 | — | — | descarte definitivo |

O `dlq-retry-count` viaja como header Kafka — não modifica o payload original. `readDLQRetryCount` lê esse header ao receber a mensagem; `PublishWithRetryHeader` escreve o valor incrementado na mensagem republicada. O payload do `OriginalPayload` dentro do `DeadLetterEvent` é o JSON original intocado.

---

### 4. Banco indisponível no worker

Quando o Postgres fica fora do ar, o worker não consegue executar `TryClaim` — o `db.ExecContext` retorna erro de conexão. Esse erro borbulha de volta através de `handleMessage`, que o retorna para `RunConsumer`. O `RunConsumer` usa `continue` sem commitar o offset, e o loop recomeça tentando buscar a próxima mensagem.

A mensagem permanece no Kafka com o offset não confirmado. Quando o banco voltar, o `FetchMessage` vai reentregá-la e o processamento segue normalmente. Nenhum evento é perdido.

É importante entender a diferença entre dois tipos de falha de banco:

- **Banco falha em `TryClaim`** (antes de qualquer processamento): `handleMessage` retorna erro → offset NÃO commitado → Kafka reentrega a mensagem inteira.
- **Banco falha dentro do processador** (ex: `NotificationProcessor` tenta gravar algo): `processWithRetry` absorve o erro, faz retry 3×, e se ainda falhar envia para a DLQ e retorna `nil` → offset É commitado → o evento está seguro na DLQ.

A regra é simples: o offset só é commitado quando o sistema tem certeza de que o evento foi processado ou está guardado em lugar seguro.

---

### 5. API morre após salvar — Outbox Pattern

Antes do Outbox Pattern existir, o código salvava o student no banco e depois publicava no Kafka em duas operações separadas. Se a API morresse entre as duas, o student estava salvo mas o evento nunca chegaria ao Kafka — uma inconsistência silenciosa, impossível de detectar.

Com o Outbox Pattern, o evento e o student são salvos juntos numa única transação. O Kafka não é chamado no request. Não existe janela entre "dado salvo" e "evento publicado" — o evento está na tabela `outbox_events` desde o mesmo instante em que o student existe no banco.

O `OutboxRelay` verifica a tabela a cada 200ms. Quando encontra linhas com `published = false`, publica no Kafka e as marca como `published = true`. Mesmo que a API reinicie, o relay encontra os eventos pendentes e os publica. Mesmo que o Kafka esteja fora no momento, os eventos ficam na tabela até que o próximo tick consiga publicar.

---

## Idempotência — TryClaim atômico

### Por que o Kafka entrega duplicatas

O Kafka garante **at-least-once delivery**: uma mensagem pode ser entregue mais de uma vez em certas situações.

A mais comum: o worker processou a mensagem com sucesso, mas o processo morreu (SIGKILL, OOM, crash) antes de executar `CommitMessages`. O Kafka não sabe que o processamento ocorreu — o offset ainda aponta para antes dessa mensagem. Quando o worker volta, ele reentrega a mensagem a partir do último offset confirmado. Sem proteção, o processador seria chamado duas vezes para o mesmo evento.

Outra situação: quando uma nova instância do worker entra no consumer group, o Kafka redistribui as partições entre as instâncias (rebalanceamento). Mensagens que ainda não foram commitadas podem ser atribuídas a outra instância, que as reentrega — mesmo que a instância anterior já as tenha processado.

### Como o TryClaim resolve

A tabela `processed_events` (`migrations/001_init.sql`) é o registro distribuído de processamento:

```sql
CREATE TABLE processed_events (
    event_id       VARCHAR(36) NOT NULL,
    consumer_group VARCHAR(64) NOT NULL,
    processed_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (event_id, consumer_group)  -- chave composta
);
```

**`TryClaim` (`internal/infra/postgres/processed_event_repository.go`):**

```go
func (r *ProcessedEventRepository) TryClaim(ctx context.Context, eventID, consumerGroup string) (bool, error) {
    result, err := r.db.ExecContext(ctx,
        "INSERT INTO processed_events (event_id, consumer_group) VALUES ($1, $2) ON CONFLICT DO NOTHING",
        eventID, consumerGroup,
    )
    rows, _ := result.RowsAffected()
    return rows == 1, nil
    // rows == 1 → este worker foi o primeiro a reivindicar → pode processar
    // rows == 0 → outro worker já processou → pular silenciosamente
}
```

**Fluxo completo por mensagem:**

```
FetchMessage recebe evento com event_id = "evt-abc-123"
                │
                ▼
TryClaim("evt-abc-123", "academic-notification")
  INSERT ... ON CONFLICT DO NOTHING
                │
        ┌───────┴─────────────┐
     rows=0                rows=1
  (já existia na tabela)  (inseriu agora — novo)
        │                     │
        ▼                     ▼
  log: "evento já         processWithRetry(payload, processor)
   processado, pulando"         │
  CommitMessages           ┌───┴────────────────────┐
  (offset avança sem      erro                     nil
   reprocessar)            │                        │
                    ReleaseClaim(eventID, group)  CommitMessages
                    publishDeadLetter(...)        (offset avança)
                    CommitMessages
                    (evento seguro na DLQ)
```

**Por que `ON CONFLICT DO NOTHING` é melhor que `HasProcessed` + `MarkProcessed`:** a abordagem antiga usaria dois round-trips separados ao banco (`SELECT` para verificar + `INSERT` para marcar), criando uma janela de race condition entre eles. Com `INSERT ... ON CONFLICT DO NOTHING`, o banco executa verificação + inserção em uma única operação atômica. Não há janela.

**A chave composta `(event_id, consumer_group)` garante que:**
- O mesmo `event_id` pode ser processado por grupos *diferentes* (`academic-notification` E `academic-audit` ambos processam o mesmo evento)
- O mesmo `event_id` *nunca* é processado duas vezes pelo *mesmo* grupo

### Race condition entre instâncias do mesmo worker

**Situação:** 3 instâncias de `worker-notification` recebem a msg5 simultaneamente (ex: após rebalanceamento):

```
worker-notification-1              worker-notification-2
         │                                  │
         ├─ TryClaim("evt-abc","notif")     ├─ TryClaim("evt-abc","notif")
         │  INSERT ... ON CONFLICT          │  INSERT ... ON CONFLICT
         │  Banco: constraint PK            │  Banco: conflito detectado
         │  → rows=1 (ganhou o lock)        │  → rows=0 (perdeu)
         │                                  │
         ├─ processWithRetry → sucesso       ├─ log: "evento já processado, pulando"
         └─ CommitMessages                  └─ CommitMessages
```

O banco garante que somente um dos INSERTs vai ganhar via PRIMARY KEY. A segunda inserção é ignorada silenciosamente com `ON CONFLICT DO NOTHING` — sem erro, sem processamento duplo.

---

## Escalabilidade horizontal

### Como o Kafka distribui trabalho

O Kafka divide cada tópico em **partições** (configurado como 3 em `docker-compose.yml`, `KAFKA_NUM_PARTITIONS: 3`). Quando múltiplos consumers do **mesmo grupo** estão ativos, o Kafka distribui as partições entre eles:

```
academic.student.registered:
  ├── partição-0: msg1, msg4, msg7 ...
  ├── partição-1: msg2, msg5, msg8 ...
  └── partição-2: msg3, msg6, msg9 ...

1 instância de worker-notification:
  └── recebe partição-0 + partição-1 + partição-2 (tudo)

2 instâncias de worker-notification:
  ├── instância-1: partição-0 + partição-1
  └── instância-2: partição-2

3 instâncias de worker-notification:
  ├── instância-1: partição-0
  ├── instância-2: partição-1
  └── instância-3: partição-2
```

### Limite de paralelismo por partições

**O número máximo de consumers úteis num grupo = número de partições.**

Com 3 partições, a 4ª instância ficará ociosa — não há partição para atribuir a ela:

```
4 instâncias, 3 partições:
  ├── instância-1: partição-0
  ├── instância-2: partição-1
  ├── instância-3: partição-2
  └── instância-4: [SEM PARTIÇÃO — ociosa, consome recursos sem processar]
```

Para aumentar o paralelismo além de 3, aumente `KAFKA_NUM_PARTITIONS` no `docker-compose.yml` **antes** de criar os tópicos (partições não podem ser reduzidas após criação).

### Como escalar um worker

```bash
# Subir 3 instâncias de worker-notification
make scale-notification n=3

# O que o Makefile executa:
# docker compose up --scale worker-notification=3 --no-recreate
# --no-recreate: não reinicia os outros serviços que já estão rodando
```

**Quando escalar faz sentido:** quando os logs ou o Kafka UI mostram que o consumer lag está crescendo — o Kafka está acumulando mensagens mais rápido do que o worker processa.

```bash
# Verificar o lag via Kafka UI
open http://localhost:8090
# → Consumer Groups → academic-notification → ver coluna "Lag" por partição
# lag = 0: worker está em dia
# lag crescendo: worker está lento ou down
```

**Workers de grupos diferentes escalam de forma completamente independente.** Escalar `worker-notification` não afeta `worker-audit` ou `worker-report` — cada grupo tem suas próprias atribuições de partições.

---

## Consistência eventual

O sistema é **eventualmente consistente**: a API responde `201` assim que o dado é salvo no banco (junto com o evento no outbox). Os workers processam de forma assíncrona.

```
t=0ms     POST /students → transação (student + outbox_entry) → responde 201
t=~200ms  OutboxRelay publica o evento no Kafka
t=~212ms  worker-notification processa → log de notificação
t=~215ms  worker-audit processa → log de auditoria
```

| Pergunta | Resposta |
|---|---|
| O aluno foi salvo? | Sim, imediatamente (no `201`) |
| O evento está seguro? | Sim, na mesma transação do aluno — antes do `201` |
| A notificação foi enviada? | Será, em ~200ms a poucos segundos |
| Posso consultar o log de auditoria agora? | Talvez não — depende do lag do worker |
| E se o worker de notificação estiver down? | A mensagem fica no Kafka (até 7 dias) e será processada quando voltar |
| E se a notificação falhar 3 vezes? | Vai para a DLQ, que auto-republica até 3 ciclos (12 tentativas total) |
| E se a API morrer antes de publicar no Kafka? | O OutboxRelay encontra `published=false` no próximo ciclo (200ms) e publica |

---

## Observando o sistema

### Kafka UI — `http://localhost:8090`

```
1. Acesse localhost:8090
2. Clique no cluster "academic"
3. Topics → academic.student.registered
   → veja as mensagens, partições e offsets

4. Consumer Groups → academic-notification
   → coluna "Lag" por partição:
     lag = 0: worker está em dia
     lag crescendo: worker está lento ou down
```

### Logs em tempo real

```bash
make logs
# equivalente a: docker compose logs -f
```

**Saída esperada após um POST /students:**
```
api                 | [GIN] 201 | 5ms | POST /students
api                 | [OutboxRelay] publicado: academic.student.registered
worker-notification | [notification] student.registered | email: joao@example.com | latência: 12ms
worker-audit        | [audit] student.registered | id: 550e... | latência: 15ms
```

**Saída de retry com backoff:**
```
worker-notification | retry 1/3: connection timeout
worker-notification | retry 2/3: connection timeout
worker-notification | retry 3/3: connection timeout
worker-dlq          | [DLQ] republicando para academic.student.registered (tentativa 1/3) aguardando 10s | event_id: ...
```

**Saída de evento definitivamente morto (após 3 ciclos DLQ):**
```
worker-dlq | [DLQ] EVENTO MORTO DEFINITIVAMENTE após 3 tentativas | topic: academic.student.registered | group: academic-notification | event_id: dlq-... | reason: ...
```

**Saída de evento duplicado (idempotência funcionando):**
```
worker-notification | [academic-notification] evento já processado, pulando: evt-abc-123
```

**Saída de erro de banco no worker (mensagem não commitada):**
```
worker-notification | [academic-notification] handleMessage: tryClaim: dial tcp: connection refused — mensagem não commitada, será reprocessada
```

---

## Resumo dos trade-offs

| Decisão | Arquivo relevante | Vantagem | Limitação |
|---|---|---|---|
| Outbox Pattern — evento + entidade na mesma transação | `postgres/student_repository.go`, `postgres/enrollment_repository.go` | Zero perda de evento: API pode morrer a qualquer momento após o COMMIT | Latência extra de ~200ms até o relay publicar no Kafka |
| `FOR UPDATE SKIP LOCKED` no relay | `postgres/outbox_relay.go` | Múltiplas réplicas da API processam o outbox sem conflito ou deadlock | Requer suporte no banco (PostgreSQL ✓, MySQL 8+ ✓) |
| `ReleaseClaim` antes de publicar na DLQ | `kafka/consumer.go` | Evento que vai para DLQ pode ser reprocessado pelo ciclo DLQ→original | `ReleaseClaim` pode falhar; a linha em `processed_events` impede nova tentativa até ser removida manualmente ou o ciclo DLQ funcionar de outro ângulo |
| Auto-reprocessamento DLQ — até 3 ciclos | `worker/dead_letter.go` | Sem ação manual — eventos com falhas transitórias se recuperam sozinhos | Eventos com falhas permanentes passam por até 12 tentativas totais antes do descarte final |
| Backoff exponencial — 1s, 2s, 4s | `kafka/consumer.go` — `processWithRetry` | Evita avalanche de requisições simultâneas quando o banco está sobrecarregado | Aumenta a latência máxima por tentativa para até 7 segundos antes de ir para a DLQ |
| Backoff DLQ — 10s, 20s, 30s | `worker/dead_letter.go` — `DeadLetterProcessor` | Dá tempo longo para o serviço com problema se recuperar entre ciclos | Durante o backoff o worker-dlq está bloqueado nesse evento; ctx.Done() permite shutdown gracioso |
| Writers Kafka persistentes por tópico — `sync.Mutex` + `map` | `kafka/publisher.go` — `Publisher` | Reutiliza conexão TCP — maior throughput e menor latência por publicação | Writers ficam vivos até o processo encerrar; `Close()` deve ser chamado no shutdown (já feito via `defer publisher.Close()` no main) |
| 3 partições por tópico | `docker-compose.yml` — `KAFKA_NUM_PARTITIONS: 3` | Paralelismo máximo de 3 workers por grupo sem configuração adicional | Escalar além de 3 instâncias não aumenta throughput; aumentar partições exige recriar os tópicos |
| Um binário de worker com `PROCESSOR` env var | `cmd/worker/main.go` | Uma única imagem Docker para todos os workers — simplifica o deploy | Adicionar um novo tipo de worker exige alterar o `switch` no main e fazer rebuild da imagem |
