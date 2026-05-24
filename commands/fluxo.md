# Fluxo, Garantias e Demonstrações

Fonte autoritativa sobre comportamento em runtime: como os dados fluem, como as garantias de entrega funcionam, retry/DLQ, idempotência e como demonstrar cada requisito do trabalho. Use `/arquitetura` para estrutura de diretórios, componentes e Docker Compose.

---

## Fluxo completo — POST /students

1. O cliente faz `POST /students {"name": "João", "email": "joao@puc.br"}`.

2. O handler chama `usecase.StudentUseCase.Register(ctx, input)`.

3. O usecase monta o `domain.Student` com um UUID novo e monta o `events.StudentRegisteredEvent` com outro UUID (`EventID`). Serializa o evento em JSON e cria um `domain.OutboxEntry` apontando para `TopicStudentRegistered`.

4. O usecase chama `studentRepository.Save(ctx, student, outboxEntry)`. Dentro do repositório, uma **única transação** insere o `student` em `students` e o `outboxEntry` em `outbox_events`. Se qualquer parte falhar, nada é persistido.

5. O handler retorna `201 Created` com o JSON do student. O cliente recebeu a resposta — o processamento secundário ainda não aconteceu.

6. Em paralelo dentro do processo `api`, o `OutboxRelay` roda em uma goroutine com um ticker de 200ms. A cada tick, abre uma transação, faz `SELECT ... FOR UPDATE SKIP LOCKED LIMIT 10` em `outbox_events` onde `published = false`, publica cada linha no Kafka via `kafka.Publisher`, marca `published = true` e faz commit. O `FOR UPDATE SKIP LOCKED` garante que múltiplas réplicas da API não publicam a mesma linha duas vezes.

7. A mensagem chega no Kafka no tópico `academic.student.registered`.

8. `worker-notification` (group `academic-notification`) e `worker-audit` (group `academic-audit`) consomem a mensagem de forma independente e simultânea, cada um aplicando o fluxo de idempotência + retry descrito abaixo.

O mesmo padrão se aplica a `POST /enrollments`, com `academic-report` também consumindo `enrollment.created`.

---

## Fluxo do consumer — At-least-once com idempotência

`auto-commit` é desabilitado. O offset só é commitado **após** o processamento ser concluído. Isso garante que se o worker morrer no meio do processamento, a mensagem será relida.

1. O consumer recebe a mensagem (offset ainda não commitado).
2. Extrai o `event_id` do payload JSON.
3. Chama `TryClaim(ctx, eventID, groupID)`. TryClaim executa `INSERT INTO processed_events ... ON CONFLICT DO NOTHING` e retorna `true` se a linha foi inserida (evento novo) ou `false` se já existia (evento duplicado). É uma operação atômica — sem race condition.
4. Se `TryClaim` retornou `false`: o evento já foi processado por este group. Commita o offset e passa para a próxima mensagem.
5. Se `TryClaim` retornou `true`: chama `processWithRetry(ctx, processor, payload)` com até 3 tentativas e backoff exponencial de 1s → 2s → 4s.
6. Se `processWithRetry` teve sucesso: commita o offset.
7. Se `processWithRetry` esgotou as 3 tentativas sem sucesso: chama `ReleaseClaim(ctx, eventID, groupID)` para remover a linha de `processed_events` (permite que o ciclo DLQ reclame o evento depois), lê o cabeçalho `dlq-retry-count` da mensagem Kafka, publica um `DeadLetterEvent` em `academic.events.dlq` via `PublishWithRetryHeader`, e commita o offset — a partição nunca trava.

Se o worker morrer entre o `TryClaim` e o `CommitOffset`, o evento será relido. Na próxima tentativa, `TryClaim` retorna `false` (a linha já existe se o processamento foi bem-sucedido antes da morte) ou o processamento é retentado normalmente (se a linha foi removida pelo `ReleaseClaim` antes da morte).

---

## Idempotência

### Por que é necessária

Com at-least-once, o mesmo evento pode chegar mais de uma vez: crash entre processamento e commit, rebalanceamento de partição, restart de container. Sem idempotência, notificações duplicadas e entradas duplas de auditoria acontecem silenciosamente.

### Implementação

Tabela `processed_events` com PK composta `(event_id, consumer_group)`:

```sql
CREATE TABLE processed_events (
    event_id       VARCHAR(36)  NOT NULL,
    consumer_group VARCHAR(64)  NOT NULL,
    processed_at   TIMESTAMPTZ  NOT NULL DEFAULT now(),
    PRIMARY KEY (event_id, consumer_group)
);
```

O mesmo `event_id` pode coexistir para `academic-notification` e `academic-audit` — cada group tem seu registro independente. O group `notification` nunca bloqueia o group `audit`.

### Interface em `internal/domain/port/port.go`

```go
type IdempotencyRepository interface {
    TryClaim(ctx context.Context, eventID, consumerGroup string) (bool, error)
    ReleaseClaim(ctx context.Context, eventID, consumerGroup string) error
}
```

`TryClaim` faz `INSERT ... ON CONFLICT DO NOTHING` e verifica `rows affected == 1`. É uma única operação atômica — sem janela de race condition entre verificação e inserção.

`ReleaseClaim` faz `DELETE FROM processed_events WHERE event_id=$1 AND consumer_group=$2`. É chamado antes de publicar no DLQ para que o ciclo de reprocessamento do DLQ possa reivindicar o evento limpo.

---

## Retry e DLQ

### Retry interno (worker → DLQ)

A lógica vive em `infra/kafka/consumer.go` e é compartilhada por todos os groups. Os processors só retornam `error` ou `nil` — não sabem que existe retry.

- 3 tentativas com backoff exponencial: 1s → 2s → 4s
- Após esgotar: `ReleaseClaim` + publica `DeadLetterEvent` em `academic.events.dlq`
- Commita o offset mesmo após falha total — a partição nunca trava
- O cabeçalho `dlq-retry-count` viaja na mensagem Kafka (não no payload) para rastrear o ciclo

### Ciclo DLQ (worker-dlq reprocessa)

O `worker-dlq` consume `academic.events.dlq`. Para cada `DeadLetterEvent`:

| `dlq_retry_count` | Ação |
|---|---|
| 0 | Aguarda 10s, republica em `academic.events.dlq` com `dlq-retry-count: 1` |
| 1 | Aguarda 20s, republica com `dlq-retry-count: 2` |
| 2 | Aguarda 30s, republica com `dlq-retry-count: 3` |
| ≥ 3 | Loga morte permanente (`[DLQ PERMANENT DEATH]`), retorna nil, descarta |

No total são 12 tentativas: 3 retries internos + até 3 ciclos DLQ de 3 retries cada.

---

## Consistência eventual

A API retorna `201 Created` assim que o dado está salvo no PostgreSQL (entidade + outbox). Nesse momento o `OutboxRelay` **ainda não publicou** no Kafka, e o worker **ainda não processou** notificação, auditoria ou relatório.

Isso é **consistência eventual**: o sistema não garante que todos os efeitos secundários aconteceram no momento da resposta, mas garante que vão acontecer — o Outbox Pattern elimina o risco de o evento ser perdido, pois ele já está durável no banco antes da resposta ao cliente.

**Como explicar na apresentação:** o banco é a fonte de verdade imediata. O outbox garante que o evento não se perde mesmo que o Kafka esteja temporariamente fora. O worker processa quando o evento chegar — a consistência final é garantida; o timing não é.

---

## Mapeamento: Requisito → Como demonstrar

| Requisito obrigatório | Como demonstrar na apresentação |
|---|---|
| **Publicação de mensagens** | POST /students → Kafka UI (`localhost:8090`) com a mensagem no tópico + log da API publicando via OutboxRelay |
| **Consumo assíncrono** | Logs dos workers com `ProcessedAt` calculado após `PublishedAt` — latência visível |
| **Tópico recebendo mensagens** | `docker compose stop worker-notification` → fazer vários POSTs → Kafka UI mostra mensagens acumuladas e consumer lag crescendo |
| **Consumidor indisponível** | Mesmo cenário acima → `docker compose start worker-notification` → worker retoma do offset, processa tudo em catch-up |
| **Retry de mensagens** | Forçar erro num processor (retornar erro fixo) → logs mostram retry 1, 2, 3 → mensagem aparece no tópico `academic.events.dlq` → `worker-dlq` loga alerta e reprocessa |
| **Tempo de publicação → processamento** | Campo `PublishedAt` no evento + `time.Since(event.PublishedAt)` logado em cada processor: `"latência: Xms"` |
| **Consistência eventual e falhas** | Explicar: API retorna 201, dado está no banco + outbox, OutboxRelay publica em até 200ms, worker processa depois — consistente eventualmente |
| **Idempotência** | Bônus: reenviar o mesmo evento manualmente → log do worker: "evento já processado, pulando" — sem notificação duplicada |

---

## Trade-offs para justificar na apresentação

**Por que Outbox Pattern e não publicar no Kafka direto da API?**
Sem o outbox, se a API salva no banco e depois falha antes de publicar no Kafka, o evento se perde para sempre — o banco tem o dado mas ninguém sabe. Com o outbox, a entidade e o evento são salvos na mesma transação. Mesmo que a API caia, o `OutboxRelay` publica quando ela voltar. Nenhum evento é perdido.

**Por que múltiplos consumer groups e não um único consumer fazendo tudo?**
Com um único consumer, uma falha na notificação bloquearia a auditoria. Groups separados isolam falhas — cada responsabilidade falha e se recupera de forma independente. Isso é o desacoplamento real que o MOM promete.

**Por que Kafka e não Redis Pub/Sub?**
Redis Pub/Sub não persiste mensagens: consumer offline = evento perdido para sempre. Kafka persiste por offset — o consumer retoma exatamente onde parou. Isso é fundamental para demonstrar o comportamento com consumidor indisponível.

**Por que DLQ em vez de retry infinito?**
Retry infinito trava a partição: nenhuma mensagem seguinte é processada enquanto uma falha persistir. O DLQ commita o offset e isola o evento problemático — o fluxo continua, e o evento morto fica rastreável para investigação e reprocessamento.

**Por que idempotência com TryClaim atômico?**
Com at-least-once, o mesmo evento pode chegar múltiplas vezes mesmo sem falha explícita (rebalanceamento de partição, restart de container). O `TryClaim` faz um único `INSERT ON CONFLICT DO NOTHING` — sem dois round-trips ao banco, sem janela de race condition entre "verificar" e "marcar".

**Por que 3 partições?**
Com 1 partição, escalar o consumer group para N instâncias não distribui carga — só uma instância recebe mensagens. Com 3 partições, até 3 instâncias do mesmo group processam em paralelo sem duplicidade.

---

## Como usar este comando

- **Como funciona o fluxo de ponta a ponta?** Consulte a seção de fluxo completo
- **Como garantir at-least-once sem duplicar?** Consulte idempotência + fluxo do consumer
- **Como demonstrar um requisito do trabalho?** Consulte a tabela de mapeamento
- **Como justificar uma decisão ao professor?** Consulte os trade-offs

Use `/arquitetura` para estrutura de diretórios, componentes e Docker Compose.
Use `/stack` para padrões de código Go e `/tp` para requisitos do trabalho.
