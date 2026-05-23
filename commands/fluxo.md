# Fluxo, Garantias e Demonstrações

Fonte autoritativa sobre comportamento em runtime: como os dados fluem, como as garantias de entrega funcionam, retry/DLQ, idempotência e como demonstrar cada requisito do trabalho. Use `/arquitetura` para estrutura de diretórios, componentes e Docker Compose.

---

## Fluxo completo — POST /students

```
1. POST /students {"name": "João", "email": "joao@puc.br"}

2. handler.RegisterStudent
   └─ monta domain.Student{ID: uuid()}
   └─ service.StudentService.Register(ctx, student)

3. service.StudentService.Register
   └─ repository.StudentRepository.Save(ctx, student)         → PostgreSQL
   └─ monta StudentRegisteredEvent{EventID: uuid(), PublishedAt: now()}
   └─ publisher.Publish(ctx, TopicStudentRegistered, event)   → Kafka
   └─ retorna nil

4. handler → 201 Created + JSON do student
   (cliente recebe resposta — processamento secundário ainda não aconteceu)

━━━ assíncrono, sem bloquear o cliente ━━━

5a. worker-notification (academic-notification)
    └─ idempotencyRepo.HasProcessed(ctx, eventID, "academic-notification") → false
    └─ NotificationProcessor.Process(ctx, payload)
       └─ loga: "notificação → joao@puc.br | latência: 11ms"
    └─ idempotencyRepo.MarkProcessed(ctx, eventID, "academic-notification")
    └─ CommitOffset

5b. worker-audit (academic-audit)
    └─ idempotencyRepo.HasProcessed(ctx, eventID, "academic-audit") → false
    └─ AuditProcessor.Process(ctx, payload)
       └─ loga: "[AUDIT] student.registered | id: <uuid> | latência: 13ms"
    └─ idempotencyRepo.MarkProcessed(ctx, eventID, "academic-audit")
    └─ CommitOffset
```

O mesmo padrão se aplica a POST /enrollments, com `academic-report` também consumindo.

---

## Garantia de entrega: At-least-once

`auto-commit` é desabilitado. O offset só é commitado **após** o processamento ser concluído. A ordem é inviolável:

```
Recebe mensagem (offset não commitado)
        │
        ▼
HasProcessed(eventID, groupID)?
        │
   ┌────┴────┐
  sim       não
   │          │
CommitOffset  Process(ctx, payload)
e skip             │
              ┌────┴────┐
           sucesso    falha
              │          │
    MarkProcessed    retry 1 — aguarda 1s
    CommitOffset          │
                     ┌────┴────┐
                  sucesso    falha
                     │          │
           MarkProcessed    retry 2 — aguarda 2s
           CommitOffset          │
                            ┌────┴────┐
                         sucesso    falha
                            │          │
                  MarkProcessed    retry 3 — aguarda 4s
                  CommitOffset          │
                                   ┌────┴────┐
                                sucesso    falha
                                   │          │
                         MarkProcessed   publica DeadLetterEvent
                         CommitOffset    CommitOffset
```

Se o processo morrer entre `MarkProcessed` e `CommitOffset`, o evento será relido. Na próxima tentativa, `HasProcessed` retorna `true` e o evento é pulado com segurança — sem duplicidade.

---

## Idempotência

### Por que é necessária

Com at-least-once, o mesmo evento pode chegar mais de uma vez: crash entre process e commit, rebalanceamento de partição, restart de container. Sem idempotência, notificações duplicadas e entradas duplas de auditoria acontecem silenciosamente.

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

O mesmo `event_id` pode coexistir para `academic-notification` e `academic-audit` — cada group tem seu registro independente.

`MarkProcessed` faz `INSERT`. Se retornar erro de chave duplicada, o consumer trata como "já processado" — commita o offset e segue sem reprocessar.

### Interface em `repository/processed_event.go`

```go
type ProcessedEventRepository interface {
    HasProcessed(ctx context.Context, eventID, consumerGroup string) (bool, error)
    MarkProcessed(ctx context.Context, eventID, consumerGroup string) error
}
```

---

## Retry e DLQ

A lógica de retry vive inteiramente em `kafka/consumer.go` e é compartilhada por todos os groups. Os processors não sabem que existe retry — eles só retornam `error` ou `nil`.

- **3 retries** com backoff exponencial: 1s → 2s → 4s
- Após esgotar: publica `DeadLetterEvent` em `academic.events.dlq`
- Commita o offset mesmo após falha total — a partição nunca trava
- `worker-dlq` consome o tópico de DLQ e loga com severidade alta

---

## Consistência eventual

A API retorna `201 Created` assim que o dado está salvo no PostgreSQL e o evento publicado no Kafka. Nesse momento o worker **ainda não processou** a notificação, auditoria ou relatório.

Isso é **consistência eventual**: o sistema não garante que todos os efeitos secundários aconteceram no momento da resposta, mas garante que vão acontecer — desde que o Kafka esteja disponível e o worker eventualmente processe a mensagem.

**Como explicar na apresentação:** o banco é a fonte de verdade imediata. O Kafka garante que os processamentos secundários vão ocorrer, mesmo que o worker esteja temporariamente fora. A consistência final é garantida; o timing não é.

---

## Mapeamento: Requisito → Como demonstrar

| Requisito obrigatório | Como demonstrar na apresentação |
|---|---|
| **Publicação de mensagens** | POST /students → mostrar log da API publicando + Kafka UI (`localhost:8090`) com a mensagem no tópico |
| **Consumo assíncrono** | Logs dos workers com `ProcessedAt` calculado após `PublishedAt` — latência visível |
| **Tópico recebendo mensagens** | `docker compose stop worker-notification` → fazer vários POSTs → Kafka UI mostra mensagens acumuladas e consumer lag crescendo |
| **Consumidor indisponível** | Mesmo cenário acima → `docker compose start worker-notification` → worker retoma do offset, processa tudo em catch-up |
| **Retry de mensagens** | Forçar erro num processor (retornar erro fixo) → logs mostram retry 1, 2, 3 → mensagem aparece no tópico `academic.events.dlq` → `worker-dlq` loga alerta |
| **Tempo de publicação → processamento** | Campo `PublishedAt` no evento + `time.Since(event.PublishedAt)` logado em cada processor: `"latência: Xms"` |
| **Consistência eventual e falhas** | Explicar: API retorna 201, dado está no banco, worker ainda não processou — sistema é consistente eventualmente, não imediatamente |
| **Idempotência** | Bônus: reenviar o mesmo evento manualmente → log do worker: "evento já processado, pulando" — sem notificação duplicada |

---

## Trade-offs para justificar na apresentação

**Por que múltiplos consumer groups e não um único consumer fazendo tudo?**
Com um único consumer, uma falha na notificação bloquearia a auditoria. Groups separados isolam falhas — cada responsabilidade falha e se recupera de forma independente. Isso é o desacoplamento real que o MOM promete.

**Por que Kafka e não Redis Pub/Sub?**
Redis Pub/Sub não persiste mensagens: consumer offline = evento perdido para sempre. Kafka persiste por offset — o consumer retoma exatamente onde parou. Isso é fundamental para demonstrar o comportamento com consumidor indisponível.

**Por que DLQ em vez de retry infinito?**
Retry infinito trava a partição: nenhuma mensagem seguinte é processada enquanto uma falha persistir. O DLQ commita o offset e isola o evento problemático — o fluxo continua, e o evento morto fica rastreável para investigação.

**Por que idempotência com tabela e não confiar no retry limitado?**
Com at-least-once, o mesmo evento pode chegar múltiplas vezes mesmo sem falha explícita (rebalanceamento de partição, restart de container pelo Docker). Sem idempotência, notificações duplicadas chegam ao aluno silenciosamente — bug invisível e difícil de detectar.

**Por que 3 partições?**
Com 1 partição, escalar o consumer group para N instâncias não distribui carga — só uma instância recebe mensagens. Com 3 partições, até 3 instâncias do mesmo group processam em paralelo sem duplicidade.

---

## Como usar este comando

- **Como funciona o fluxo de ponta a ponta?** Consulte a seção de fluxo completo
- **Como garantir at-least-once sem duplicar?** Consulte idempotência + fluxo de commit
- **Como demonstrar um requisito do trabalho?** Consulte a tabela de mapeamento
- **Como justificar uma decisão ao professor?** Consulte os trade-offs

Use `/arquitetura` para estrutura de diretórios, componentes e Docker Compose.
Use `/stack` para padrões de código Go e `/tp` para requisitos do trabalho.
