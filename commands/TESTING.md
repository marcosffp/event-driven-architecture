# Roteiro de Testes — Arquitetura Orientada a Eventos

> Siga cada fase em ordem. Cada passo tem o comando exato, o que deve aparecer e o que aquele resultado prova em relação aos critérios de aceite.

---

## Antes de começar

Copie o arquivo de variáveis de ambiente:

```bash
cp .env.example .env
```

Confirme que o `.env` está assim (FAIL_RATE vazio por enquanto):

```
POSTGRES_DB=academic
POSTGRES_USER=academic
POSTGRES_PASSWORD=academic
DATABASE_URL=postgres://academic:academic@postgres:5432/academic?sslmode=disable
API_PORT=8080
FAIL_RATE=
```

---

## FASE 0 — Subir o ambiente

### 0.1 — Build e inicialização

```bash
docker compose up --build -d
```

Aguarde todos os containers ficarem prontos:

```bash
docker compose ps
```

**Esperado:**

```
NAME                             STATUS
academic-postgres-1              running (healthy)
academic-kafka-1                 running (healthy)
academic-kafka-ui-1              running
academic-api-1                   running
academic-worker-notification-1   running
academic-worker-audit-1          running
academic-worker-report-1         running
academic-worker-dlq-1            running
```

**O que prova:** infraestrutura completa (Postgres, Kafka, API, 4 workers) sobe sem erros — critério 5.6.

---

### 0.2 — Verificar startup dos workers

```bash
docker compose logs worker-notification worker-audit worker-report worker-dlq
```

**Esperado — uma linha dessas para cada worker:**

```
[academic-notification] consumidor atualizado, sem mensagens pendentes
[academic-audit] consumidor atualizado, sem mensagens pendentes
[academic-report] consumidor atualizado, sem mensagens pendentes
[academic-dlq] consumidor atualizado, sem mensagens pendentes
```

**O que prova:** o `checkConsumerLag` executa no startup e confirma que não há mensagens acumuladas da sessão anterior. Esta é a base para o critério 4.4 (comportamento de retomada após indisponibilidade).

---

### 0.3 — Abrir o Kafka UI

Abrir no browser: **http://localhost:8090**

**Esperado:** interface do Kafka UI carrega, cluster `academic` aparece na lista, seção `Topics` e `Consumer Groups` acessíveis.

---

## FASE 1 — Ação principal via API

> Cobre os critérios: 1.1 (endpoint HTTP), 1.2 (publicação automática de evento), 3.3 (demonstração de envio).

### 1.1 — Cadastrar aluno

```bash
curl -s -X POST http://localhost:8080/students \
  -H "Content-Type: application/json" \
  -d '{"name": "Ana Costa", "email": "ana@puc.br"}' | jq .
```

**Esperado:**

```json
{
  "id": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
  "name": "Ana Costa",
  "email": "ana@puc.br",
  "created_at": "2026-06-04T..."
}
```

- Status HTTP: `201 Created`
- Resposta retorna imediatamente, sem aguardar workers

**Copie o campo `id` — ele será usado no passo 1.2.**

**O que prova:** endpoint HTTP existe, responde de forma síncrona, não bloqueia em tarefas secundárias (critério 1.1).

---

### 1.2 — Criar matrícula

Substitua `<ID_DO_ALUNO>` pelo `id` retornado no passo anterior:

```bash
curl -s -X POST http://localhost:8080/enrollments \
  -H "Content-Type: application/json" \
  -d '{"student_id": "<ID_DO_ALUNO>", "course_id": "curso-arq-software"}' | jq .
```

**Esperado:**

```json
{
  "id": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
  "student_id": "<ID_DO_ALUNO>",
  "course_id": "curso-arq-software",
  "created_at": "2026-06-04T..."
}
```

- Status HTTP: `201 Created`

---

### 1.3 — Verificar eventos publicados no Kafka UI

**Tópico do aluno:**

No Kafka UI: `Topics → academic.student.registered → Messages`

**Esperado — payload da mensagem:**

```json
{
  "event_id": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
  "student_id": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
  "name": "Ana Costa",
  "email": "ana@puc.br",
  "published_at": "2026-06-04T..."
}
```

**Tópico da matrícula:**

No Kafka UI: `Topics → academic.enrollment.created → Messages`

**Esperado — payload da mensagem:**

```json
{
  "event_id": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
  "enrollment_id": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
  "student_id": "<ID_DO_ALUNO>",
  "course_id": "curso-arq-software",
  "published_at": "2026-06-04T..."
}
```

**O que prova:** evento publicado automaticamente após a ação, contém dados relevantes do domínio (`event_id`, `published_at`, campos do negócio) — critérios 1.2 e 3.3.

---

## FASE 2 — Consumo assíncrono e latência

> Cobre os critérios: 1.3 (consumidor separado), 1.4 (segundo plano), 4.2 (demo ao vivo), 4.6 (latência visível).

### 2.1 — Acompanhar workers em tempo real

```bash
docker compose logs -f worker-notification worker-audit worker-report
```

**Esperado — linhas aparecendo nos logs:**

```
worker-notification-1  | [notification] student.registered | email: ana@puc.br | latência: 312ms
worker-audit-1         | [audit] student.registered | id: <uuid> | latência: 318ms
worker-notification-1  | [notification] enrollment.created | student: <uuid> | course: curso-arq-software | latência: 205ms
worker-audit-1         | [audit] enrollment.created | id: <uuid> | student: <uuid> | latência: 210ms
worker-report-1        | [report] enrollment.created | id: <uuid> | student: <uuid> | course: curso-arq-software | latência: 198ms
```

**O que observar:**

- `notification` e `audit` aparecem para `student.registered` → dois consumer groups independentes lendo o mesmo tópico
- `report` **não aparece** para `student.registered`, só para `enrollment.created` → consumer group com tópico específico
- Campo `latência:` visível em cada linha → critério 4.6 cumprido

**O que prova:** consumidores separados do produtor (critério 1.3), processamento em segundo plano sem bloquear HTTP (critério 1.4), latência mensurável e visível (critério 4.6).

---

### 2.2 — Verificar consumer groups no Kafka UI

No Kafka UI: `Consumer Groups`

**Esperado — 3 grupos com lag zerado:**

```
academic-notification   lag: 0
academic-audit          lag: 0
academic-report         lag: 0
```

**O que prova:** cada consumer group tem offset próprio e independente — base para explicar escalabilidade e desacoplamento (critério 5.7).

---

## FASE 3 — Desacoplamento: API sem workers

> Cobre o critério 1.5 (produtor continua sem consumidor) e 4.4 parcial.

### 3.1 — Parar todos os workers

```bash
docker compose stop worker-notification worker-audit worker-report
```

### 3.2 — API deve continuar respondendo

```bash
curl -s -X POST http://localhost:8080/students \
  -H "Content-Type: application/json" \
  -d '{"name": "Teste Desacoplado", "email": "desacoplado@puc.br"}' | jq .
```

**Esperado:** `201 Created` com JSON — resposta imediata, igual ao passo 1.1.

**O que prova:** produtor não conhece o consumidor diretamente e continua funcionando sem ele — critério 1.5.

### 3.3 — Religar os workers

```bash
docker compose start worker-notification worker-audit worker-report
```

---

## FASE 4 — Consumidor indisponível: acúmulo e retomada

> Cobre o critério 4.4 completo.

### 4.1 — Parar apenas o worker de notification

```bash
docker compose stop worker-notification
```

### 4.2 — Disparar 3 ações com worker fora

```bash
for i in 1 2 3; do
  curl -s -X POST http://localhost:8080/students \
    -H "Content-Type: application/json" \
    -d "{\"name\": \"Aluno $i\", \"email\": \"aluno${i}@puc.br\"}" | jq .id
done
```

**Esperado:** 3 UUIDs retornados sem erro — API não sabe nem liga que o worker está fora.

### 4.3 — Confirmar lag no Kafka UI

No Kafka UI: `Consumer Groups → academic-notification`

**Esperado:** `lag: 3` (ou mais) — mensagens esperando na fila.

**Verificar também:** `academic-audit` e `academic-report` com `lag: 0` — eles continuam processando normalmente.

**O que prova:** mensagens se acumulam sem serem perdidas, outros consumers não são afetados — critério 4.4.

### 4.4 — Religar o worker e observar a retomada

```bash
docker compose start worker-notification
docker compose logs -f worker-notification
```

**Esperado — primeiras linhas dos logs após religar:**

```
[academic-notification] *** RETOMANDO APÓS INDISPONIBILIDADE: 3 mensagens acumuladas aguardando processamento ***
[notification] student.registered | email: aluno1@puc.br | latência: ...
[notification] student.registered | email: aluno2@puc.br | latência: ...
[notification] student.registered | email: aluno3@puc.br | latência: ...
```

**No Kafka UI:** após alguns segundos, o lag do `academic-notification` volta para `0`.

**O que prova:** ao religar, o consumer processa as mensagens acumuladas sem perda — critério 4.4.

---

## FASE 5 — Retry automático e Dead Letter Queue

> Cobre o critério 4.5 completo.

### 5.1 — Ativar falha simulada

Edite o `.env` e mude `FAIL_RATE`:

```
FAIL_RATE=1
```

Recrie os workers com a nova variável:

```bash
docker compose up -d --force-recreate worker-notification worker-audit worker-report worker-dlq
```

### 5.2 — Disparar uma ação

```bash
curl -s -X POST http://localhost:8080/students \
  -H "Content-Type: application/json" \
  -d '{"name": "Teste Retry", "email": "retry@puc.br"}' | jq .
```

**Esperado:** `201 Created` — API não sabe que o worker vai falhar.

### 5.3 — Observar os retries com backoff

```bash
docker compose logs -f worker-notification
```

**Esperado — sequência de retry visível:**

```
retry 1/3: NotificationProcessor: falha simulada para demo (FAIL_RATE=1)
retry 2/3: NotificationProcessor: falha simulada para demo (FAIL_RATE=1)
retry 3/3: NotificationProcessor: falha simulada para demo (FAIL_RATE=1)
```

Com delays de 1s, 2s e 4s entre as tentativas (backoff exponencial).

**O que prova:** retry automático com backoff — critério 4.5.

### 5.4 — Verificar mensagem na DLQ

No Kafka UI: `Topics → academic.events.dlq → Messages`

**Esperado — payload:**

```json
{
  "event_id": "dlq-academic.student.registered-0-<offset>",
  "original_topic": "academic.student.registered",
  "consumer_group": "academic-notification",
  "original_payload": "{... payload original completo ...}",
  "failure_reason": "NotificationProcessor: falha simulada para demo (FAIL_RATE=1)",
  "failed_at": "2026-06-04T...",
  "dlq_retry_count": 0
}
```

**O que prova:** mensagens que falham em todas as tentativas têm destino garantido (DLQ) — critério 4.5.

### 5.5 — Observar o worker-dlq reprocessando

```bash
docker compose logs -f worker-dlq
```

**Esperado — DLQ tentando republicar com backoff crescente:**

```
[DLQ] republicando para academic.student.registered (tentativa 1/3) aguardando 10s | event_id: dlq-...
[DLQ] republicando para academic.student.registered (tentativa 2/3) aguardando 20s | event_id: dlq-...
[DLQ] republicando para academic.student.registered (tentativa 3/3) aguardando 30s | event_id: dlq-...
[DLQ] EVENTO MORTO DEFINITIVAMENTE após 3 tentativas | topic: academic.student.registered | group: academic-notification | event_id: dlq-... | reason: ...
```

**O que prova:** DLQ tem lógica própria de reprocessamento com backoff crescente (10s, 20s, 30s) e descarte final após 3 tentativas — critério 4.5.

### 5.6 — Desligar FAIL_RATE e restaurar o ambiente

Edite o `.env`:

```
FAIL_RATE=
```

Recrie os workers:

```bash
docker compose up -d --force-recreate worker-notification worker-audit worker-report worker-dlq
```

---

## FASE 6 — Idempotência

> Prova que o mesmo evento não é processado duas vezes pelo mesmo consumer group, mesmo com retry.

### 6.1 — Cadastrar um aluno

```bash
curl -s -X POST http://localhost:8080/students \
  -H "Content-Type: application/json" \
  -d '{"name": "Idem Potente", "email": "idem@puc.br"}' | jq .
```

### 6.2 — Verificar a tabela de eventos processados

```bash
docker compose exec postgres psql -U academic -d academic \
  -c "SELECT event_id, consumer_group, processed_at FROM processed_events ORDER BY processed_at DESC LIMIT 6;"
```

**Esperado:**

```
 event_id  |     consumer_group      |      processed_at
-----------+-------------------------+--------------------------
 <uuid-1>  | academic-notification   | 2026-06-04 ...
 <uuid-1>  | academic-audit          | 2026-06-04 ...
```

O mesmo `event_id` aparece com consumer groups diferentes, mas **nunca repete** o mesmo par `(event_id, consumer_group)`.

**O que prova:** idempotência por `processed_events` — cada consumer group processa cada evento exatamente uma vez, mesmo que o offset seja reentregue pelo Kafka.

---

## FASE 7 — Outbox: atomicidade entre domínio e evento

> Prova que o evento nunca é perdido mesmo se o broker estiver lento.

### 7.1 — Ver a outbox no banco

```bash
docker compose exec postgres psql -U academic -d academic \
  -c "SELECT id, topic, published, created_at FROM outbox_events ORDER BY created_at DESC LIMIT 5;"
```

**Esperado em condições normais:**

```
 id      | topic                            | published | created_at
---------+----------------------------------+-----------+------------
 <uuid>  | academic.student.registered      | t         | 2026-06-04 ...
 <uuid>  | academic.enrollment.created      | t         | 2026-06-04 ...
```

- `published = t` significa que o relay já entregou para o Kafka
- Linhas com `published = f` só aparecem brevemente (dentro dos 200ms do ticker do relay)

**O que prova:** o Outbox Pattern garante que o evento é salvo na mesma transação que o domínio — se o broker estiver fora, o relay tenta novamente na próxima iteração, sem perder o evento.

---

## FASE 8 — Validações da API

> Cobre critério 5.4 (tratamento de erros adequado).

### 8.1 — Request com campo obrigatório faltando

```bash
curl -s -X POST http://localhost:8080/students \
  -H "Content-Type: application/json" \
  -d '{"name": "Sem Email"}' | jq .
```

**Esperado:** `400 Bad Request`

```json
{"error": "invalid request body"}
```

### 8.2 — Email duplicado

```bash
curl -s -X POST http://localhost:8080/students \
  -H "Content-Type: application/json" \
  -d '{"name": "Ana Costa Duplicada", "email": "ana@puc.br"}' | jq .
```

**Esperado:** `409 Conflict`

```json
{"error": "email already taken"}
```

### 8.3 — Swagger

Abrir no browser: **http://localhost:8080/swagger/index.html**

**Esperado:** interface do Swagger com `POST /students` e `POST /enrollments` documentados.

---

## FASE 9 — Encerramento limpo

### 9.1 — Derrubar tudo

```bash
docker compose down
```

**Esperado:** todos os containers param sem erro.

### 9.2 — Subir novamente do zero (simula o dia da apresentação)

```bash
docker compose up --build -d
docker compose logs worker-notification worker-audit worker-report worker-dlq
```

**Esperado:** workers sobem e logam `consumidor atualizado, sem mensagens pendentes` (sem estado da sessão anterior, já que os offsets e dados ficam nos volumes).

---

## Checklist rápida para o dia da apresentação

| # | Verificação | Comando / Ação | Esperado |
|---|---|---|---|
| 1 | Todos os containers up | `docker compose ps` | 8 containers `running` |
| 2 | Workers sem lag inicial | `docker compose logs worker-*` | "sem mensagens pendentes" |
| 3 | POST /students retorna 201 | curl | JSON com `id` imediatamente |
| 4 | Evento visível no Kafka UI | browser :8090 → Topics | payload com `event_id` e `published_at` |
| 5 | Workers logam latência | `docker compose logs -f` | `latência: Xms` em cada linha |
| 6 | API responde com worker parado | stop worker + curl | `201` normalmente |
| 7 | Lag aparece no Kafka UI | browser :8090 → Consumer Groups | `lag > 0` para o worker parado |
| 8 | Worker retoma acumulado | start worker + logs | "RETOMANDO APÓS INDISPONIBILIDADE" |
| 9 | FAIL_RATE=1 mostra retry | logs worker-notification | `retry 1/3`, `retry 2/3`, `retry 3/3` |
| 10 | DLQ recebe mensagem falhada | Kafka UI → academic.events.dlq | payload com `failure_reason` |
| 11 | DLQ descarta após 3 tentativas | `docker compose logs -f worker-dlq` | "EVENTO MORTO DEFINITIVAMENTE" |
| 12 | Email duplicado retorna 409 | curl com email repetido | `{"error": "email already taken"}` |
| 13 | Swagger acessível | browser :8080/swagger | endpoints documentados |

---

## Respostas para perguntas esperadas do professor

**"O que acontece se o broker cair?"**
O evento já foi salvo no Postgres na tabela `outbox_events` com `published = false`, na mesma transação que salvou o aluno/matrícula. O `OutboxRelay` tenta publicar a cada 200ms — quando o broker voltar, ele pega os eventos pendentes e publica. Nenhum dado é perdido.

**"O que acontece se o consumidor processar a mesma mensagem duas vezes?"**
A tabela `processed_events` funciona como guard com `INSERT ... ON CONFLICT DO NOTHING`. Se o mesmo par `(event_id, consumer_group)` já existe, o consumer pula a mensagem. O `ReleaseClaim` garante que, se o processamento falhou, a claim é liberada para que o retry funcione.

**"Como vocês garantem que a mensagem não é perdida?"**
Três camadas: (1) Outbox Pattern — evento persiste no Postgres antes de ir ao Kafka; (2) Kafka retém as mensagens por tópico com offset — consumer fora do ar não perde nada; (3) DLQ — mensagens que falham em todos os retries têm destino garantido em `academic.events.dlq`.

**"Como vocês escalariam para mais carga?"**
Kafka tem 3 partições por padrão (`KAFKA_NUM_PARTITIONS: 3`). Para escalar, sobe mais réplicas do mesmo worker — elas entram no mesmo consumer group e o Kafka distribui as partições automaticamente entre elas. Não muda nada no producer.

**"Por que Kafka e não RabbitMQ?"**
Consumer groups independentes: `notification`, `audit` e `report` leem o mesmo tópico cada um com seu offset próprio. Em RabbitMQ isso exigiria fanout exchange com filas separadas para cada consumer. Kafka também permite replay (reprocessar eventos antigos mudando o offset), o que RabbitMQ não faz nativamente.

**"Qual é o trade-off do processamento assíncrono?"**
Ganho: resposta HTTP imediata, workers escaláveis independentemente, falha isolada (notification cair não afeta audit). Custo: consistência eventual (o email de confirmação chega milissegundos depois, não na mesma chamada), complexidade operacional (DLQ, idempotência, monitoramento de lag) e debugging mais difícil (rastrear um evento pelo sistema requer correlação por `event_id`).

**"Como vocês detectariam uma mensagem travada na fila?"**
Kafka UI mostra o lag por consumer group em tempo real. Um lag crescendo sem zerar indica worker com problema. O `checkConsumerLag` no startup também loga o acumulado — ponto de partida para investigação.
