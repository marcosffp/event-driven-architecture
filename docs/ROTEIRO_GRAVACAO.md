# Roteiro de Gravação — Demonstração do Projeto Academic

> Roteiro para gravar o vídeo de apresentação do Trabalho Prático **"Arquitetura Orientada a Eventos e Mensageria"** (Arquitetura de Software, 5º período, PUC Minas — Prof. Filipe Tório Lopes Ruas Nhimi, 25 pontos).
>
> Cada bloco abaixo corresponde diretamente a um item de **"Requisitos Funcionais"** ou **"Métricas e Demonstrações Obrigatórias"** do enunciado (`docs/Arquitetura Orientada a Eventos e Mensageria.pdf`, páginas 7–8). Siga a ordem — ela conta uma história: ação síncrona → evento assíncrono → consumo → falhas → recuperação → discussão teórica.

---

## Como usar este roteiro

- **Fala sugerida** = texto que você pode ler/adaptar enquanto grava.
- **Mostrar na tela** = o que precisa estar visível (terminal, browser, Kafka UI).
- **Comando** = comando exato a digitar.
- **Esperado** = o que deve aparecer — se não aparecer, está descrito no "Plano B" do bloco.
- **Cobre (TP)** = referência ao item do PDF/`ACCEPTANCE_CRITERIA.md`.

Documentos de apoio (não precisam ser mostrados, mas servem de cola para você): `docs/MESSAGING.md` (explicação técnica completa), `commands/TESTING.md` (script de testes detalhado), `commands/ACCEPTANCE_CRITERIA.md` (checklist de avaliação).

---

## Visão geral / cronograma sugerido

| Bloco | Conteúdo | Duração aprox. | Cobre (TP) |
|---|---|---|---|
| 0 | Abertura: cenário e objetivo | 1 min | Contexto |
| 1 | Arquitetura geral (diagrama + camadas) | 3 min | 5.1, 5.3 |
| 2 | Subir o ambiente — estrutura mínima | 1 min | Estrutura mínima (produtor/broker/consumidor) |
| 3 | Ação principal via API | 1–2 min | 1.1, 4.1 |
| 4 | Publicação automática + fila recebendo mensagens (Kafka UI) | 2 min | 1.2, 3.3, 4.1, 4.3 |
| 5 | Consumo assíncrono + latência | 2 min | 1.3, 1.4, 4.2, 4.6 |
| 6 | Desacoplamento: produtor sem consumidor | 1–2 min | 1.5 |
| 7 | Consumidor indisponível: acúmulo e retomada | 2–3 min | 4.4 |
| 8 | Retry com backoff + Dead Letter Queue | 3–4 min | 4.5 |
| 9 | Idempotência (bônus) | 1–2 min | 5.7 |
| 10 | Outbox Pattern + consistência eventual | 2 min | 3.3, 4.7 |
| 11 | Discussão final: trade-offs e perguntas do professor | 2–3 min | 3.3, 4.7, 5.5, 5.7 |
| 12 | Encerramento | 30 s | — |

**Total estimado: ~22–28 minutos.** Se precisar cortar, os blocos 9 e 11 são os mais fáceis de resumir (mas não pular — são os que mais pontuam em "qualidade da mensageria" e "compreensão dos trade-offs").

---

## Preparação antes de gravar (checklist)

1. **Resetar o ambiente para um estado limpo** (opcional, mas recomendado para a gravação ficar "redonda"):
   ```bash
   docker compose down -v
   ```
   Isso remove os volumes (Postgres e Kafka zerados) — a gravação começa sem dados de sessões anteriores.

2. **Confirmar o `.env`:**
   ```bash
   cp .env.example .env
   ```
   Confirme que `FAIL_RATE=` está **vazio** (ele só será ativado no Bloco 8).

3. **Subir tudo já antes de começar a gravar** (o build pode demorar — não precisa aparecer no vídeo):
   ```bash
   docker compose up --build -d
   docker compose ps
   ```
   Espere todos os 8 serviços ficarem `running`/`healthy`.

4. **Layout de tela sugerido** (grave em tela cheia ou com janelas organizadas):
   - **Terminal 1** (esquerda, grande): para os comandos `curl` e `docker compose`.
   - **Terminal 2** (direita, embaixo): `docker compose logs -f` dos workers — sempre visível.
   - **Browser aba 1**: Kafka UI — `http://localhost:8090`.
   - **Browser aba 2**: Swagger — `http://localhost:8080/swagger/index.html`.
   - Tenha o **diagrama de arquitetura** (do `README.md` ou `commands/arquitetura.md`) aberto para mostrar no Bloco 1 — pode ser print de tela ou o markdown renderizado.

5. **Use `jq`** para formatar JSON (`brew install jq` se não tiver) — deixa as respostas mais legíveis no vídeo.

---

## BLOCO 0 — Abertura: cenário e objetivo (1 min)

**Mostrar na tela:** seu rosto/slide de título ou o `README.md`.

**Fala sugerida:**

> "Este trabalho implementa uma plataforma acadêmica orientada a eventos. O cenário é o do enunciado: depois que um aluno se cadastra ou faz uma matrícula, o sistema precisa disparar tarefas secundárias — notificação, auditoria e relatório — sem bloquear a resposta da API. O foco não é só 'mandar uma mensagem', mas demonstrar como a troca assíncrona de eventos reduz acoplamento, melhora escalabilidade e disponibilidade, e como o sistema se recupera de falhas. Vamos mostrar a arquitetura, depois o fluxo funcionando ao vivo, incluindo os cenários de falha exigidos."

---

## BLOCO 1 — Arquitetura geral (3 min)

**Cobre (TP):** 5.1 (comunicação clara), 5.3 (linguagem técnica e justificativa do desenho)

**Mostrar na tela:** diagrama de arquitetura (do `README.md`, seção "Arquitetura").

**Fala sugerida:**

> "A stack é Go, Apache Kafka como broker e PostgreSQL como banco, tudo orquestrado via Docker Compose — um único `docker compose up` sobe a infraestrutura inteira.
>
> A aplicação segue Clean Architecture: a API recebe a requisição HTTP, o usecase monta a entidade de domínio e o evento, e o repositório salva os dois — entidade e evento — **na mesma transação** do Postgres. Isso é o **Outbox Pattern**: o evento nunca existe sem o dado, e vice-versa.
>
> Uma goroutine chamada `OutboxRelay`, dentro do próprio processo da API, varre essa tabela de outbox a cada 200ms e publica no Kafka o que ainda não foi publicado.
>
> Do lado do consumo, temos quatro workers independentes, cada um seu próprio **consumer group** do Kafka:
> - `worker-notification` — simula envio de notificação ao aluno
> - `worker-audit` — registra auditoria
> - `worker-report` — gera relatório, só para matrículas
> - `worker-dlq` — processa a Dead Letter Queue
>
> Cada consumer group tem seu próprio offset — ou seja, o `notification` pode estar atrasado sem afetar o `audit`. Isso é o desacoplamento real que a mensageria promete: o produtor (a API) nunca chama esses workers diretamente, ele só publica no tópico. Adicionar, remover ou escalar um worker não muda uma linha sequer da API."

**Pontos-chave a citar (vocabulário técnico avaliado em 5.1/5.3):** *broker*, *tópico*, *partição*, *consumer group*, *offset*, *Pub/Sub*, *Outbox Pattern*, *at-least-once*.

---

## BLOCO 2 — Subir o ambiente / estrutura mínima (1 min)

**Cobre (TP):** "Estrutura mínima da solução" (produtor + broker + consumidor)

**Mostrar na tela:** Terminal 1.

**Comando:**
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

**Fala sugerida:**

> "Aqui está a estrutura mínima exigida pelo trabalho: o **produtor** é a API Go; o **broker** é o Apache Kafka, rodando em modo KRaft sem precisar de Zookeeper; e os **consumidores** são esses quatro workers. Todos sobem com `docker compose up --build` — nenhuma dependência extra na máquina além do Docker."

Em seguida, mostre os logs de inicialização dos workers:

```bash
docker compose logs worker-notification worker-audit worker-report worker-dlq
```

**Esperado:** uma linha por worker:
```
[academic-notification] consumidor atualizado, sem mensagens pendentes
[academic-audit] consumidor atualizado, sem mensagens pendentes
[academic-report] consumidor atualizado, sem mensagens pendentes
[academic-dlq] consumidor atualizado, sem mensagens pendentes
```

> "Essa checagem de lag no startup já é a primeira prova de que o sistema rastreia o quanto cada consumer group está atrasado em relação ao tópico — vamos usar isso de novo no bloco de indisponibilidade."

Abra o **Kafka UI** (`http://localhost:8090`) e mostre rapidamente o cluster `academic` e a aba `Consumer Groups` (ainda vazios/zero lag).

---

## BLOCO 3 — Ação principal via API (1–2 min)

**Cobre (TP):** 1.1 (endpoint síncrono, não bloqueante), 4.1 (publicação ao vivo)

**Mostrar na tela:** Terminal 1 + (opcional) Swagger.

**Fala sugerida:**

> "Vamos disparar a ação principal: cadastrar um aluno via `POST /students`. Reparem no tempo de resposta — a API responde imediatamente, antes de qualquer notificação, auditoria ou relatório acontecer."

**Comando:**
```bash
curl -s -X POST http://localhost:8080/students \
  -H "Content-Type: application/json" \
  -d '{"name": "Ana Costa", "email": "ana@puc.br"}' | jq .
```

**Esperado:** `201 Created` imediato:
```json
{
  "id": "<uuid>",
  "name": "Ana Costa",
  "email": "ana@puc.br",
  "created_at": "2026-06-09T..."
}
```

> Anote/copie o `id` retornado — você vai usá-lo no próximo comando.

Agora crie a matrícula (segunda ação principal):

```bash
curl -s -X POST http://localhost:8080/enrollments \
  -H "Content-Type: application/json" \
  -d '{"student_id": "<ID_DO_ALUNO>", "course_id": "curso-arq-software"}' | jq .
```

**Esperado:** `201 Created` com `id`, `student_id`, `course_id`, `created_at`.

**Fala sugerida (fechando o bloco):**

> "Os dois endpoints responderam em milissegundos, com `201 Created`. A requisição principal não esperou nenhuma tarefa secundária — exatamente o requisito 1.1 do enunciado. Por baixo dos panos, o aluno e a matrícula já foram salvos no Postgres **junto com o evento correspondente, na mesma transação**. Vamos ver isso no Kafka agora."

(Opcional) Mostre rapidamente o Swagger em `http://localhost:8080/swagger/index.html` para evidenciar a documentação da API.

---

## BLOCO 4 — Publicação automática + fila recebendo mensagens (2 min)

**Cobre (TP):** 1.2 (publicação automática com dados do domínio), 3.3 (envio/armazenamento/consumo), 4.1, 4.3 (fila/tópico observável)

**Mostrar na tela:** Browser — Kafka UI.

**Fala sugerida:**

> "Sem nenhuma intervenção manual, o `OutboxRelay` — que roda a cada 200ms dentro da API — já publicou os dois eventos no Kafka. Vamos ver no Kafka UI."

No Kafka UI:
1. `Topics → academic.student.registered → Messages`
2. Abra a mensagem mais recente.

**Esperado — payload:**
```json
{
  "event_id": "<uuid>",
  "student_id": "<uuid>",
  "name": "Ana Costa",
  "email": "ana@puc.br",
  "published_at": "2026-06-09T..."
}
```

> "Reparem: o evento carrega os dados do domínio — nome, email, IDs — e um `published_at`, que vamos usar daqui a pouco para medir a latência. Cada tópico tem 3 partições, configuradas via `KAFKA_NUM_PARTITIONS=3` — isso é o que permite escalar até 3 instâncias de cada worker em paralelo, sem duplicar processamento."

Repita para `academic.enrollment.created`.

**Fala sugerida:**

> "Esse é o requisito principal de mensageria: **envio**, **armazenamento temporário no broker** — a mensagem fica retida no tópico, com um offset, mesmo que ninguém tenha consumido ainda — e, no próximo bloco, o **consumo**."

---

## BLOCO 5 — Consumo assíncrono + latência (2 min)

**Cobre (TP):** 1.3 (consumidor separado), 1.4 (background sem bloquear), 4.2 (consumo ao vivo), 4.6 (latência visível)

**Mostrar na tela:** Terminal 2, com logs ao vivo.

**Comando:**
```bash
docker compose logs -f worker-notification worker-audit worker-report
```

Se os eventos do Bloco 3 já tiverem sido processados, dispare uma **nova** ação principal para capturar o log ao vivo:

```bash
curl -s -X POST http://localhost:8080/students \
  -H "Content-Type: application/json" \
  -d '{"name": "Bruno Lima", "email": "bruno@puc.br"}' | jq .id
```

**Esperado nos logs:**
```
worker-notification-1  | [notification] student.registered | email: bruno@puc.br | latência: 312ms
worker-audit-1         | [audit] student.registered | id: <uuid> | latência: 318ms
```

Se já tiver feito o `enrollment` também:
```
worker-notification-1  | [notification] enrollment.created | student: <uuid> | course: curso-arq-software | latência: 205ms
worker-audit-1         | [audit] enrollment.created | id: <uuid> | student: <uuid> | latência: 210ms
worker-report-1        | [report] enrollment.created | id: <uuid> | student: <uuid> | course: curso-arq-software | latência: 198ms
```

**Fala sugerida:**

> "Aqui temos a prova viva do consumo assíncrono. Reparem três coisas:
>
> Primeiro, `worker-notification` e `worker-audit` processaram **o mesmo evento**, de forma independente — são dois consumer groups diferentes lendo o mesmo tópico, cada um com seu próprio offset.
>
> Segundo, `worker-report` só aparece para `enrollment.created` — ele está inscrito apenas nesse tópico, não em `student.registered`. Isso é o `cmd/worker/main.go` decidindo, via variável de ambiente `PROCESSOR`, quais tópicos cada consumer group assina.
>
> Terceiro — e isso responde diretamente ao requisito 4.6 — cada linha de log mostra `latência: Xms`. Esse valor é `time.Since(event.PublishedAt)`, calculado pelo próprio worker: a diferença entre o momento em que o evento foi publicado no Kafka e o momento em que o worker terminou de processá-lo. É a métrica de tempo entre publicação e processamento, exigida na demonstração."

Abra o Kafka UI → `Consumer Groups` e mostre `academic-notification`, `academic-audit`, `academic-report` com **lag = 0**.

> "Lag zero significa que cada grupo está em dia com o tópico."

---

## BLOCO 6 — Desacoplamento: produtor sem consumidor (1–2 min)

**Cobre (TP):** 1.5 (produtor não conhece consumidor; continua funcionando sem ele)

**Mostrar na tela:** Terminal 1.

**Fala sugerida:**

> "Agora vamos provar o desacoplamento na prática: vou derrubar **todos** os workers e mostrar que a API continua funcionando normalmente — porque ela nunca chamou esses processos diretamente, só publicou num tópico Kafka."

**Comando:**
```bash
docker compose stop worker-notification worker-audit worker-report worker-dlq
```

```bash
curl -s -X POST http://localhost:8080/students \
  -H "Content-Type: application/json" \
  -d '{"name": "Teste Desacoplado", "email": "desacoplado@puc.br"}' | jq .
```

**Esperado:** `201 Created`, idêntico aos anteriores — resposta imediata.

**Fala sugerida:**

> "`201 Created`, na mesma velocidade de sempre. A API não sabe — e não precisa saber — que não há nenhum consumidor vivo agora. O evento foi salvo no outbox e será publicado pelo relay normalmente. No próximo bloco vamos religar só um desses workers e mostrar o que acontece com as mensagens que se acumularam."

Religue **apenas o `worker-audit`** e **`worker-report`** agora (deixe `worker-notification` parado de propósito para o próximo bloco):

```bash
docker compose start worker-audit worker-report worker-dlq
```

---

## BLOCO 7 — Consumidor indisponível: acúmulo e retomada (2–3 min)

**Cobre (TP):** 4.4 (acúmulo, retomada sem perda, API continua respondendo)

**Mostrar na tela:** Terminal 1 (comandos) + Browser (Kafka UI) + Terminal 2 (logs, depois).

**Fala sugerida:**

> "`worker-notification` continua parado. Vou disparar três cadastros novos com ele fora do ar."

**Comando:**
```bash
for i in 1 2 3; do
  curl -s -X POST http://localhost:8080/students \
    -H "Content-Type: application/json" \
    -d "{\"name\": \"Aluno $i\", \"email\": \"aluno${i}@puc.br\"}" | jq .id
done
```

**Esperado:** três UUIDs, sem erro.

> "API respondeu normalmente para os três. Vamos ver o que aconteceu no Kafka."

No Kafka UI → `Consumer Groups → academic-notification`:

**Esperado:** `lag: 3` (ou mais).

> "O grupo `academic-notification` está com lag 3 — três mensagens publicadas no tópico que ele ainda não consumiu. Reparem que `academic-audit` e `academic-report` continuam com `lag: 0` — eles processaram normalmente, porque têm offsets independentes. A indisponibilidade de um consumer group não afeta os outros."

Agora religue o worker e mostre a retomada:

```bash
docker compose start worker-notification
docker compose logs -f worker-notification
```

**Esperado — primeiras linhas:**
```
[academic-notification] *** RETOMANDO APÓS INDISPONIBILIDADE: 3 mensagens acumuladas aguardando processamento ***
[notification] student.registered | email: aluno1@puc.br | latência: ...
[notification] student.registered | email: aluno2@puc.br | latência: ...
[notification] student.registered | email: aluno3@puc.br | latência: ...
```

**Fala sugerida:**

> "Ao subir, o `checkConsumerLag` detecta que existem 3 mensagens pendentes e loga isso explicitamente. Em seguida, o consumer retoma exatamente do offset onde parou e processa as três mensagens acumuladas — nenhuma foi perdida. Isso só é possível porque o Kafka **persiste** as mensagens por offset, ao contrário de, por exemplo, Redis Pub/Sub, onde uma mensagem publicada sem consumidor ativo se perde para sempre."

Confirme no Kafka UI que o lag de `academic-notification` voltou para `0`.

---

## BLOCO 8 — Retry com backoff + Dead Letter Queue (3–4 min)

**Cobre (TP):** 4.5 (retry automático, política de tentativas/backoff, destino para falhas — DLQ)

**Mostrar na tela:** Terminal 1 (comandos) + Terminal 2 (logs) + Kafka UI.

**Fala sugerida:**

> "Para demonstrar retry e Dead Letter Queue sem depender de uma falha real, o projeto tem uma variável `FAIL_RATE`: quando igual a `1`, todo processador retorna erro propositalmente em 100% das mensagens. Vou ativar isso agora."

**Comando — editar `.env`:**
```
FAIL_RATE=1
```

**Comando — recriar os workers com a nova variável:**
```bash
docker compose up -d --force-recreate worker-notification worker-audit worker-report worker-dlq
```

Disparar uma ação:
```bash
curl -s -X POST http://localhost:8080/students \
  -H "Content-Type: application/json" \
  -d '{"name": "Teste Retry", "email": "retry@puc.br"}' | jq .
```

**Esperado:** `201 Created` normalmente — "a API não sabe que o worker vai falhar."

Acompanhar os logs:
```bash
docker compose logs -f worker-notification
```

**Esperado — sequência de retry:**
```
retry 1/3: NotificationProcessor: falha simulada para demo (FAIL_RATE=1)
retry 2/3: NotificationProcessor: falha simulada para demo (FAIL_RATE=1)
retry 3/3: NotificationProcessor: falha simulada para demo (FAIL_RATE=1)
```

**Fala sugerida:**

> "Três tentativas, com backoff exponencial: 1 segundo, depois 2, depois 4. Esse delay crescente evita que, em caso de sobrecarga real do banco ou de um serviço externo, dezenas de instâncias retentem todas ao mesmo tempo e piorem o problema. Depois da terceira falha, o evento é encaminhado para a Dead Letter Queue."

Mostre a mensagem na DLQ pelo Kafka UI: `Topics → academic.events.dlq → Messages`.

**Esperado — payload:**
```json
{
  "event_id": "dlq-academic.student.registered-0-<offset>",
  "original_topic": "academic.student.registered",
  "consumer_group": "academic-notification",
  "original_payload": "{... payload original completo ...}",
  "failure_reason": "NotificationProcessor: falha simulada para demo (FAIL_RATE=1)",
  "failed_at": "2026-06-09T...",
  "dlq_retry_count": 0
}
```

> "A DLQ guarda o tópico original, o consumer group que falhou, o motivo e — fundamental — o **payload original intacto**, para que possa ser reprocessado depois."

Acompanhe o `worker-dlq`:
```bash
docker compose logs -f worker-dlq
```

**Esperado:**
```
[DLQ] republicando para academic.student.registered (tentativa 1/3) aguardando 10s | event_id: dlq-...
[DLQ] republicando para academic.student.registered (tentativa 2/3) aguardando 20s | event_id: dlq-...
[DLQ] republicando para academic.student.registered (tentativa 3/3) aguardando 30s | event_id: dlq-...
[DLQ] EVENTO MORTO DEFINITIVAMENTE após 3 tentativas | topic: academic.student.registered | group: academic-notification | event_id: dlq-... | reason: ...
```

**Fala sugerida:**

> "O `worker-dlq` tem sua própria política: ele republica o evento no tópico original com backoff crescente — 10, 20, 30 segundos — incrementando um contador `dlq-retry-count` que viaja como **header** da mensagem Kafka, sem tocar no payload original. Cada vez que republica, o ciclo completo de 3 tentativas roda de novo no `worker-notification`. Depois de 3 ciclos completos — ou seja, até **12 tentativas no total** — o evento é descartado definitivamente e fica registrado no log como 'EVENTO MORTO DEFINITIVAMENTE'. Esse é o destino final garantido para mensagens que falham persistentemente."

> *(Dica de gravação: o ciclo completo de DLQ leva minutos por causa dos backoffs de 10/20/30s. Para o vídeo, mostre o início de pelo menos um ciclo de republicação ao vivo e, se necessário, corte/acelere até o log "EVENTO MORTO DEFINITIVAMENTE", explicando verbalmente que os backoffs foram acelerados na edição.)*

**Restaurar o ambiente:**

Edite `.env`:
```
FAIL_RATE=
```

```bash
docker compose up -d --force-recreate worker-notification worker-audit worker-report worker-dlq
```

---

## BLOCO 9 — Idempotência (1–2 min, bônus)

**Cobre (TP):** 5.7 (solução vai além do mínimo — idempotência), resposta à pergunta "e se a mesma mensagem for processada duas vezes?"

**Mostrar na tela:** Terminal 1.

**Fala sugerida:**

> "Um detalhe que vai além do mínimo exigido: o Kafka garante *at-least-once delivery* — uma mensagem pode, em certas situações (crash do worker antes do commit, rebalanceamento de partição), ser entregue mais de uma vez. Para não duplicar notificações ou registros de auditoria, cada worker reivindica o evento atomicamente antes de processar."

**Comando:**
```bash
docker compose exec postgres psql -U academic -d academic \
  -c "SELECT event_id, consumer_group, processed_at FROM processed_events ORDER BY processed_at DESC LIMIT 6;"
```

**Esperado:**
```
 event_id  |     consumer_group      |      processed_at
-----------+-------------------------+--------------------------
 <uuid-1>  | academic-notification   | 2026-06-09 ...
 <uuid-1>  | academic-audit          | 2026-06-09 ...
```

**Fala sugerida:**

> "Essa tabela tem chave primária composta `(event_id, consumer_group)`. A operação que insere aqui é um único `INSERT ... ON CONFLICT DO NOTHING` — atômico, sem round-trip de 'verificar e depois marcar', que criaria uma janela de corrida entre múltiplas instâncias do mesmo worker. O mesmo `event_id` aparece para `academic-notification` **e** `academic-audit` — grupos diferentes processam o mesmo evento de forma independente — mas o par `(event_id, consumer_group)` nunca se repete. Se o Kafka reentregar essa mensagem para o mesmo grupo, o `TryClaim` retorna `false`, o worker loga `'evento já processado, pulando'` e segue sem reprocessar."

---

## BLOCO 10 — Outbox Pattern + consistência eventual (2 min)

**Cobre (TP):** 3.3 (armazenamento/consumo + explicação de consistência eventual e falhas), 4.7

**Mostrar na tela:** Terminal 1.

**Comando:**
```bash
docker compose exec postgres psql -U academic -d academic \
  -c "SELECT id, topic, published, created_at FROM outbox_events ORDER BY created_at DESC LIMIT 5;"
```

**Esperado:**
```
 id      | topic                            | published | created_at
---------+----------------------------------+-----------+------------
 <uuid>  | academic.student.registered      | t         | 2026-06-09 ...
 <uuid>  | academic.enrollment.created      | t         | 2026-06-09 ...
```

**Fala sugerida:**

> "Essa é a tabela do Outbox Pattern. `published = true` significa que o `OutboxRelay` já entregou ao Kafka. A linha `f` só existe por, no máximo, 200ms — o intervalo do ticker do relay.
>
> Vamos falar de consistência eventual: no instante `t=0`, o `POST /students` responde `201` assim que o aluno **e** o evento estão salvos juntos, na mesma transação Postgres. Em `t≈200ms`, o relay publica no Kafka. Em `t≈200-400ms`, os workers processam — e é aí que a notificação, a auditoria e o relatório realmente acontecem.
>
> Ou seja: o dado principal está consistente imediatamente; os efeitos colaterais (notificação, auditoria, relatório) ficam **eventualmente** consistentes — vão acontecer, mas não no mesmo instante da resposta HTTP.
>
> E os cenários de falha:
> - **Se o broker (Kafka) cair:** o evento já está seguro na tabela `outbox_events` com `published = false`. O relay tenta a cada 200ms; quando o Kafka voltar, publica normalmente. Nada se perde.
> - **Se o consumidor falhar durante o processamento:** entra o retry com backoff que vimos no Bloco 8; se persistir, vai para a DLQ.
> - **Se o produtor (a API) morrer logo após responder `201`:** o evento já está na mesma transação do dado principal — o `OutboxRelay`, ao reiniciar, encontra a linha `published = false` e publica normalmente."

---

## BLOCO 11 — Discussão final: trade-offs e perguntas do professor (2–3 min)

**Cobre (TP):** 3.3 (explicações obrigatórias: acoplamento, disponibilidade, escalabilidade, rastreabilidade, falhas, consistência eventual), 5.5, 5.7

**Mostrar na tela:** você (fala direta) — pode usar o diagrama de novo como apoio visual.

**Fala sugerida — cubra cada um destes pontos, na ordem:**

**1. Acoplamento**
> "Produtor e consumidores se comunicam **só** pelo contrato JSON dos eventos em `internal/events/event.go` — nunca por chamada direta. Adicionar o `worker-report`, por exemplo, não exigiu nenhuma mudança na API."

**2. Disponibilidade**
> "A API continua respondendo mesmo com todos os workers fora — vimos isso no Bloco 6. E o Kafka retém mensagens por até 7 dias (configuração padrão) mesmo sem consumidores ativos."

**3. Escalabilidade**
> "Cada tópico tem 3 partições. Até 3 instâncias de um mesmo consumer group processam em paralelo — o Kafka distribui as partições automaticamente. `make scale-notification n=3` sobe 3 réplicas sem tocar nos outros serviços. Acima de 3 instâncias, as extras ficam ociosas — esse é o limite de paralelismo por partição, e é um trade-off consciente."

**4. Rastreabilidade**
> "Todo evento tem um `event_id` (UUID) gerado no usecase. Esse ID aparece no payload publicado, na tabela `processed_events` de cada consumer group e, se a mensagem falhar, no `DeadLetterEvent`. Dá para seguir o rastro de qualquer evento do início ao fim."

**5. Tratamento de falhas**
> "Três camadas: Outbox Pattern (evento nunca se perde antes do Kafka), retry com backoff exponencial (1s/2s/4s) e Dead Letter Queue com auto-reprocessamento em até 3 ciclos (10s/20s/30s) — 12 tentativas no total antes do descarte definitivo."

**6. Consistência eventual**
> "Já detalhamos no bloco anterior: dado principal consistente na hora; efeitos colaterais eventualmente consistentes."

**Perguntas esperadas — respostas rápidas:**

| Pergunta do professor | Resposta direta |
|---|---|
| **Por que Kafka e não RabbitMQ?** | Consumer groups independentes lendo o mesmo tópico com offsets próprios — em RabbitMQ precisaria de fanout exchange + filas separadas por consumidor. Kafka também permite replay. |
| **Por que Outbox Pattern?** | Sem ele, a API salvaria o dado e publicaria no Kafka em duas operações separadas — se morresse entre as duas, o evento se perderia silenciosamente. Com outbox, as duas coisas são atômicas. |
| **Como tratam idempotência?** | `TryClaim` com `INSERT ... ON CONFLICT DO NOTHING` na tabela `processed_events`, chave composta `(event_id, consumer_group)` — atômico, sem race condition. |
| **Como escalariam para mais carga?** | Mais réplicas do mesmo worker, mesmo consumer group — Kafka redistribui partições automaticamente. Producer não muda nada. |
| **Trade-off do processamento assíncrono?** | Resposta HTTP imediata e falhas isoladas por worker, ao custo de consistência eventual, mais complexidade operacional (DLQ, idempotência, monitoramento de lag) e debugging via correlação por `event_id`. |
| **Como detectam mensagem travada na fila?** | Kafka UI mostra lag por consumer group em tempo real; `checkConsumerLag` também loga no startup. |

---

## BLOCO 12 — Encerramento (30 s)

**Fala sugerida:**

> "Resumindo: implementamos produtor (API Go), broker (Kafka com 3 tópicos e 3 partições cada) e quatro consumidores independentes, cobrindo publicação, consumo assíncrono, indisponibilidade de consumidor, retry com backoff, Dead Letter Queue, idempotência e Outbox Pattern — com latência de publicação-a-processamento visível em cada log. Toda a documentação técnica detalhada está em `docs/MESSAGING.md`. Obrigado!"

(Opcional, fora da gravação) Restaurar o ambiente:
```bash
docker compose down
```

---

## Apêndice — Mapeamento rápido requisito → bloco

| Item do PDF (`docs/Arquitetura Orientada a Eventos e Mensageria.pdf`) | Bloco |
|---|---|
| Executar ação principal via API | 3 |
| Publicar mensagem/evento após ação principal | 4 |
| Consumir mensagem de forma assíncrona | 5 |
| Processar tarefa em segundo plano | 5 |
| Demonstrar desacoplamento produtor/consumidor | 6 |
| Estrutura mínima (produtor + broker + consumidor) | 2 |
| Publicação de mensagens | 4 |
| Consumo assíncrono | 5 |
| Fila/tópico recebendo mensagens | 4, 7 |
| Comportamento com consumidor indisponível | 7 |
| Reprocessamento/retry de mensagens | 8 |
| Tempo entre publicação e processamento | 5 |
| Discussão sobre consistência eventual e falhas | 10, 11 |

## Apêndice — Plano B (se algo não aparecer durante a gravação)

- **Mensagem não aparece no Kafka UI em até 1s:** o `OutboxRelay` roda a cada 200ms — espere até 1s e dê refresh na lista de mensagens.
- **Lag não atualiza no Kafka UI:** dê refresh na página; o Kafka UI faz polling, não é tempo real.
- **`docker compose up -d --force-recreate` demora:** já é esperado — ele recompila a imagem do worker. Pode cortar esse tempo na edição.
- **Ciclo completo da DLQ (12 tentativas) é muito longo para o vídeo:** mostre o primeiro ciclo completo ao vivo (3 retries internos + 1 republicação da DLQ) e explique verbalmente o restante do ciclo, citando a tabela do Bloco 8.
- **Quer recomeçar do zero antes de gravar de novo:** `docker compose down -v` apaga os volumes (Postgres e Kafka) e volta tudo ao estado inicial.
