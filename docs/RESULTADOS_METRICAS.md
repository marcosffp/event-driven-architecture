# Resultados das Métricas — Coleta Real (10/06/2026)

> Dados coletados seguindo o [`ROTEIRO_METRICAS.md`](./ROTEIRO_METRICAS.md), a partir dos logs brutos em `DADOS_CAPTURADOS.MD` e `dados_capturados_parte_2.md`. Pronto para colar nos slides de "Métricas".

---

## 📊 Tabela-resumo (slide final)

| Categoria | Métrica | Valor coletado |
|---|---|---|
| Latência síncrona | Resposta média da API (`POST /students`) | **4,03 ms** (min 2,60 / máx 7,32 ms, n=20) |
| Latência fim-a-fim | `academic-notification` (student.registered, média / p95) | **17 899,55 ms / 33 948,30 ms** (n=50) |
| Latência fim-a-fim | `academic-audit` (student.registered, média / p95) | **17 899,47 ms / 33 954,30 ms** (n=50) |
| Latência fim-a-fim | `academic-report` (enrollment.created, média / p95) | **34 590,09 ms / 61 331,71 ms** (n=30) |
| Latência fim-a-fim | `academic-audit` (enrollment.created, média / p95) | **34 592,68 ms / 61 326,05 ms** (n=30) |
| Throughput | Produção (API) | **470,2 req/s** (50 requisições em 0,106 s) |
| Throughput | Consumo (1 instância, burst de 50) | **≈ 0,92 msg/s** (drenagem em ~54,2 s) |
| Indisponibilidade | Pico de lag acumulado | **5 mensagens** |
| Indisponibilidade | Tempo de catch-up (lag → 0) | **~3 s** (Kafka) / ~12,7 s (E2E completo) |
| Retry | Intervalo retry 1→2→3 | esperado 1s/2s/4s — **medido 1s / 2s / 4s** ✅ |
| DLQ | Backoff ciclos 1/2/3 | esperado 10s/20s/30s — **medido 11s / 21s / 31s** ✅ |
| DLQ | Tempo total até reentrega bem-sucedida | **~144 s** (2min24s) |
| Escalabilidade | 1 vs 3 instâncias (`academic-notification`) | ver nota — partições distribuídas em **3 consumers distintos** ✅ |
| Idempotência | Publicados vs. processados (`academic-report` / `academic-audit`) | 30 vs. 30 e 305 vs. 305 (iguais) ✅ |
| Idempotência | Reset de offset (`--to-earliest`, 275 mensagens reentregues) | **120 duplicatas bloqueadas** (`já processado, pulando`) + 155 novas processadas → `academic-notification` 150→305, paridade com `academic-audit` ✅ |

---

## Métrica 1 — Latência de resposta da API (síncrona)

20 requisições sequenciais a `POST /students`:

| Métrica | Valor |
|---|---|
| Requisições | 20 |
| Latência mínima | 2,60 ms |
| Latência média | **4,03 ms** |
| Latência máxima | 7,32 ms |

> ✅ **Ponto a destacar:** resposta na casa de **milissegundos**, totalmente independente do estado dos workers/Kafka — a transação é só `INSERT` na entidade + outbox no Postgres.

---

## Métrica 2 — Latência fim-a-fim (publicação → processamento)

### `student.registered` — `academic-notification` / `academic-audit`

50 eventos `student.registered` foram observados (20 da rajada da Métrica 1 + 30 disparados especificamente para esta métrica), processados em lotes pelo `worker-notification`/`worker-audit`:

| Worker / Consumer Group | n | Mín | Média | p95 | Máx |
|---|---|---|---|---|---|
| `academic-notification` | 50 | 5 394,42 ms | **17 899,55 ms** | 33 948,30 ms | 33 978,60 ms |
| `academic-audit` | 50 | 5 394,61 ms | **17 899,47 ms** | 33 954,30 ms | 33 985,50 ms |

> ✅ **Ponto a destacar:** `notification` e `audit` processam o **mesmo evento em paralelo** com latências praticamente idênticas (diferença < 1 ms na média) — prova de que múltiplos consumer groups independentes recebem a mesma mensagem sem se bloquear.
>
> ⚠️ **Nota sobre os valores altos (5s–34s):** os eventos foram disparados em rajada (sem espaçamento), e o worker os processa em lotes sucessivos — por isso a latência cresce em "degraus" (5s → 14s → 23s → 7s → 16s → 25s → 34s) conforme a fila se acumula. Isso **ainda comprova o critério 4.6** (processamento assíncrono, medido via `time.Since(event.PublishedAt)`), só que sob uma carga mais agressiva do que o cenário "estado estável".

### `enrollment.created` — `academic-report` / `academic-audit`

Com o payload corrigido (`student_id`/`course_id` em snake_case + UUID real do aluno), 30 matrículas foram disparadas em rajada e processadas por `worker-report` e `worker-audit`:

| Worker / Consumer Group | n | Mín | Média | p95 | Máx |
|---|---|---|---|---|---|
| `academic-report` | 30 | 7 836,63 ms | **34 590,09 ms** | 61 331,71 ms | 61 365,76 ms |
| `academic-audit` (enrollment.created) | 30 | 7 843,36 ms | **34 592,68 ms** | 61 326,05 ms | 61 371,66 ms |

> ✅ **Ponto a destacar:** mesma história do `student.registered` — `report` e `audit` processam o **mesmo evento `enrollment.created` em paralelo**, com médias praticamente idênticas (diferença ~2,6 ms). Confirma que o critério 4.6 também vale para o segundo tipo de evento do sistema, não só para `student.registered`. As 30 matrículas foram confirmadas como **30/30 processadas** em `processed_events` (ver Métrica 8).
>
> ⚠️ **Nota sobre os valores altos (7,8s–61,4s):** mesmo padrão da rajada de `student.registered` — sem espaçamento entre requisições, o worker processa em lotes crescentes (degraus de ~7,8s → 16,7s → 25,6s → 34,5s → 43,5s → 52,4s → 61,3s). Os valores acima de 60s aparecem no log do Go como `1m1.3xxs`; o script de normalização do roteiro original não reconhecia esse formato (assumia sempre `<seg>s`), gerando `min=0,00ms` por engano — corrigido nesta rodada via reprocessamento manual dos 30 valores.

---

## Métrica 3 — Throughput (mensagens por segundo)

### Produção (API)

| Métrica | Valor |
|---|---|
| Requisições enviadas | 50 (paralelas) |
| Tempo total | 0,106 s |
| **Throughput de produção** | **470,2 req/s** |

### Consumo (1 instância)

A última mensagem da rajada de 50 (`carga_42`) foi processada com latência de **54,23 s**, o que define a janela de drenagem total:

| Métrica | Valor |
|---|---|
| Mensagens | 50 |
| Tempo até lag zerar (`academic-notification`) | ~54,2 s |
| **Throughput de consumo (1 instância)** | **≈ 0,92 msg/s** |

> ✅ **Ponto a destacar:** produção e consumo são **completamente desacoplados** — a API sustenta **470 req/s** (limitada pelo Postgres), enquanto o consumo fica perto de **1 msg/s por instância** (limitado pelo worker). Esse gap é justamente o que a arquitetura assíncrona absorve sem derrubar a API.

---

## Métrica 4 — Lag do consumer group ao longo do tempo

Série temporal coletada com `worker-notification` parado e depois religado, com 5 mensagens publicadas durante a janela de indisponibilidade:

| Timestamp | Lag total | Evento |
|---|---|---|
| 07:35:33 → 07:35:52 | 0 | estado estável |
| 07:35:55 | 0 | worker parado (`no active members`) |
| 07:35:58 | 1 | mensagens começam a chegar |
| 07:36:01 | 4 | acumulando |
| 07:36:04 | **5** | **pico** |
| 07:36:07 | **5** | pico mantido |
| 07:36:10 | **0** | worker volta — fila drenada |
| 07:36:13 → 07:36:38 | 0 | estado estável |

> ✅ **Ponto a destacar:** curva clássica de **acúmulo → pico → recuperação total**, sem nenhuma mensagem perdida. Pico de **5 mensagens**, drenado em ~3 s assim que o worker volta. Ótimo gráfico de linha (lag × tempo) para o slide.

---

## Métrica 5 — Tempo de recuperação (catch-up time)

Duas visões complementares do mesmo evento:

| Visão | Valor |
|---|---|
| Pelo lag do Kafka (pico → 0) | **~3 s** para drenar 5 mensagens (≈ 1,67 msg/s) |
| Pelo log do worker (`*** RETOMANDO ***` → última mensagem processada) | **~12,7 s** de latência E2E por mensagem (publish → process) |

Trecho de log real:

```
worker-notification-1 | 10:35:56 [academic-notification] consumidor atualizado, sem mensagens pendentes
worker-notification-1 | 10:36:08 [notification] student.registered | lagteste_3 | latência: 12.690246298s
worker-notification-1 | 10:36:08 [notification] student.registered | lagteste_2 | latência: 12.702510964s
worker-notification-1 | 10:36:08 [notification] student.registered | lagteste_5 | latência: 12.676711881s
worker-notification-1 | 10:36:08 [notification] student.registered | lagteste_1 | latência: 12.715856173s
worker-notification-1 | 10:36:08 [notification] student.registered | lagteste_4 | latência: 12.688200631s
```

> ✅ **Ponto a destacar:** as 5 mensagens acumuladas durante a indisponibilidade foram **todas reprocessadas em bloco**, nos primeiros segundos após o worker voltar — o lag do Kafka zera em ~3s, e a latência E2E (que inclui o tempo em que a mensagem ficou esperando) fica em ~12,7s.

---

## Métrica 6 — Tempos reais de retry e DLQ ⭐

Esta foi a métrica com os dados mais limpos — o backoff exponencial bateu quase exatamente com o esperado pelo código.

| Etapa | Horário | Δ medido | Δ esperado |
|---|---|---|---|
| retry 1/3 | 10:19:28 | — | — |
| retry 2/3 | 10:19:29 | **1 s** | 1 s ✅ |
| retry 3/3 | 10:19:31 | **2 s** | 2 s ✅ |
| publica na DLQ (tentativa 1/3, aguardando 10s) | 10:19:35 | **4 s** | 4 s ✅ |
| DLQ processa (`evento já processado`) | 10:19:46 | **11 s** | ~10 s ✅ |
| retry 1/3 (2º ciclo) | 10:19:55 | — | — |
| retry 2/3 | 10:19:56 | **1 s** | 1 s ✅ |
| retry 3/3 | 10:19:58 | **2 s** | 2 s ✅ |
| DLQ tentativa 2/3, aguardando 20s | 10:20:02 | **4 s** | 4 s ✅ |
| DLQ processa | 10:20:23 | **21 s** | ~20 s ✅ |
| retry 1/3 (3º ciclo) | 10:20:31 | — | — |
| retry 2/3 | 10:20:32 | **1 s** | 1 s ✅ |
| retry 3/3 | 10:20:34 | **2 s** | 2 s ✅ |
| DLQ tentativa 3/3, aguardando 30s | 10:20:38 | **4 s** | 4 s ✅ |
| DLQ processa | 10:21:09 | **31 s** | ~30 s ✅ |
| `*** RETOMANDO ***` (1 msg pendente) | 10:21:14 | 5 s | — |
| **Processamento final com sucesso** | 10:21:52 | **38 s** | — |

| Resumo | Valor |
|---|---|
| **Total até reentrega bem-sucedida** | **~144 s (2min24s)** |
| Backoff de retry interno | 1s / 2s / 4s (medido **1s / 2s / 4s** — perfeito) |
| Backoff DLQ (3 ciclos) | 10s / 20s / 30s (medido **11s / 21s / 31s**) |

> ✅ **Ponto a destacar:** o backoff exponencial documentado no código **acontece de fato**, com precisão de ~1s. Após 3 ciclos de retry (1+2+4=7s) + 3 republicações da DLQ (10s/20s/30s), o evento foi **reprocessado com sucesso** (não descartado) — a DLQ funciona como mecanismo de **retentativa de longo prazo**, não só de descarte.

---

## Métrica 7 — Escalabilidade horizontal (1 vs 3 instâncias)

Com a checagem de lag corrigida (exige as 3 partições de `academic.student.registered` com `LAG` numérico e soma 0 — sem o falso-zero do `PreparingRebalance`):

| Cenário | Instâncias | Mensagens | Tempo de drenagem (lag→0) |
|---|---|---|---|
| A | 1 | 15 | **1,07 s** |
| B | 3 | 15 | **16,36 s** |

**Distribuição final das partições (`--describe` após o teste, com 3 instâncias ativas):**

| Partição | Tópico | Consumer-ID |
|---|---|---|
| 0 | `academic.student.registered` | `worker@4dd3d7d1cc28-...` |
| 1 | `academic.student.registered` | `worker@4beafaa12d9c-...` |
| 2 | `academic.student.registered` | `worker@1c69c6c22408-...` |

Todas as partições com `LAG=0` e **3 `CONSUMER-ID` distintos** (3 réplicas diferentes) — confirma que o rebalanceamento atribuiu **uma partição por instância**.

> ⚠️ **Nota sobre os tempos (1,07s vs 16,36s):** ao contrário do esperado (3 instâncias deveriam drenar igual ou mais rápido), o cenário B saiu **mais lento**. Isso não é o consumer group ficando mais lento com mais réplicas — é **carga de fundo concorrente**: o cenário B foi medido logo após o `--scale=3` provocar um rebalanceamento que reatribuiu **5 mensagens pendentes** de uma rajada anterior (`*** RETOMANDO APÓS INDISPONIBILIDADE: 5 mensagens acumuladas ***`) e enquanto o sistema ainda processava ~60 eventos da Métrica 2 (report/audit). O cenário A, por outro lado, foi medido com a fila já drenada. Os dois números não são uma comparação isolada justa.
>
> ✅ **Ponto a destacar (evidência mais forte que o cronômetro):** a tabela de distribuição de partições acima — capturada com `kafka-consumer-groups.sh --describe` — mostra **3 `CONSUMER-ID` diferentes**, cada um dono de exatamente 1 partição de `academic.student.registered`, todos com `LAG=0`. Isso comprova o paralelismo por partição de forma direta, sem depender de cronômetro. Para um número de tempo limpo (1 vs 3, mesma carga), repetir o teste em isolamento — ver [`ROTEIRO_METRICAS_PENDENTES.md`](./ROTEIRO_METRICAS_PENDENTES.md).

---

## Métrica 8 — Idempotência: duplicatas evitadas via reset de offset

### 1. Contagem ANTES do reset

```sql
SELECT consumer_group, COUNT(*) FROM processed_events GROUP BY consumer_group;
```

| Consumer group | Eventos publicados (`outbox_events`) | Eventos processados (antes) |
|---|---|---|
| `academic-report` (enrollment.created) | 30 | **30** (igual) ✅ |
| `academic-audit` | 305 (acumulado de todas as rodadas) | **305** (igual) ✅ |
| `academic-notification` | 275 (`student.registered`) + 30 (`enrollment.created`) | **150** (atrasado em relação ao audit) |

> `academic-report` e `academic-audit` já fecham 100% antes do teste — publicados = processados. `academic-notification` está **atrasado**: só processou 150 dos 305 eventos disponíveis, ou seja, tem um **backlog real de 155 mensagens** que ainda não foram consumidas.

### 2. Esperar o grupo ficar `Empty` e resetar offsets para `--to-earliest`

O `worker-notification` foi parado e o roteiro **esperou o grupo `academic-notification` chegar ao estado `Empty`** antes de executar o reset (na rodada anterior, o reset tinha falhado porque o grupo ainda estava em `PreparingRebalance`):

```
GROUP                 COORDINATOR (ID)   ASSIGNMENT-STRATEGY  STATE   #MEMBERS
academic-notification kafka:9092 (1)     -                    Empty   0
```

```bash
docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group academic-notification \
  --topic academic.student.registered \
  --reset-offsets --to-earliest --execute
```

```
GROUP                 TOPIC                       PARTITION  NEW-OFFSET
academic-notification academic.student.registered 1          0
academic-notification academic.student.registered 0          0
academic-notification academic.student.registered 2          0
```

> ✅ O reset **funcionou de primeira** desta vez — confirma que o erro anterior (`PreparingRebalance`) era só uma corrida de tempo (scale concorrente), não uma limitação do sistema. Resultado: offsets zerados em `academic.student.registered` (log-end-offset em 91/92/92 por partição = **275 mensagens** prontas para reentrega completa).

### 3. Religar o worker e capturar a reentrega

```bash
docker compose up -d --scale worker-notification=1 --no-recreate
sleep 8
docker compose logs worker-notification | grep -E "RETOMANDO|já processado"
```

```
worker-notification-1 | 11:29:15 [academic-notification] *** RETOMANDO APÓS INDISPONIBILIDADE: 275 mensagens acumuladas aguardando processamento ***
worker-notification-1 | 11:29:18 [academic-notification] evento já processado, pulando: d84f511a-eb94-4d36-970e-f23fe3e401f3
worker-notification-1 | 11:29:18 [academic-notification] evento já processado, pulando: 9728bd9a-83ce-469f-920b-8b54f9f8522d
... (120 linhas no total)
worker-notification-1 | 11:29:18 [academic-notification] evento já processado, pulando: f7a3ac8b-c8cd-4747-8d1d-f80c63827961
worker-notification-1 | 11:29:18 [academic-notification] evento já processado, pulando: 291b42ab-20f9-4cfe-8b5a-28e8f8de053a
```

```sql
SELECT consumer_group, COUNT(*) FROM processed_events GROUP BY consumer_group;
```

| Consumer group | Processados antes | Mensagens reentregues | `já processado, pulando` | Novos processados | Processados depois |
|---|---|---|---|---|---|
| `academic-notification` | 150 | 275 (`RETOMANDO`) | **120** | **155** | **305** |

A conta fecha exatamente: `120 (duplicatas bloqueadas) + 155 (novos) = 275 (total reentregue)` e `150 (antes) + 155 (novos) = 305 (depois)` — `academic-notification` chega à **paridade com `academic-audit` (305 = 305)**.

> ✅ **Ponto a destacar:** o reset `--to-earliest` forçou o Kafka a reentregar **as 275 mensagens inteiras do tópico**, incluindo as **120 que já tinham sido processadas** em rodadas anteriores. Para essas 120, `TryClaim(event_id, consumer_group)` retornou `false` e o worker logou `evento já processado, pulando` **sem reprocessar** — a garantia `(event_id, consumer_group)` bloqueou a duplicata na prática, com o `event_id` real no log. As outras **155 mensagens nunca tinham sido processadas por `academic-notification`** (backlog real, item 1) — para essas o `TryClaim` retornou `true`, foram processadas normalmente e elevaram o total de 150 para 305, fechando a paridade com `academic-audit`.
>
> ✅ **Evidência complementar (Métrica 6):** durante o teste de retry/DLQ, o mesmo `event_id` (`dlq-academic.student.registered-*`) já tinha aparecido **3 vezes** nos logs da DLQ com `evento já processado, pulando` — o mesmo mecanismo, em outro contexto.

---

## Apêndice — origem dos dados

- Dados brutos: [`DADOS_CAPTURADOS.MD`](./DADOS_CAPTURADOS.MD) e [`dados_capturados_parte_2.md`](./dados_capturados_parte_2.md)
- Roteiro de coleta: [`ROTEIRO_METRICAS.md`](./ROTEIRO_METRICAS.md)
- Execução em: 2026-06-10, ambiente local via `docker compose`
