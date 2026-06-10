# Roteiro de Coleta de Métricas — Dados para o Slide de "Métricas"

> Guia prático com comandos prontos para extrair **números reais** do sistema rodando, para usar em slides/relatório do TP. Cobre principalmente os critérios **4.6** (tempo entre publicação e processamento), **4.4** (acúmulo/recuperação de lag), **4.5** (retry/DLQ) e **5.7** (escalabilidade e compreensão dos trade-offs).
>
> Cada métrica tem: o que ela prova, o comando para coletar, como agregar os dados e uma sugestão de como apresentar (tabela ou gráfico).

---

## Visão geral das métricas

| # | Métrica | O que prova | Cobre (TP) |
|---|---|---|---|
| 1 | Latência de resposta da API (síncrona) | Ação principal não bloqueia | 1.1, 1.4 |
| 2 | Latência fim-a-fim (publicação → processamento) | Tempo de processamento assíncrono, por worker | 4.6 |
| 3 | Throughput (mensagens/segundo) | Vazão do sistema sob carga | 5.7 |
| 4 | Lag do consumer group ao longo do tempo | Acúmulo durante indisponibilidade | 4.4 |
| 5 | Tempo de recuperação (catch-up) | Velocidade de retomada após reconectar | 4.4 |
| 6 | Tempos reais de retry/DLQ | Backoff exponencial funcionando | 4.5 |
| 7 | Escalabilidade horizontal (1 vs 3 instâncias) | Paralelismo por partição | 5.7 |
| 8 | Idempotência — duplicatas evitadas | Garantia de processamento único | 5.7 |

---

## Preparação

Ambiente limpo (zera Postgres e offsets do Kafka):

```bash
docker compose down -v
cp .env.example .env   # confirme FAIL_RATE= vazio
docker compose up --build -d
docker compose ps      # espere todos "running (healthy)"
```

Instale `jq` e `bc` se ainda não tiver (`brew install jq bc`) — usados nos cálculos abaixo.

---

## Métrica 1 — Latência de resposta da API (síncrona)

**O que prova:** o `POST /students` responde em milissegundos, independente do processamento assíncrono — base do critério 1.1/1.4.

### Coleta — 20 requisições sequenciais

```bash
for i in $(seq 1 20); do
  curl -s -o /dev/null -w "%{time_total}\n" -X POST http://localhost:8080/students \
    -H "Content-Type: application/json" \
    -d "{\"name\": \"Metrica API $i\", \"email\": \"metrica_api_${i}@puc.br\"}"
done | tee /tmp/api_latency.txt
```

### Agregação (min / média / máx em ms)

```bash
awk '{
  ms = $1 * 1000
  sum += ms; n++
  if (min == "" || ms < min) min = ms
  if (ms > max) max = ms
} END {
  printf "n=%d  min=%.2fms  avg=%.2fms  max=%.2fms\n", n, min, sum/n, max
}' /tmp/api_latency.txt
```

### Como apresentar

| Métrica | Valor |
|---|---|
| Requisições | 20 |
| Latência mínima | X ms |
| Latência média | X ms |
| Latência máxima | X ms |

> **Ponto a destacar no slide:** essa latência é apenas a transação Postgres (entidade + outbox). O Kafka **nunca** é chamado durante o request — por isso o tempo é estável e baixo independente do estado dos workers.

---

## Métrica 2 — Latência fim-a-fim (publicação → processamento)

**O que prova:** o tempo `time.Since(event.PublishedAt)` que cada worker loga — atende diretamente o critério 4.6.

### Coleta — gerar carga e extrair as latências logadas

1. Gere 30 eventos:
   ```bash
   for i in $(seq 1 30); do
     curl -s -o /dev/null -X POST http://localhost:8080/students \
       -H "Content-Type: application/json" \
       -d "{\"name\": \"Metrica E2E $i\", \"email\": \"metrica_e2e_${i}_$(date +%s%N)@puc.br\"}"
   done
   echo "30 eventos disparados — aguarde ~5s para os workers processarem"
   sleep 5
   ```

2. Extraia as latências do `worker-notification` (repita trocando o nome do serviço para `worker-audit`, `worker-report`):
   ```bash
   docker compose logs worker-notification | grep -oP 'latência: \K[0-9.]+(ns|µs|ms|s)' > /tmp/lat_notification.txt
   ```

### Agregação — normaliza unidades (Go duration string) para ms e calcula min/média/máx/p95

```bash
normalize_and_stats() {
  awk '
    {
      val = $0
      if (val ~ /ms$/)      { gsub("ms","",val); ms = val }
      else if (val ~ /µs$/) { gsub("µs","",val); ms = val/1000 }
      else if (val ~ /ns$/) { gsub("ns","",val); ms = val/1000000 }
      else if (val ~ /s$/)  { gsub("s","",val);  ms = val*1000 }
      print ms
    }' "$1" | sort -n > /tmp/_sorted.txt

  awk '
    { sum += $1; n++; arr[n] = $1
      if (min == "" || $1 < min) min = $1
      if ($1 > max) max = $1
    }
    END {
      p95_idx = int(n * 0.95); if (p95_idx < 1) p95_idx = 1
      printf "n=%d  min=%.2fms  avg=%.2fms  p95=%.2fms  max=%.2fms\n", n, min, sum/n, arr[p95_idx], max
    }' /tmp/_sorted.txt
}

normalize_and_stats /tmp/lat_notification.txt
```

Repita para `worker-audit` e `worker-report` (eventos de `enrollment.created` — use `POST /enrollments` para gerar carga para o `report`).

### Como apresentar

| Worker / Consumer Group | n | Mín | Média | p95 | Máx |
|---|---|---|---|---|---|
| `academic-notification` | 30 | X ms | X ms | X ms | X ms |
| `academic-audit` | 30 | X ms | X ms | X ms | X ms |
| `academic-report` | N | X ms | X ms | X ms | X ms |

> **Ponto a destacar:** a latência média deve ficar na faixa de **dezenas a poucas centenas de ms** — composta por: até 200ms do `OutboxRelay` (ticker) + tempo de fetch do Kafka + `TryClaim` no Postgres + processamento. Se quiser decompor isso visualmente, use um gráfico de barras empilhadas com essas 3 faixas (a parcela do `OutboxRelay` é a dominante, por construção).

---

## Métrica 3 — Throughput (mensagens por segundo)

**O que prova:** quantas requisições/eventos o sistema sustenta por segundo — usado para discutir escalabilidade (5.7).

### Coleta — disparo de N requisições em paralelo

```bash
N=50
START=$(date +%s.%N)
for i in $(seq 1 $N); do
  curl -s -o /dev/null -X POST http://localhost:8080/students \
    -H "Content-Type: application/json" \
    -d "{\"name\": \"Carga $i\", \"email\": \"carga_${i}_$(date +%s%N)@puc.br\"}" &
done
wait
END=$(date +%s.%N)
ELAPSED=$(echo "$END - $START" | bc)
echo "Total: $N requisições em ${ELAPSED}s -> $(echo "$N / $ELAPSED" | bc -l) req/s"
```

### Verificar quanto tempo o consumer levou para drenar a fila

Logo após o comando acima, monitore o lag até zerar (ver Métrica 4 para o comando de lag). Anote:
- `t0` = momento em que o burst de 50 requisições terminou
- `t1` = momento em que `LAG total` do `academic-notification` volta a 0

```
throughput de consumo ≈ 50 mensagens / (t1 - t0) segundos
```

### Como apresentar

| Métrica | Valor |
|---|---|
| Requisições enviadas | 50 |
| Tempo total (API) | X s |
| Throughput de produção (API) | X req/s |
| Tempo até lag zerar (`academic-notification`) | X s |
| Throughput de consumo | X msg/s |

> **Ponto a destacar:** produção (API) e consumo são desacoplados — o throughput de produção depende só do Postgres; o de consumo depende do worker e do número de partições/réplicas (ver Métrica 7).

---

## Métrica 4 — Lag do consumer group ao longo do tempo

**O que prova:** mensagens se acumulam sem perda durante indisponibilidade — critério 4.4. Os dados aqui geram um **gráfico de linha (lag x tempo)** muito forte para o slide.

### Comando para ler o lag total de um consumer group

```bash
docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe --group academic-notification
```

Saída tem uma linha por partição, com a coluna `LAG`. Para somar:

```bash
docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe --group academic-notification \
  | awk 'NR>1 && $6 ~ /^[0-9]+$/ {sum+=$6} END {print sum+0}'
```

### Coleta — série temporal durante o cenário de indisponibilidade

1. Pare o worker:
   ```bash
   docker compose stop worker-notification
   ```

2. Em um terminal, rode o coletor de série temporal (grava em `/tmp/lag_series.csv`):
   ```bash
   echo "timestamp,lag" > /tmp/lag_series.csv
   for i in $(seq 1 30); do
     LAG=$(docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh \
       --bootstrap-server localhost:9092 \
       --describe --group academic-notification \
       | awk 'NR>1 && $6 ~ /^[0-9]+$/ {sum+=$6} END {print sum+0}')
     echo "$(date +%H:%M:%S),$LAG" | tee -a /tmp/lag_series.csv
     sleep 2
   done
   ```

3. **No meio dessa janela** (por exemplo, depois de ~10s, em outro terminal), gere carga e religue o worker:
   ```bash
   for i in 1 2 3 4 5; do
     curl -s -o /dev/null -X POST http://localhost:8080/students \
       -H "Content-Type: application/json" \
       -d "{\"name\": \"Lag Teste $i\", \"email\": \"lagteste_${i}_$(date +%s%N)@puc.br\"}"
   done
   docker compose start worker-notification
   ```

4. Ao final dos 30 ciclos (60s), `/tmp/lag_series.csv` terá uma série como:
   ```
   timestamp,lag
   14:32:01,0
   14:32:03,0
   14:32:05,5
   14:32:07,5
   14:32:09,3
   14:32:11,1
   14:32:13,0
   ...
   ```

### Como apresentar

Importe `/tmp/lag_series.csv` no Excel/Google Sheets/Numbers e gere um **gráfico de linha**: eixo X = timestamp, eixo Y = lag. A curva deve subir quando o worker está parado, ter um pico, e cair a 0 quando ele volta — visualmente prova acúmulo + recuperação sem perda.

---

## Métrica 5 — Tempo de recuperação (catch-up time)

**O que prova:** quão rápido o sistema absorve o backlog acumulado — complementa a Métrica 4 com um número único.

### Cálculo a partir do CSV da Métrica 4

```bash
awk -F, '
  $2 > 0 && !peak_seen { peak_time=$1; peak_lag=$2; peak_seen=1 }
  $2 > peak_lag { peak_time=$1; peak_lag=$2 }
  $2 == 0 && peak_seen && !recovered && NR > 2 { recovered_time=$1; recovered=1 }
  END {
    print "Pico de lag: " peak_lag " em " peak_time
    print "Recuperado (lag=0) em: " recovered_time
  }' /tmp/lag_series.csv
```

### Alternativa via logs (mais preciso)

Use `docker compose logs -t` (com timestamp) para pegar o instante exato em que o worker:
1. Loga `*** RETOMANDO APÓS INDISPONIBILIDADE: N mensagens acumuladas ***` (t_início)
2. Loga a última das N mensagens pendentes (t_fim)

```bash
docker compose logs -t worker-notification | grep -E "RETOMANDO|notification"
```

```
worker-notification-1  | 2026-06-09T14:32:05.123Z [academic-notification] *** RETOMANDO APÓS INDISPONIBILIDADE: 5 mensagens acumuladas aguardando processamento ***
worker-notification-1  | 2026-06-09T14:32:05.130Z [notification] student.registered | email: lagteste_1@puc.br | latência: 8.2s
...
worker-notification-1  | 2026-06-09T14:32:05.612Z [notification] student.registered | email: lagteste_5@puc.br | latência: 8.7s
```

```
catch-up time = t_fim - t_início
```

### Como apresentar

| Métrica | Valor |
|---|---|
| Mensagens acumuladas | 5 |
| Tempo de catch-up | X ms / s |
| Throughput de catch-up | X msg/s |

---

## Métrica 6 — Tempos reais de retry e DLQ

**O que prova:** o backoff exponencial documentado (1s/2s/4s nos retries internos, 10s/20s/30s nos ciclos DLQ) acontece de fato — critério 4.5.

### Ativar a falha simulada

```bash
sed -i.bak 's/^FAIL_RATE=.*/FAIL_RATE=1/' .env
docker compose up -d --force-recreate worker-notification worker-dlq
```

### Disparar 1 evento e capturar timestamps

```bash
curl -s -o /dev/null -X POST http://localhost:8080/students \
  -H "Content-Type: application/json" \
  -d '{"name": "Metrica Retry", "email": "metrica_retry@puc.br"}'

docker compose logs -t worker-notification --since 1m | grep -E "retry|RETOMANDO"
docker compose logs -t worker-dlq --since 2m | grep "DLQ"
```

### Calcular os intervalos

Com `-t`, cada linha tem um timestamp ISO 8601. Calcule a diferença entre:

| Evento | Intervalo esperado |
|---|---|
| `retry 1/3` → `retry 2/3` | ~1s |
| `retry 2/3` → `retry 3/3` | ~2s |
| `retry 3/3` → publicação na DLQ | ~4s |
| DLQ recebida → `republicando ... aguardando 10s` (1ª republicação) | ~10s |
| 1ª republicação → 2ª (`aguardando 20s`) | ~20s + ciclo de retry interno (1+2+4=7s) |
| 2ª → 3ª (`aguardando 30s`) | ~30s + 7s |

Script para extrair e converter timestamps em segundos relativos:

```bash
docker compose logs -t worker-notification worker-dlq --since 5m \
  | grep -E "retry|DLQ|RETOMANDO" \
  | sed -E 's/^(\S+)\s+\|\s+([0-9T:.Z-]+)\s+(.*)$/\2 \3/' \
  | sort
```

### Como apresentar

| Etapa | Tempo decorrido (medido) | Tempo esperado (código) |
|---|---|---|
| retry 1 → 2 | X s | 1 s |
| retry 2 → 3 | X s | 2 s |
| retry 3 → DLQ | X s | 4 s |
| DLQ ciclo 1 (republicação) | X s | 10 s |
| DLQ ciclo 2 | X s | 20 s |
| DLQ ciclo 3 | X s | 30 s |
| **Total até descarte definitivo** | X s | ~97 s (4 ciclos × 7s de retries + 10+20+30s de backoff DLQ) |

**Restaurar o ambiente ao final:**
```bash
sed -i.bak 's/^FAIL_RATE=.*/FAIL_RATE=/' .env
docker compose up -d --force-recreate worker-notification worker-audit worker-report worker-dlq
```

---

## Métrica 7 — Escalabilidade horizontal (1 vs 3 instâncias)

**O que prova:** múltiplas instâncias do mesmo consumer group processam em paralelo até o limite de partições (3) — critério 5.7.

### Cenário A — 1 instância (baseline)

```bash
docker compose stop worker-notification
sleep 1
docker compose start worker-notification

for i in $(seq 1 15); do
  curl -s -o /dev/null -X POST http://localhost:8080/students \
    -H "Content-Type: application/json" \
    -d "{\"name\": \"Scale A $i\", \"email\": \"scale_a_${i}_$(date +%s%N)@puc.br\"}"
done

START=$(date +%s.%N)
while true; do
  LAG=$(docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh \
    --bootstrap-server localhost:9092 --describe --group academic-notification \
    | awk 'NR>1 && $6 ~ /^[0-9]+$/ {sum+=$6} END {print sum+0}')
  if [ "$LAG" = "0" ]; then break; fi
  sleep 0.5
done
END=$(date +%s.%N)
echo "1 instância: 15 mensagens em $(echo "$END - $START" | bc)s"
```

### Cenário B — 3 instâncias

```bash
make scale-notification n=3
docker compose ps worker-notification

for i in $(seq 1 15); do
  curl -s -o /dev/null -X POST http://localhost:8080/students \
    -H "Content-Type: application/json" \
    -d "{\"name\": \"Scale B $i\", \"email\": \"scale_b_${i}_$(date +%s%N)@puc.br\"}"
done

START=$(date +%s.%N)
while true; do
  LAG=$(docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh \
    --bootstrap-server localhost:9092 --describe --group academic-notification \
    | awk 'NR>1 && $6 ~ /^[0-9]+$/ {sum+=$6} END {print sum+0}')
  if [ "$LAG" = "0" ]; then break; fi
  sleep 0.5
done
END=$(date +%s.%N)
echo "3 instâncias: 15 mensagens em $(echo "$END - $START" | bc)s"

docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --group academic-notification
```

A última saída mostra a coluna `CONSUMER-ID` — confirme que as 3 partições estão distribuídas entre 3 `CONSUMER-ID` diferentes.

### Voltar para 1 instância

```bash
docker compose up -d --scale worker-notification=1 --no-recreate
```

### Como apresentar

| Cenário | Instâncias | Mensagens | Tempo de drenagem | Throughput |
|---|---|---|---|---|
| A | 1 | 15 | X s | X msg/s |
| B | 3 | 15 | X s | X msg/s |

| Partição | Consumer (cenário A) | Consumer (cenário B) |
|---|---|---|
| 0 | worker-notification-1 | worker-notification-1 |
| 1 | worker-notification-1 | worker-notification-2 |
| 2 | worker-notification-1 | worker-notification-3 |

> **Ponto a destacar:** com 3 partições, o ganho de throughput entre 1 e 3 instâncias deve ser visível mas **sub-linear** (overhead de coordenação do consumer group). Acima de 3 instâncias, o ganho é **zero** — partição é o teto de paralelismo.

---

## Métrica 8 — Idempotência: duplicatas evitadas (bônus)

**O que prova:** o mesmo evento não é processado duas vezes pelo mesmo consumer group, mesmo com reentrega — critério 5.7.

### Contagem de eventos publicados vs. processados

```bash
docker compose exec -T postgres psql -U academic -d academic -c \
  "SELECT consumer_group, COUNT(*) AS processados FROM processed_events GROUP BY consumer_group;"
```

```bash
docker compose exec -T postgres psql -U academic -d academic -c \
  "SELECT topic, COUNT(*) AS publicados FROM outbox_events WHERE published = true GROUP BY topic;"
```

### Forçar uma redelivery real (reset de offset) — opcional, avançado

```bash
docker compose stop worker-notification

docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group academic-notification \
  --topic academic.student.registered \
  --reset-offsets --to-earliest --execute

docker compose start worker-notification
docker compose logs -f worker-notification
```

**Esperado:** todas as mensagens antigas do tópico são reentregues, mas para cada `event_id` já presente em `processed_events`, o log mostra:
```
[academic-notification] evento já processado, pulando: <event_id>
```

### Como apresentar

| Consumer group | Eventos publicados | Eventos processados | Duplicatas evitadas |
|---|---|---|---|
| `academic-notification` | X | X (igual) | N reentregas → 0 reprocessamentos |
| `academic-audit` | X | X (igual) | — |

> **Ponto a destacar:** mesmo após o reset de offset forçar a reentrega de **todas** as mensagens do tópico, a contagem em `processed_events` não muda — `TryClaim` bloqueia o reprocessamento via `(event_id, consumer_group)`.

---

## Resumo final — tabela única para o slide

Consolide os números coletados acima nesta tabela única, pronta para colar no slide de "Métricas":

| Categoria | Métrica | Valor coletado |
|---|---|---|
| Latência síncrona | Resposta média da API (`POST /students`) | _ ms |
| Latência fim-a-fim | `academic-notification` (média / p95) | _ ms / _ ms |
| Latência fim-a-fim | `academic-audit` (média / p95) | _ ms / _ ms |
| Latência fim-a-fim | `academic-report` (média / p95) | _ ms / _ ms |
| Throughput | Produção (API) | _ req/s |
| Throughput | Consumo (1 instância) | _ msg/s |
| Throughput | Consumo (3 instâncias) | _ msg/s |
| Indisponibilidade | Pico de lag acumulado | _ mensagens |
| Indisponibilidade | Tempo de catch-up | _ s |
| Retry | Intervalo retry 1→2→3 | 1s / 2s / 4s (medido: _ / _ / _) |
| DLQ | Backoff ciclos 1/2/3 | 10s / 20s / 30s (medido: _ / _ / _) |
| DLQ | Tempo total até descarte definitivo | _ s |
| Idempotência | Reentregas vs. reprocessamentos | _ vs. 0 |

---

## Apêndice — limpeza após a coleta

```bash
rm -f /tmp/api_latency.txt /tmp/lat_*.txt /tmp/_sorted.txt /tmp/lag_series.csv .env.bak
docker compose down -v   # se quiser voltar ao estado zero antes da gravação final
```
