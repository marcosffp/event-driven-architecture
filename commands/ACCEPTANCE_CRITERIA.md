# Critérios de Aceite — Arquitetura Orientada a Eventos e Mensageria

> **Disciplina:** Arquitetura de Software (5º período) · PUC Minas  
> **Professor:** Filipe Tório Lopes Ruas Nhimi  
> **Valor:** 25 pontos  
>
> Use este documento para verificar se a implementação cobre **todos** os requisitos do trabalho antes da apresentação. Marque cada item conforme avança.

---

## Como usar

- `[ ]` — não implementado / não demonstrado  
- `[x]` — implementado e funcionando  
- `[-]` — não se aplica à escolha tecnológica do grupo  

---

## 1. Requisitos Funcionais

> O sistema **deve** ser capaz de realizar tudo abaixo. São os requisitos mínimos de funcionamento.

### 1.1 Ação principal via API

- [ ] Existe pelo menos um endpoint HTTP que representa uma **ação principal** (ex: cadastro de aluno, criação de matrícula)
- [ ] O endpoint responde de forma síncrona ao cliente (retorna status HTTP imediatamente)
- [ ] A requisição principal **não é bloqueada** pela execução das tarefas secundárias

### 1.2 Publicação de evento / mensagem

- [ ] Após a ação principal ser executada, o sistema **publica uma mensagem ou evento** no broker
- [ ] A publicação ocorre de forma **automática**, sem intervenção manual
- [ ] O evento contém dados relevantes da ação que o originou (ex: id, timestamp, campos do domínio)

### 1.3 Consumo assíncrono

- [ ] Existe pelo menos um **consumidor** que processa as mensagens de forma assíncrona
- [ ] O consumidor é um componente **separado** do produtor (processos, serviços ou threads distintas)
- [ ] O consumidor processa a mensagem **sem que o produtor precise aguardar**

### 1.4 Processamento em segundo plano

- [ ] Pelo menos uma tarefa secundária é executada em background após a ação principal
- [ ] Exemplos de tarefas aceitas: envio de notificação, registro de auditoria, geração de relatório, processamento de fila de atividades
- [ ] A tarefa de segundo plano **não bloqueia** a resposta HTTP ao cliente

### 1.5 Desacoplamento entre produtor e consumidor

- [ ] O produtor **não conhece** o consumidor diretamente (sem chamada direta entre eles)
- [ ] É possível adicionar ou remover consumidores **sem alterar o produtor**
- [ ] O produtor continua funcionando mesmo que o consumidor esteja indisponível

---

## 2. Estrutura Mínima da Solução

> A solução deve ter obrigatoriamente os três componentes abaixo.

- [ ] **Produtor de mensagens** — componente que gera e publica eventos/mensagens
- [ ] **Broker / mecanismo de mensageria** — fila, tópico, bus ou mecanismo Pub/Sub intermediando a comunicação
- [ ] **Consumidor** — componente responsável pelo processamento assíncrono das mensagens

---

## 3. Requisitos Técnicos

### 3.1 Mensageria obrigatória

- [ ] A solução usa obrigatoriamente **fila, tópico, broker de mensagens ou mecanismo Pub/Sub**
- [ ] A tecnologia de mensageria escolhida está em operação (não é simulada por chamada direta ou banco de dados simples sem semântica de fila)
- [ ] A escolha tecnológica é justificável e adequada ao cenário

### 3.2 Tecnologias (livre escolha)

- [ ] A stack tecnológica utilizada é **uma das permitidas** ou equivalente (back-end, banco de dados, mensageria, cloud)
- [ ] As tecnologias escolhidas são **adequadas** para suportar os requisitos do trabalho
- [ ] O grupo consegue **explicar e justificar** cada escolha tecnológica

### 3.3 Requisito principal de mensageria — demonstração e explicação

- [ ] A solução demonstra o **envio** de mensagens
- [ ] A solução demonstra o **armazenamento temporário** das mensagens no broker
- [ ] A solução demonstra o **consumo** das mensagens

**Explicações obrigatórias durante a apresentação:**

- [ ] Como a mensageria **reduz acoplamento** entre componentes
- [ ] Como a mensageria **melhora disponibilidade** do sistema
- [ ] Como a mensageria **melhora escalabilidade** da solução
- [ ] Como a mensageria **garante rastreabilidade** dos eventos
- [ ] Como o sistema lida com **tratamento de falhas**
- [ ] Como o sistema trata **consistência eventual**

---

## 4. Métricas e Demonstrações Obrigatórias

> Estes itens **precisam ser demonstrados ao vivo** durante a apresentação.

### 4.1 Publicação de mensagens

- [ ] É possível **demonstrar ao vivo** que uma mensagem é publicada após uma ação principal
- [ ] A mensagem aparece no broker (ex: via ferramenta de monitoramento, log, interface do broker)
- [ ] O payload da mensagem é visível e contém os dados esperados

### 4.2 Consumo assíncrono

- [ ] É possível **demonstrar ao vivo** que o consumidor recebe e processa a mensagem
- [ ] O processamento ocorre **sem que o cliente HTTP esteja aguardando**
- [ ] Logs ou saída visível confirmam que o consumidor processou a mensagem

### 4.3 Fila ou tópico recebendo mensagens

- [ ] A fila ou tópico do broker **pode ser observado** durante a apresentação (ex: Kafka UI, RabbitMQ Management, Azure Portal, etc.)
- [ ] É possível ver mensagens chegando e sendo consumidas em tempo real
- [ ] O offset, posição na fila ou contagem de mensagens fica visível

### 4.4 Comportamento quando o consumidor estiver indisponível

- [ ] É possível **simular o consumidor fora do ar** (ex: parar o processo/container)
- [ ] As mensagens **se acumulam na fila/tópico** durante a indisponibilidade
- [ ] Ao consumidor voltar, ele **processa as mensagens acumuladas** (não as perde)
- [ ] A API continua respondendo normalmente durante a indisponibilidade do consumidor

### 4.5 Reprocessamento ou retry de mensagens

- [ ] Existe um mecanismo de **retry** quando o processamento de uma mensagem falha
- [ ] O retry acontece de forma **automática**, sem intervenção manual
- [ ] É possível demonstrar ou explicar como o retry funciona (política de tentativas, backoff, etc.)
- [ ] Existe um destino para mensagens que falham em todas as tentativas (ex: Dead Letter Queue, fila de erro, log de falha)

### 4.6 Tempo entre publicação e processamento

- [ ] O sistema **mede ou registra** o tempo entre a publicação e o processamento da mensagem
- [ ] Essa métrica é **visível** durante a apresentação (ex: log com timestamp, dashboard, saída no terminal)
- [ ] O grupo consegue explicar o que influencia essa latência

### 4.7 Discussão sobre consistência eventual e falhas

- [ ] O grupo consegue explicar o conceito de **consistência eventual** no contexto da solução
- [ ] O grupo consegue explicar **o que acontece se o broker cair** (mensagens em trânsito, perda, reentrega)
- [ ] O grupo consegue explicar **o que acontece se o consumidor falhar** durante o processamento
- [ ] O grupo consegue explicar **o que acontece se o produtor falhar** logo após publicar (mensagem já está no broker?)
- [ ] O grupo reconhece e explica os **trade-offs** entre consistência forte e consistência eventual

---

## 5. Critérios de Avaliação (nota)

> Estes critérios definem **como a nota será atribuída**. Cada item deve estar coberto tanto na implementação quanto na apresentação oral.

### 5.1 Comunicação e clareza na apresentação

- [ ] A solução é apresentada de forma **clara e organizada**
- [ ] O grupo consegue explicar o que cada componente faz sem ler do slide
- [ ] Perguntas do professor são respondidas com segurança e precisão
- [ ] A linguagem técnica é usada corretamente (broker, tópico, offset, consumer group, etc.)

### 5.2 Cumprimento dos requisitos técnicos

- [ ] Todos os requisitos funcionais da seção 1 estão implementados
- [ ] A estrutura mínima (produtor + broker + consumidor) existe e funciona
- [ ] A solução usa mensageria real (não simulada)

### 5.3 Apresentação dos detalhes arquiteturais com linguagem técnica

- [ ] O grupo apresenta a **arquitetura da solução** (diagrama ou explicação estruturada)
- [ ] Os termos corretos são usados: produtor, consumidor, tópico/fila, broker, pub/sub, offset, consumer group, etc.
- [ ] O grupo explica **por que** a arquitetura foi desenhada dessa forma, não apenas **o que** ela faz

### 5.4 Qualidade da implementação

- [ ] O código é **organizado** e fácil de entender
- [ ] A separação entre produtor, broker e consumidor é clara no código
- [ ] Não há acoplamento direto entre produtor e consumidor no código
- [ ] O tratamento de erros existe e é adequado

### 5.5 Capacidade de demonstrar e justificar decisões arquiteturais

- [ ] O grupo consegue justificar a escolha do broker (ex: por que Kafka e não RabbitMQ?)
- [ ] O grupo consegue justificar o padrão de publicação utilizado (ex: por que Outbox Pattern?)
- [ ] O grupo consegue justificar como tratou idempotência, retry e falhas
- [ ] O grupo consegue apontar **limitações e trade-offs** da solução escolhida

### 5.6 Funcionamento da solução durante a apresentação

- [ ] A solução **sobe sem erros** no ambiente de apresentação
- [ ] O fluxo completo (ação principal → evento → consumo) funciona ao vivo
- [ ] A demonstração dos cenários de falha funciona (consumidor fora do ar, retry, etc.)
- [ ] Não há crashes ou erros inesperados durante a demonstração

### 5.7 Qualidade da solução de mensageria e compreensão dos trade-offs

- [ ] A solução de mensageria vai **além do mínimo** (ex: múltiplos consumidores, múltiplos tópicos, DLQ, idempotência)
- [ ] O grupo demonstra **compreensão profunda** do processamento assíncrono (não só que funciona, mas por quê)
- [ ] Os trade-offs de processamento assíncrono são explicados com propriedade:
  - [ ] Latência eventual vs. resposta imediata
  - [ ] Entrega garantida vs. processamento exatamente uma vez
  - [ ] Escalabilidade vs. complexidade operacional
  - [ ] Disponibilidade vs. consistência (teorema CAP)

---

## 6. Checklist de Apresentação

> Use esta seção como roteiro no dia da apresentação para garantir que nada é esquecido.

### Antes de começar

- [ ] Docker / infraestrutura está subindo sem erros
- [ ] Broker está operacional e acessível
- [ ] Todos os consumidores estão ativos
- [ ] Interface de monitoramento do broker está aberta (Kafka UI, RabbitMQ Management, etc.)
- [ ] Terminal com logs dos consumidores visível

### Durante a demonstração

- [ ] Disparar a **ação principal** via API e mostrar a resposta imediata
- [ ] Mostrar a **mensagem chegando** no broker (interface de monitoramento)
- [ ] Mostrar o **consumidor processando** a mensagem (log ou saída)
- [ ] Mostrar o **tempo de latência** entre publicação e processamento
- [ ] **Parar o consumidor** e disparar novas ações — mostrar mensagens acumulando
- [ ] **Religar o consumidor** — mostrar as mensagens acumuladas sendo processadas
- [ ] **Simular falha de processamento** — mostrar o retry acontecendo
- [ ] Mostrar o destino final de uma mensagem que falhou em todos os retries (DLQ ou equivalente)

### Perguntas esperadas do professor

- [ ] "O que acontece se o broker cair?"
- [ ] "O que acontece se o consumidor processar a mesma mensagem duas vezes?"
- [ ] "Como vocês garantem que a mensagem não é perdida?"
- [ ] "Como vocês escalariam a solução para suportar mais carga?"
- [ ] "Por que escolheram essa tecnologia de mensageria?"
- [ ] "Qual é o trade-off de usar processamento assíncrono aqui?"
- [ ] "Como o sistema se comporta com consistência eventual?"
- [ ] "Como vocês detectariam que uma mensagem está travada na fila?"

---

## 7. Resumo de Pontuação (referência)

| Critério | O que o professor avalia |
|---|---|
| Comunicação e clareza | Explicação oral, uso de linguagem técnica, organização |
| Requisitos técnicos | Produtor + broker + consumidor funcionando |
| Detalhes arquiteturais | Diagrama, terminologia correta, justificativas |
| Qualidade da implementação | Código organizado, separação de responsabilidades |
| Decisões arquiteturais | Justificativas de escolha, trade-offs reconhecidos |
| Funcionamento ao vivo | Demo sem crash, cenários de falha demonstrados |
| Qualidade da mensageria | Profundidade da solução, compreensão dos trade-offs assíncronos |

**Valor total: 25 pontos**
