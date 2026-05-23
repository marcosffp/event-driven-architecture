# Trabalho Prático — Arquitetura Orientada a Eventos e Mensageria

Este comando define as diretrizes oficiais do trabalho prático da disciplina **Arquitetura de Software (5º período)** da PUC Minas. Toda decisão de implementação, arquitetura e demonstração deve estar alinhada com o que está descrito abaixo.

---

## Objetivo

Projetar, implementar e demonstrar uma solução baseada em **mensageria e processamento assíncrono**, explorando conceitos de arquitetura orientada a eventos.

O foco principal **não é apenas enviar mensagens**, mas demonstrar como a troca assíncrona de eventos pode:
- Reduzir acoplamento entre componentes
- Melhorar escalabilidade
- Permitir maior flexibilidade

---

## Cenário

Plataforma acadêmica que executa tarefas secundárias após uma ação principal (ex: cadastro de aluno ou matrícula). Após a ação, o sistema deve:
- Enviar notificação
- Registrar auditoria
- Gerar relatório
- Processar fila de atividades **sem bloquear** a requisição principal

---

## Requisitos Funcionais

A solução deve:
1. Executar uma **ação principal via API**
2. **Publicar** uma mensagem/evento após a ação principal
3. **Consumir** a mensagem de forma assíncrona
4. **Processar** uma tarefa em segundo plano
5. **Demonstrar desacoplamento** entre produtor e consumidor

---

## Estrutura Mínima Obrigatória

| Componente | Descrição |
|---|---|
| **Produtor** | Publica mensagens/eventos após a ação principal |
| **Broker/Mecanismo de Mensageria** | Fila, tópico ou Pub/Sub |
| **Consumidor** | Processa mensagens de forma assíncrona |

---

## Requisitos Técnicos

### Mensageria (obrigatória)
Implementar obrigatoriamente uma das seguintes opções:
- Fila (queue)
- Tópico (topic)
- Broker de mensagens
- Mecanismo Pub/Sub

### Tecnologias (livre escolha)
- **Back-end:** .NET, Node.js, Java, Python, Go
- **Mensageria:** RabbitMQ, Kafka, Azure Service Bus, Redis Pub/Sub, MassTransit
- **Banco de dados:** PostgreSQL, MongoDB, SQL Server, MySQL
- **Observabilidade:** Grafana, Prometheus
- **Cloud:** livre escolha

---

## Demonstrações Obrigatórias na Apresentação

A apresentação deve cobrir **todos** os itens abaixo:

- [ ] Publicação de mensagens
- [ ] Consumo assíncrono
- [ ] Fila ou tópico recebendo mensagens
- [ ] Comportamento quando o **consumidor estiver indisponível**
- [ ] Reprocessamento ou **retry** de mensagens
- [ ] **Tempo entre publicação e processamento**
- [ ] Discussão sobre **consistência eventual** e tratamento de falhas

---

## Critérios de Avaliação (25 pontos)

| Critério |
|---|
| Comunicação e clareza na apresentação |
| Cumprimento dos requisitos técnicos |
| Apresentação com linguagem técnica adequada |
| Qualidade da implementação |
| Capacidade de demonstrar e justificar decisões arquiteturais |
| Funcionamento da solução durante a apresentação |
| Qualidade da solução de mensageria e compreensão dos trade-offs de processamento assíncrono |

---

## Como usar este comando

Ao invocar `/tp`, use as diretrizes acima como **fonte autoritativa** para:

- Avaliar se uma decisão de arquitetura está alinhada com o trabalho
- Sugerir implementações que atendam os requisitos funcionais e técnicos
- Verificar se algum requisito obrigatório ainda não foi coberto
- Preparar a equipe para a apresentação (checklist de demonstrações)
- Discutir trade-offs de mensageria (acoplamento, disponibilidade, escalabilidade, rastreabilidade, falhas, consistência eventual)

Ao responder, sempre verifique quais itens do checklist de demonstrações ainda precisam ser implementados ou testados.
