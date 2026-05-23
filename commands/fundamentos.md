# Fundamentos — Arquiteturas Orientadas a Mensagens (Aula 26)

Este comando consolida os conceitos teóricos da Aula 26 da disciplina **Arquitetura de Software** (Prof. Filipe Nhimi — PUC Minas). Use como base conceitual para justificar decisões arquiteturais no trabalho prático.

---

## O que é Arquitetura de Software?

| Autor | Definição |
|---|---|
| **Pressman** | Organização fundamental de um sistema: componentes, relacionamentos e princípios que governam seu projeto e evolução |
| **Richards & Ford** | Estrutura fundamental incluindo componentes, responsabilidades, relações e princípios de evolução |
| **Martin Fowler** | "Um conjunto de decisões difíceis de serem mudadas no futuro" |
| **Bass, Clements & Kazman** | Estrutura(s) de um sistema compostas por elementos de software, suas propriedades visíveis externamente e as relações entre eles |

---

## Evolução Arquitetural: do Monolito ao Assíncrono

### Arquitetura Monolítica — Problemas

- Escalabilidade limitada — escala tudo junto
- Deploy crítico — uma mudança derruba o sistema inteiro
- Baixa manutenibilidade — código grande, acoplado e complexo
- Performance instável — um módulo lento afeta todos
- Falha única — qualquer erro interrompe tudo
- Dificuldade de inovação — preso a uma linguagem/tecnologia
- Time lento — muitos devs no mesmo código
- Banco único como gargalo — concorrência, deadlocks, lentidão

### Arquitetura Distribuída — Trade-offs

**Vantagens:**
- Escalabilidade por módulo
- Deploy independente
- Isolamento parcial de falhas
- Manutenção mais simples
- Observabilidade mais clara
- Domínios separados

**Desvantagens:**
- Alto acoplamento temporal via HTTP
- Cascata de latência e falhas
- Orquestração rígida
- Baixa resiliência
- Não absorve picos de carga

> A arquitetura distribuída síncrona melhora o acoplamento estrutural, mas mantém o acoplamento temporal — uma falha em cadeia ainda compromete fluxos críticos de negócio.

---

## Middleware Orientado a Mensagens (MOM)

**Definição:** Infraestrutura tecnológica que permite a comunicação assíncrona entre sistemas por meio de troca de mensagens, utilizando mecanismos como filas, tópicos ou streams, implementados por um broker, para **desacoplar emissores e consumidores**.

O MOM atua como **camada intermediária** — análogo a uma agência dos Correios: recebe, armazena, monta rotas e entrega às partes interessadas.

---

## Estilos Arquiteturais com MOM

### Point-to-Point (Fila / Queue)

- Um produtor envia mensagens para uma fila
- Múltiplos consumidores concorrentes possíveis, mas **um único processa** cada mensagem
- Consumidor remove a mensagem após processar
- Modelo FIFO (geralmente)
- **Garantias de entrega:** at-least-once, at-most-once, etc.
- Acoplamento fraco entre produtor e consumidor
- **Backpressure natural** — fila cresce quando consumidor está lento
- Ideal para: tarefas assíncronas de alta carga

### Publish/Subscribe (Tópico / Topic)

- Produtor publica em um tópico
- **Todos os assinantes** interessados recebem a mensagem
- Comunicação **1→N**
- Base para sistemas event-driven e broadcast
- Baixo acoplamento
- Ideal para: integrações e reações a eventos

### Event Streaming (Stream)

- Mensagens **não são removidas** após consumo
- Consumidores independentes podem reprocessar eventos
- Características: reprocessamento, ordenação, alto throughput, persistência longa
- Fundamental em arquiteturas reativas e big data
- Soluções: **Kafka**, Pulsar, Kinesis (logs distribuídos imutáveis)

---

## Quando usar MOM?

Use MOM quando o sistema precisa de:

- **Desacoplamento** entre componentes
- **Alta escalabilidade**
- **Comunicação assíncrona** (não pode bloquear o fluxo principal)
- **Tolerância a falhas e resiliência**
- **Processamento distribuído**
- **Filas para workloads pesados**

> A ideia central é **substituir integrações síncronas sensíveis** por comunicação assíncrona resiliente.

---

## Conclusão — MOM não é bala de prata

> "No Silver Bullet" — Fred Brooks: não existe tecnologia capaz de eliminar de forma mágica a complexidade inerente do software.

MOM é uma **decisão arquitetural** que traz benefícios claros quando aplicada corretamente, mas também introduz novos desafios:

- **Monitoramento** da fila e dos consumidores
- **Idempotência** — garantir que processar a mesma mensagem duas vezes não cause efeito duplicado
- **Orquestração** de consumidores e retries
- **Governança de mensagens** — schema, versionamento, contratos

---

## Referências

1. Hohpe & Woolf — *Enterprise Integration Patterns* (Addison-Wesley, 2003)
2. Bass, Clements & Kazman — *Software Architecture in Practice*, 4. ed. (Addison-Wesley, 2021)
3. Ford & Richards — *Fundamentals of Software Architecture* (O'Reilly, 2020)
4. Kreps & Pathella — *Building Event-Driven Microservices* (O'Reilly, 2023)
5. Fowler — *Patterns of Enterprise Application Architecture* (Addison-Wesley, 2003)
6. Brooks — *No Silver Bullet* (IEEE Computer, 1987)

---

## Como usar este comando

Ao invocar `/fundamentos`, use o conteúdo acima como **base teórica** para:

- Justificar a escolha entre fila (Point-to-Point), tópico (Pub/Sub) ou stream para o trabalho prático
- Explicar trade-offs de arquitetura monolítica vs distribuída vs assíncrona
- Embasar a discussão sobre consistência eventual, resiliência e desacoplamento
- Responder perguntas conceituais do professor durante a apresentação
- Conectar a implementação técnica com a teoria da aula

Use em conjunto com `/tp` para alinhar os fundamentos teóricos com os requisitos práticos do trabalho.
