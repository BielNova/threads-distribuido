# Roteiro da apresentação (≈ 10–12 min) + perguntas prováveis

## Antes de começar (5 min antes)

```bash
docker compose up -d --build        # sobe tudo
docker compose run --rm ferramentas seed historico 500000   # só se ainda não tiver feito
```

Deixe abertos:
1. `http://localhost:8080` (interface do chat);
2. `http://localhost:15672` (painel do RabbitMQ, login guest/guest), na aba **Queues**;
3. um terminal na pasta do projeto.

## Sequência

| Tempo | O que mostrar | O que falar |
|---|---|---|
| 1 min | Diagrama do README | "Na Parte 1 era um processo só com 3 threads e um `synchronized`. Agora são 8 containers. A ideia é a mesma, só que distribuída." |
| 1 min | Interface: mandar uma mensagem | "O gateway responde **202**: aceitou e pôs na fila. Quem grava é o serviço de persistência. Olhem o `X-Gateway` mudando: o nginx alterna entre as 2 réplicas." |
| 1 min | Botão **Rajada** (X, Y e Z × 100) + painel do RabbitMQ | "É a `Main` da Parte 1 pela rede. A fila sobe e desce: produtor e consumidor estão desacoplados." |
| 2 min | `scripts/demo-consistencia.sh` | "Foram 2000 mensagens no mesmo canal, gravadas por 2 réplicas, e a seq ficou de 1 a 2000 sem buraco. Na edição simultânea, uma leva 200 e a outra 409." |
| 3 min | `scripts/demo-falha.sh consumidor` e depois `gateway` | "Matamos com kill -9 no meio da carga. As mensagens sem ack voltam para a fila e a outra réplica pega. No fim: enviadas = gravadas." |
| 2 min | Relatório paralelo na interface (canal **historico**, 8 threads) + tabela do benchmark | "O sequencial levou X ms e o paralelo Y ms. Mas o carregamento do banco agora é o gargalo (lei de Amdahl)." |
| 1 min | Fechamento | "Consistência forte onde importa (ordem e edição) e eventual no feed ao vivo." |

Se der tempo, mostre também `scripts/demo-falha.sh banco`: o Postgres fica fora 15 s e o chat continua aceitando mensagens.

## Perguntas prováveis

**Por que RabbitMQ e não Kafka?**
Precisamos de ack individual por mensagem, *dead letter queue* e vários consumidores dividindo uma fila. No RabbitMQ isso é nativo. O Kafka brilha em log de eventos com altíssimo volume e *replay*, e não precisávamos disso.

**Como garantem que a mensagem não se perde?**
São quatro camadas:
1. O gateway publica e espera o **publisher confirm**. Só responde 202 depois que o broker gravou.
2. A fila é **durável** e a mensagem é **persistente** (deliveryMode = 2), então sobrevivem a um restart do RabbitMQ (Falha 4).
3. O consumidor usa **ack manual**: só confirma depois do COMMIT no banco. Se morrer antes, o RabbitMQ reentrega (Falha 1).
4. Mensagem inválida vai para a **DLQ** em vez de sumir.

**E se o worker gravar e morrer antes do ack? Não duplica?**
Não. O `id` da mensagem é gerado no gateway e é chave primária. O worker faz `INSERT ... ON CONFLICT (id) DO NOTHING`: se já existe, faz rollback (devolvendo o número de seq) e só dá ack. Isso torna o consumo **idempotente**: a entrega é *at-least-once*, mas o efeito é *exactly-once*.

**Como funciona a ordem das mensagens com 4 consumidores ao mesmo tempo?**
Cada canal tem uma linha com `ultimo_seq`. O `UPDATE canais SET ultimo_seq = ultimo_seq + 1 ... RETURNING` trava essa linha até o COMMIT. É o `synchronized` da Parte 1, só que o "lock" está no banco e vale para todos os processos.

**Isso não vira gargalo?**
Vira, **por canal**, e o benchmark mostra isso. Com 1 canal, colocar mais réplicas ajuda pouco. Com 8 canais, ajuda mais, porque os locks são de linhas diferentes. É uma escolha consciente: preferimos ordem correta a vazão máxima num canal só.

**Por que o modelo de consistência é forte?**
Para mensagens (ordem) e edições, sim. Ordem errada ou edição perdida é um bug visível para o usuário. O feed em tempo real (SSE) e os relatórios são **eventuais**: chegam milissegundos depois, e quem é a fonte da verdade é o banco.

**O que é o 409 na edição?**
É **controle de concorrência otimista**. O cliente manda a `versao` que leu e o UPDATE só acontece `WHERE versao = ?`. Se outro editou antes, a versão mudou, 0 linhas são afetadas e devolvemos 409 com o texto atual. Ninguém sobrescreve ninguém às cegas.

**Onde está o paralelismo?**
Em `AnalisadorMensagens.analisarParalelo()`. A lista é dividida em N blocos e cada um vira um `Callable` num `ExecutorService`. Cada thread tem seu próprio resultado parcial (sem lock, sem disputa). No fim, `Future.get()` (o "join") e juntamos tudo (map-reduce).

**Por que o speedup não é igual ao número de threads?**
Por três motivos:
- **lei de Amdahl:** a etapa de juntar os parciais é sequencial;
- **memória:** todas as threads disputam a mesma memória e cache;
- **CPU:** o processador tem núcleos de desempenho e de eficiência, e o Docker divide a CPU com o resto da máquina.

**Por que o gateway e a persistência são serviços separados?**
O gateway lida com a rede e precisa responder rápido. A persistência espera o banco. Separados, um banco lento não trava a recepção (Falha 3), e cada um escala de forma independente.

**Por que a análise é separada da persistência?**
Um relatório de 500 mil mensagens ocupa todos os núcleos por segundos. Se rodasse junto, atrasaria a gravação das mensagens do chat.

**O nginx não é um ponto único de falha?**
É, e o Postgres e o RabbitMQ também, porque cada um roda numa instância só. Em produção usaríamos RabbitMQ em cluster com *quorum queues* e Postgres com réplica e failover. Para o escopo da disciplina, demonstramos a tolerância a falhas nos serviços da aplicação e a durabilidade do broker.
