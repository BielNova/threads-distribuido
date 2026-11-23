# Relatório de uso de IA — Entrega 2

**Equipe:** Arthur Xavier, Caio Melo, Gabriel Assef da Nova, Ranilton  
**Ferramenta:** Claude Code (modelo Claude Opus), utilizado como copiloto de programação e apoio técnico durante o desenvolvimento.

## 1. Participação da equipe e uso da IA

A Entrega 2 foi conduzida pela equipe como uma evolução do projeto desenvolvido na Parte 1. O trabalho anterior já contava com um modelo de domínio, uma arquitetura funcional e decisões importantes sobre concorrência. Esses elementos constituíram a base conceitual e técnica sobre a qual a segunda entrega foi construída.

Ao longo da Entrega 2, a equipe definiu os objetivos, forneceu o enunciado e os materiais do projeto, acompanhou as etapas de desenvolvimento e a validação dos resultados. Gabriel conduziu a sessão de desenvolvimento, organizando as solicitações, verificando se a base utilizada era a versão oficial e pedindo revisões quando necessário.

O Claude Code foi utilizado como copiloto de programação. A ferramenta auxiliou na implementação inicial de partes do ambiente distribuído, na configuração do Docker e dos scripts, na identificação e correção de problemas, na execução de testes e benchmarks e na organização da documentação. As soluções técnicas foram propostas e justificadas pela IA e aceitas pela equipe, que assume a responsabilidade de compreendê-las e defendê-las.

| Etapa | Participação da equipe | Apoio da IA |
|---|---|---|
| Modelo de domínio (Usuário, Canal, Mensagem, ChatService) e arquitetura base | Desenvolveu na Parte 1 o protótipo funcional, que serviu de fundamento para a Entrega 2 | Reutilizou e analisou a estrutura existente, sem reivindicar sua autoria |
| Conceitos de concorrência (`synchronized`, `start` e `join`) | Definiu e documentou esses conceitos na Parte 1 | Ajudou a transportar os conceitos para o ambiente distribuído, com lock de linha no banco e `Future.get()` |
| Compreensão teórica da concorrência | Registrou por que utilizar `synchronized`, o funcionamento de `start()` e `join()` e a variação na ordem das mensagens | Utilizou esse material como referência para apoiar a preparação da apresentação |
| Escopo e contexto da Entrega 2 | Forneceu o enunciado, o histórico do grupo e o código-fonte oficial | Leu, organizou e utilizou os materiais como contexto |
| Condução do desenvolvimento | Definiu objetivos, orientou as tarefas, questionou resultados e solicitou revisões | Executou tarefas de apoio conforme as orientações recebidas |
| Verificação da versão definitiva | Questionou se a base utilizada era realmente a versão oficial e forneceu o ZIP do grupo para conferência | Comparou os arquivos e informou que eram idênticos |
| Implementação inicial do código distribuído, Docker e scripts | Definiu o objetivo e acompanhou a implementação | Gerou a implementação, sujeita à revisão da equipe |
| Demos e benchmarks | Acompanhou e conferiu os resultados, considerando-os na avaliação do sistema | Auxiliou na execução dos testes e na coleta dos resultados |
| Apresentação oral e defesa técnica | Responsável pela apresentação e pela explicação do trabalho | Ajudou a organizar um roteiro de estudo (`ROTEIRO_APRESENTACAO.md`) |

**Síntese:** a equipe foi responsável pela concepção original, pela continuidade do projeto, pela definição dos objetivos, pela condução do desenvolvimento e pela aprovação das decisões técnicas. A IA atuou como ferramenta de apoio à implementação e à verificação, sem substituir a participação e a responsabilidade dos integrantes pelo trabalho.

## 2. Base desenvolvida pela equipe na Parte 1

A Entrega 2 não começou do zero. Ela aproveitou decisões e estruturas que já haviam sido desenvolvidas pela equipe no protótipo da Parte 1 (código em `parte1-original/`). Essa continuidade foi importante para transformar o sistema original em uma aplicação distribuída sem abandonar os conceitos que já haviam sido estudados e implementados.

| Decisão e desenvolvimento da equipe na Parte 1 | Continuidade na Entrega 2 |
|---|---|
| Separação entre `ChatService` (quem envia) e `Canal` (quem armazena) | Evoluiu para a separação entre **gateway** (produtor) e **persistência** (consumidor) |
| Proteção do histórico do canal com `synchronized` | Evoluiu para o **lock da linha do canal** no PostgreSQL (`UPDATE ... RETURNING`), aplicando o conceito entre processos |
| Várias threads enviando mensagens ao mesmo canal (`t1`, `t2`, `t3`) | Foi aproveitada no botão **Rajada X/Y/Z** da interface e no gerador de carga do benchmark |
| Uso de `join()` para aguardar todas as threads | Teve continuidade com `Future.get()` no relatório paralelo e `join()` no gerador de carga |
| Entidade `Mensagem` associada a `Usuario` e ao texto | Foi mantida e ampliada com `id` e `seq` para permitir o trânsito pela rede |

Esses pontos demonstram que a segunda entrega aproveitou uma base de autoria da equipe. A IA auxiliou na adaptação dessa base ao novo cenário, mas os conceitos e a estrutura inicial já faziam parte do trabalho desenvolvido anteriormente pelos integrantes.

## 3. Prompts e condução do trabalho

Os prompts abaixo representam momentos relevantes da interação. Eles mostram que a equipe acompanhou o processo, verificou as informações utilizadas e solicitou ajustes no resultado.

| # | Solicitação ou ação da equipe (resumo fiel) | Resultado |
|---|---|---|
| 1 | “Preciso fazer a entrega 2”, com o enunciado completo e o export do grupo do WhatsApp | A IA localizou o código da Parte 1 e apresentou uma proposta inicial de arquitetura para a nova etapa |
| 2 | “Mas como você tem certeza que é o projeto atualizado?” | A equipe questionou a confiabilidade da base; a IA reconheceu que possuía apenas indícios e pediu confirmação |
| 3 | “Aqui está o projeto que enviaram no grupo, veja se é isso mesmo” (`Threads.zip`) | A equipe forneceu o arquivo oficial para conferência; a IA comparou os arquivos com o protótipo identificado |
| 4 | “Você usou o zip que te mandei? Que era o definitivo” | A equipe solicitou uma segunda confirmação; foi realizada nova conferência por hash SHA-1 |
| 5 | “Valorize mais o trabalho da equipe na entrega 2” | A equipe orientou a revisão do relatório para explicitar melhor sua participação e a continuidade do trabalho da Parte 1 |

## 4. Alternativas técnicas consideradas

Durante o desenvolvimento, a IA propôs alternativas de arquitetura e implementação e justificou cada escolha de acordo com os objetivos da entrega, a continuidade do projeto e a necessidade de demonstrar os conceitos técnicos. A equipe aceitou as decisões abaixo e se responsabiliza por explicá-las.

| Alternativa | Decisão final | Justificativa |
|---|---|---|
| Manter **Java** e o domínio da Parte 1 | Aceita | Preserva a continuidade com o trabalho anterior e aproveita uma base que a equipe já havia desenvolvido |
| Utilizar **RabbitMQ** como broker | Aceita | Ack manual, DLQ e vários consumidores em uma fila são recursos adequados e demonstráveis |
| Separar **gateway / persistência / análise** | Aceita | Cada serviço possui um perfil de carga diferente (rede, banco e CPU) |
| Implementar **idempotência** pelo `id` da mensagem | Aceita | Como o RabbitMQ trabalha com entrega *at-least-once*, a idempotência evita duplicações em reentregas |
| Utilizar **controle otimista** (`versao`) nas edições | Aceita | Permite tratar conflitos com HTTP 409 sem bloquear todos os usuários |
| Executar em paralelo o **relatório de moderação** | Aceita | É uma tarefa de CPU com mensagens independentes |
| Adotar Spring Boot | Descartada | Esconderia ack, confirms e transações atrás de anotações, dificultando a demonstração direta desses conceitos |
| Adotar Kafka | Descartada | Acrescentaria infraestrutura sem benefício proporcional ao volume de um chat de turma |
| Gravar diretamente no banco pelo gateway | Descartada | Acoplaria o chat à disponibilidade do banco (ver Falha 3 no README) |
| Exigir consistência forte também no feed ao vivo | Descartada | O feed funciona como notificação; a fonte da verdade permanece no banco |

As justificativas de cada decisão estão no roteiro da apresentação (`ROTEIRO_APRESENTACAO.md`), para que todos os integrantes consigam defendê-las.

## 5. Problemas identificados e correções durante os testes

Os testes e benchmarks ajudaram a identificar problemas práticos. A IA identificou e corrigiu esses problemas, e a equipe acompanhou os resultados.

- **Logs não apareciam:** foi identificado um conflito de versões do `slf4j` (o `amqp-client` trazia a versão 1.7). A correção aplicada foi fixar a versão 2.0.13 no `pom.xml`.
- **Porta 5432 ocupada:** como a porta padrão do PostgreSQL estava ocupada na máquina de desenvolvimento, o serviço passou a ser exposto na porta 5433.
- **`/api/stats` lento no benchmark:** a rota somava os 500 mil registros do seed a cada consulta. Foi adicionado o filtro `?prefixo=`.
- **Botão Rajada:** foi identificado que, se o usuário trocasse de canal durante o envio, as mensagens poderiam se espalhar entre canais. A correção fixou o canal no momento do clique.

## 6. Contribuições da equipe após a geração inicial

Esta seção deve ser preenchida pelos integrantes com as alterações que efetivamente realizaram no código depois da primeira versão gerada com auxílio da IA. O objetivo é registrar contribuições verificáveis, sem atribuir tarefas que não tenham sido executadas.

| Integrante | Tarefa realizada pela equipe | Status |
|---|---|---|
|  | Gravar as mensagens em lote (*batch commit*), melhoria relacionada ao gargalo apontado pelo benchmark | ☐ |
|  | Criar novo cenário em `scripts/demo-falha.sh` (por exemplo, derrubar o serviço de análise durante um relatório) | ☐ |
|  | Melhorar a interface web (lista de canais dinâmica, indicador da fila) | ☐ |
|  | Revisar o README e ensaiar a demonstração com o roteiro | ☐ |

> **Importante:** preencham esta seção somente com atividades realmente realizadas e indiquem quem foi responsável por cada uma. Ela permite demonstrar, com exemplos concretos, a revisão e a evolução do trabalho pela equipe.

## 7. Considerações finais

A Entrega 2 representa a evolução de um projeto cuja base foi desenvolvida pela própria equipe na Parte 1. Os integrantes já haviam definido e implementado o domínio do chat, a organização das classes e os principais conceitos de concorrência. Na segunda etapa, a equipe conduziu a adaptação dessa base para uma arquitetura distribuída, estabelecendo os objetivos, acompanhando a implementação e verificando o comportamento do sistema.

O Claude Code contribuiu como copiloto de programação, oferecendo apoio na implementação inicial de componentes, na investigação de problemas, nos testes e na documentação. Seu uso não elimina a participação da equipe: os materiais de origem, a definição dos objetivos e a responsabilidade pelo resultado permaneceram com os integrantes.

Assim, o uso da IA foi complementar ao trabalho da equipe. A ferramenta ajudou a acelerar tarefas técnicas, enquanto os integrantes mantêm a responsabilidade por compreender, avaliar e apresentar a solução desenvolvida.
