-- Esquema do banco (executado automaticamente pelo container do Postgres na primeira subida)

-- Um canal guarda o contador de sequência das mensagens.
-- A linha do canal funciona como o "synchronized" da Parte 1: quem for gravar
-- uma mensagem trava essa linha (UPDATE ... RETURNING), então dois workers
-- nunca recebem o mesmo número de sequência.
CREATE TABLE canais (
    nome        TEXT PRIMARY KEY,
    ultimo_seq  BIGINT NOT NULL DEFAULT 0
);

CREATE TABLE mensagens (
    id              UUID PRIMARY KEY,          -- gerado no gateway: garante idempotência no reprocessamento
    canal           TEXT NOT NULL REFERENCES canais(nome),
    seq             BIGINT NOT NULL,           -- ordem total dentro do canal
    usuario         TEXT NOT NULL,
    texto           TEXT NOT NULL,
    versao          INT  NOT NULL DEFAULT 1,   -- controle de concorrência otimista nas edições
    enviada_em      TIMESTAMPTZ NOT NULL,      -- quando o gateway aceitou
    persistida_em   TIMESTAMPTZ NOT NULL DEFAULT now(),
    editada_em      TIMESTAMPTZ,
    processada_por  TEXT NOT NULL,             -- qual réplica do worker gravou (útil na demo de falha)
    UNIQUE (canal, seq)                        -- rede de segurança: nunca dois iguais na mesma posição
);

CREATE INDEX idx_mensagens_canal_seq ON mensagens (canal, seq DESC);

CREATE TABLE relatorios (
    id            UUID PRIMARY KEY,
    canal         TEXT NOT NULL,
    status        TEXT NOT NULL,               -- PENDENTE | PROCESSANDO | CONCLUIDO | ERRO
    threads       INT  NOT NULL,
    total_msgs    INT,
    duracao_ms    BIGINT,
    resultado     JSONB,
    criado_em     TIMESTAMPTZ NOT NULL DEFAULT now(),
    concluido_em  TIMESTAMPTZ
);
