package threads.persistencia;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import threads.comum.Config;
import threads.comum.Json;
import threads.comum.Mensagem;
import threads.comum.Rabbit;
import threads.comum.Util;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Serviço de persistência (CONSUMIDOR). Pode rodar em várias réplicas ao mesmo tempo:
 * o RabbitMQ distribui as mensagens da fila "mensagens.novas" entre elas (competing consumers).
 *
 * É o equivalente distribuído do Canal.adicionarMensagem() synchronized da Parte 1:
 * em vez de um lock na memória, usamos o lock de linha do Postgres na tabela "canais".
 */
public class PersistenciaWorker {

    private static final Logger log = LoggerFactory.getLogger("persistencia");

    private static final int CONSUMIDORES = Config.inteiro("CONSUMIDORES", 2);   // consumidores (canais AMQP) por processo
    private static final int PREFETCH = Config.inteiro("PREFETCH", 20);          // máx. de mensagens sem ack por consumidor
    private static final int ATRASO_MS = Config.inteiro("ATRASO_PROCESSAMENTO_MS", 0); // simula trabalho (demo de falha)
    private static final boolean LOG_MSGS = Boolean.parseBoolean(Config.str("LOG_MSGS", "true"));
    private static final int MAX_TENTATIVAS = 8;

    private static final AtomicLong gravadas = new AtomicLong();
    private static final AtomicLong duplicadas = new AtomicLong();
    private static final AtomicLong rejeitadas = new AtomicLong();
    private static final AtomicLong errosBanco = new AtomicLong();

    private static HikariDataSource db;
    private static Channel canalEventos;

    public static void main(String[] args) throws Exception {
        String eu = Config.instancia();
        db = threads.comum.Db.criarPool(CONSUMIDORES + 1);
        Connection conn = Rabbit.conectar("persistencia");

        try (Channel setup = conn.createChannel()) {
            Rabbit.declararTopologia(setup);
        }
        canalEventos = conn.createChannel();

        for (int i = 0; i < CONSUMIDORES; i++) {
            Channel ch = conn.createChannel();
            ch.basicQos(PREFETCH);
            // autoAck = false -> só removemos da fila depois de gravar no banco
            ch.basicConsume(Rabbit.FILA_MENSAGENS, false, eu + "-c" + i, new Consumidor(ch));
        }
        log.info("[{}] pronto: {} consumidores, prefetch={}, atraso={}ms", eu, CONSUMIDORES, PREFETCH, ATRASO_MS);

        // resumo periódico (útil para acompanhar a demo e o benchmark)
        long ultimo = -1;
        while (true) {
            Util.dormir(2_000);
            long g = gravadas.get();
            if (g != ultimo) {
                log.info("[{}] total gravadas={} duplicadas_ignoradas={} rejeitadas_dlq={} erros_banco={}",
                        eu, g, duplicadas.get(), rejeitadas.get(), errosBanco.get());
                ultimo = g;
            }
        }
    }

    private static final class Consumidor extends DefaultConsumer {

        Consumidor(Channel ch) {
            super(ch);
        }

        @Override
        public void handleDelivery(String tag, Envelope env, AMQP.BasicProperties props, byte[] corpo) throws java.io.IOException {
            Channel ch = getChannel();
            long deliveryTag = env.getDeliveryTag();

            // 1) Mensagem malformada: não adianta tentar de novo -> Dead Letter Queue (não se perde, fica para análise)
            Mensagem m;
            try {
                m = Json.ler(corpo, Mensagem.class);
                m.validar();
            } catch (Exception e) {
                rejeitadas.incrementAndGet();
                log.warn("Mensagem inválida enviada para a DLQ: {}", e.getMessage());
                ch.basicReject(deliveryTag, false);
                return;
            }

            if (ATRASO_MS > 0) Util.dormir(ATRASO_MS);

            // 2) Grava com retry + backoff. Enquanto não gravar, NÃO dá ack:
            //    se este processo morrer aqui, o RabbitMQ entrega a mensagem para outra réplica.
            long espera = 200;
            for (int tentativa = 1; ; tentativa++) {
                try {
                    Resultado r = gravar(m);
                    if (r.duplicada) {
                        duplicadas.incrementAndGet();
                        log.info("[{}] DUPLICADA ignorada id={} (já estava gravada — reentrega após falha)",
                                Config.instancia(), m.id());
                    } else {
                        gravadas.incrementAndGet();
                        if (LOG_MSGS) {
                            log.info("[{}] gravada canal={} seq={} {}: {}", Config.instancia(), m.canal(), r.seq, m.usuario(), m.texto());
                        }
                        publicarEvento(m, r.seq);
                    }
                    ch.basicAck(deliveryTag, false);   // 3) só agora a mensagem sai da fila
                    return;
                } catch (SQLException e) {
                    errosBanco.incrementAndGet();
                    if (tentativa >= MAX_TENTATIVAS) {
                        // devolve para a fila (requeue=true): outra réplica ou esta mesma tenta depois
                        log.error("Banco indisponível após {} tentativas, devolvendo id={} para a fila", tentativa, m.id());
                        Util.dormir(espera);
                        ch.basicNack(deliveryTag, false, true);
                        return;
                    }
                    log.warn("Falha no banco (tentativa {}/{}): {}. Retry em {} ms", tentativa, MAX_TENTATIVAS, e.getMessage(), espera);
                    Util.dormir(espera);
                    espera = Math.min(espera * 2, 5_000);
                }
            }
        }
    }

    private record Resultado(long seq, boolean duplicada) {}

    /**
     * Grava a mensagem com consistência forte na ordem do canal.
     *
     * 1. UPDATE canais SET ultimo_seq = ultimo_seq + 1 ... RETURNING
     *    -> trava a linha do canal até o COMMIT: duas réplicas gravando no mesmo canal
     *       ficam em fila aqui, igual ao synchronized da Parte 1. Resultado: seq 1, 2, 3... sem buracos nem repetição.
     * 2. INSERT ... ON CONFLICT (id) DO NOTHING
     *    -> se a mensagem já foi gravada (reentrega após queda de um worker), não insere de novo
     *       e fazemos ROLLBACK para devolver o número de sequência. Isso torna o consumo IDEMPOTENTE.
     */
    private static Resultado gravar(Mensagem m) throws SQLException {
        try (java.sql.Connection c = db.getConnection()) {
            c.setAutoCommit(false);
            try {
                try (PreparedStatement ps = c.prepareStatement(
                        "INSERT INTO canais(nome) VALUES (?) ON CONFLICT (nome) DO NOTHING")) {
                    ps.setString(1, m.canal());
                    ps.executeUpdate();
                }

                long seq;
                try (PreparedStatement ps = c.prepareStatement(
                        "UPDATE canais SET ultimo_seq = ultimo_seq + 1 WHERE nome = ? RETURNING ultimo_seq")) {
                    ps.setString(1, m.canal());
                    try (ResultSet rs = ps.executeQuery()) {
                        rs.next();
                        seq = rs.getLong(1);
                    }
                }

                int inseridas;
                try (PreparedStatement ps = c.prepareStatement("""
                        INSERT INTO mensagens(id, canal, seq, usuario, texto, enviada_em, processada_por)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT (id) DO NOTHING""")) {
                    ps.setObject(1, m.id());
                    ps.setString(2, m.canal());
                    ps.setLong(3, seq);
                    ps.setString(4, m.usuario());
                    ps.setString(5, m.texto());
                    ps.setTimestamp(6, Timestamp.from(Instant.ofEpochMilli(m.enviadaEmMillis())));
                    ps.setString(7, Config.instancia());
                    inseridas = ps.executeUpdate();
                }

                if (inseridas == 0) {
                    c.rollback();
                    return new Resultado(-1, true);
                }
                c.commit();
                return new Resultado(seq, false);
            } catch (SQLException e) {
                try { c.rollback(); } catch (SQLException ignorada) { /* conexão já morreu */ }
                throw e;
            }
        }
    }

    /** Avisa os gateways (fanout) que há mensagem nova -> eles empurram para os navegadores via SSE. */
    private static synchronized void publicarEvento(Mensagem m, long seq) {
        Map<String, Object> ev = new LinkedHashMap<>();
        ev.put("tipo", "mensagem");
        ev.put("id", m.id());
        ev.put("canal", m.canal());
        ev.put("seq", seq);
        ev.put("usuario", m.usuario());
        ev.put("texto", m.texto());
        ev.put("versao", 1);
        ev.put("processadaPor", Config.instancia());
        try {
            // evento de notificação: não é a fonte da verdade (o banco é), então não precisa ser persistente
            canalEventos.basicPublish(Rabbit.EXCHANGE_EVENTOS, "", null, Json.bytes(ev));
        } catch (Exception e) {
            log.warn("Não foi possível publicar evento de tempo real: {}", e.getMessage());
        }
    }
}
