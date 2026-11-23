package threads.analise;

import com.fasterxml.jackson.databind.JsonNode;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import threads.comum.Config;
import threads.comum.Db;
import threads.comum.Json;
import threads.comum.Rabbit;
import threads.comum.Util;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Serviço de análise (CONSUMIDOR da fila "relatorios.solicitados").
 * Fica separado da persistência porque é CPU-bound e demorado: se rodasse no mesmo
 * processo, um relatório grande atrasaria a gravação das mensagens do chat.
 */
public class AnaliseWorker {

    private static final Logger log = LoggerFactory.getLogger("analise");

    private static HikariDataSource db;

    public static void main(String[] args) throws Exception {
        db = Db.criarPool(2);
        Connection conn = Rabbit.conectar("analise");
        Channel ch = conn.createChannel();
        Rabbit.declararTopologia(ch);
        ch.basicQos(1); // um relatório por vez por réplica: o paralelismo é DENTRO do relatório

        ch.basicConsume(Rabbit.FILA_RELATORIOS, false, new DefaultConsumer(ch) {
            @Override
            public void handleDelivery(String tag, Envelope env, AMQP.BasicProperties p, byte[] corpo) throws java.io.IOException {
                try {
                    processar(Json.MAPPER.readTree(corpo));
                    ch.basicAck(env.getDeliveryTag(), false);
                } catch (IllegalArgumentException e) {
                    log.warn("Pedido inválido -> DLQ: {}", e.getMessage());
                    ch.basicReject(env.getDeliveryTag(), false);
                } catch (Exception e) {
                    // falha transitória (ex.: banco fora): devolve para a fila e tenta de novo depois
                    log.error("Erro processando relatório, devolvendo para a fila: {}", e.toString());
                    Util.dormir(2_000);
                    ch.basicNack(env.getDeliveryTag(), false, true);
                }
            }
        });
        log.info("[{}] pronto. CPUs disponíveis = {}", Config.instancia(), Runtime.getRuntime().availableProcessors());
    }

    private static void processar(JsonNode pedido) throws Exception {
        UUID id = UUID.fromString(pedido.path("id").asText());
        String canal = pedido.path("canal").asText();
        int threads = Math.max(1, Math.min(64, pedido.path("threads").asInt(4)));
        boolean comparar = pedido.path("comparar").asBoolean(true);

        if (jaConcluido(id)) {
            log.info("Relatório {} já estava concluído (reentrega) — ignorando", id);
            return;
        }
        atualizarStatus(id, "PROCESSANDO");
        log.info("Relatório {} canal={} threads={} iniciado", id, canal, threads);

        long t0 = System.nanoTime();
        List<AnalisadorMensagens.Entrada> msgs = carregar(canal);
        long carregamentoMs = (System.nanoTime() - t0) / 1_000_000;

        Map<String, Object> resultado = new LinkedHashMap<>();
        Long seqMs = null;
        if (comparar) {
            long s0 = System.nanoTime();
            AnalisadorMensagens.analisarSequencial(msgs);
            seqMs = (System.nanoTime() - s0) / 1_000_000;
        }
        long p0 = System.nanoTime();
        Map<String, Object> analise = AnalisadorMensagens.analisarParalelo(msgs, threads);
        long parMs = (System.nanoTime() - p0) / 1_000_000;

        Map<String, Object> tempos = new LinkedHashMap<>();
        tempos.put("carregamentoBancoMs", carregamentoMs);
        if (seqMs != null) tempos.put("sequencialMs", seqMs);
        tempos.put("paraleloMs", parMs);
        tempos.put("threads", threads);
        if (seqMs != null && parMs > 0) tempos.put("speedup", Math.round(100.0 * seqMs / parMs) / 100.0);
        tempos.put("processadoPor", Config.instancia());
        resultado.put("tempos", tempos);
        resultado.putAll(analise);

        try (var c = db.getConnection(); PreparedStatement ps = c.prepareStatement("""
                UPDATE relatorios SET status='CONCLUIDO', total_msgs=?, duracao_ms=?, resultado=?::jsonb, concluido_em=now()
                WHERE id=?""")) {
            ps.setInt(1, msgs.size());
            ps.setLong(2, parMs);
            ps.setString(3, Json.MAPPER.writeValueAsString(resultado));
            ps.setObject(4, id);
            ps.executeUpdate();
        }
        log.info("Relatório {} concluído: {} msgs | banco {} ms | sequencial {} ms | paralelo({}t) {} ms",
                id, msgs.size(), carregamentoMs, seqMs, threads, parMs);
    }

    private static List<AnalisadorMensagens.Entrada> carregar(String canal) throws Exception {
        List<AnalisadorMensagens.Entrada> lista = new ArrayList<>();
        try (var c = db.getConnection()) {
            c.setAutoCommit(false); // necessário para o driver usar cursor (fetchSize) e não trazer tudo de uma vez
            try (PreparedStatement ps = c.prepareStatement("SELECT usuario, texto FROM mensagens WHERE canal = ?")) {
                ps.setFetchSize(10_000);
                ps.setString(1, canal);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) lista.add(new AnalisadorMensagens.Entrada(rs.getString(1), rs.getString(2)));
                }
            }
            c.commit();
        }
        return lista;
    }

    private static boolean jaConcluido(UUID id) throws Exception {
        try (var c = db.getConnection(); PreparedStatement ps = c.prepareStatement("SELECT status FROM relatorios WHERE id=?")) {
            ps.setObject(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) throw new IllegalArgumentException("relatório inexistente " + id);
                return "CONCLUIDO".equals(rs.getString(1));
            }
        }
    }

    private static void atualizarStatus(UUID id, String status) throws Exception {
        try (var c = db.getConnection(); PreparedStatement ps = c.prepareStatement("UPDATE relatorios SET status=? WHERE id=?")) {
            ps.setString(1, status);
            ps.setObject(2, id);
            ps.executeUpdate();
        }
    }
}
