package threads.gateway;

import com.fasterxml.jackson.databind.JsonNode;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import threads.comum.Config;
import threads.comum.Db;
import threads.comum.Json;
import threads.comum.Mensagem;
import threads.comum.Rabbit;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Gateway HTTP (PRODUTOR). É o "ChatService" da Parte 1 virado serviço de rede:
 *  - POST de mensagem NÃO grava no banco: publica no RabbitMQ e responde 202 Accepted
 *    assim que o broker confirma que guardou a mensagem (publisher confirm).
 *  - leituras (histórico, relatórios, estatísticas) vão direto no Postgres.
 *  - edição usa controle de concorrência otimista (coluna versao) -> conflito vira HTTP 409.
 *  - /api/stream entrega mensagens novas em tempo real (Server-Sent Events).
 *
 * Roda em 2 réplicas atrás do nginx: se uma cair, o nginx manda as requisições para a outra.
 */
public class GatewayServer {

    private static final Logger log = LoggerFactory.getLogger("gateway");
    private static final int PORTA = Config.inteiro("PORTA", 8080);

    private static HikariDataSource db;
    private static Connection rabbit;

    /** Channel do RabbitMQ não é thread-safe: cada thread do servidor HTTP usa o seu. */
    private static final ThreadLocal<Channel> CANAL_PUBLICACAO = new ThreadLocal<>();

    private static final List<OutputStream> clientesSse = new CopyOnWriteArrayList<>();

    public static void main(String[] args) throws Exception {
        db = Db.criarPool(Config.inteiro("DB_POOL", 8));
        rabbit = Rabbit.conectar("gateway");
        try (Channel ch = rabbit.createChannel()) {
            Rabbit.declararTopologia(ch);
        }
        iniciarAssinaturaDeEventos();

        HttpServer server = HttpServer.create(new InetSocketAddress(PORTA), 512);
        server.setExecutor(Executors.newFixedThreadPool(Config.inteiro("HTTP_THREADS", 32)));
        server.createContext("/", GatewayServer::rotear);
        server.start();

        Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(
                () -> enviarSse(": ping\n\n"), 15, 15, TimeUnit.SECONDS);

        log.info("[{}] gateway ouvindo na porta {}", Config.instancia(), PORTA);
    }

    // ------------------------------------------------------------------ roteamento

    private static void rotear(HttpExchange ex) {
        String metodo = ex.getRequestMethod();
        String[] p = ex.getRequestURI().getPath().replaceAll("/+$", "").split("/");
        // p[0] = "" ; p[1] = "api" ...
        try {
            ex.getResponseHeaders().add("X-Gateway", Config.instancia());
            if (p.length <= 1 || ex.getRequestURI().getPath().equals("/index.html")) {
                estatico(ex);
            } else if (p[1].equals("health")) {
                responder(ex, 200, Map.of("status", "ok", "instancia", Config.instancia()));
            } else if (!p[1].equals("api")) {
                responder(ex, 404, Map.of("erro", "não encontrado"));
            } else if (p.length == 5 && p[2].equals("canais") && p[4].equals("mensagens") && metodo.equals("POST")) {
                enviarMensagem(ex, p[3]);
            } else if (p.length == 5 && p[2].equals("canais") && p[4].equals("mensagens") && metodo.equals("GET")) {
                historico(ex, p[3]);
            } else if (p.length == 4 && p[2].equals("mensagens") && metodo.equals("PUT")) {
                editarMensagem(ex, p[3]);
            } else if (p.length == 5 && p[2].equals("canais") && p[4].equals("relatorios") && metodo.equals("POST")) {
                pedirRelatorio(ex, p[3]);
            } else if (p.length == 4 && p[2].equals("relatorios") && metodo.equals("GET")) {
                verRelatorio(ex, p[3]);
            } else if (p.length == 3 && p[2].equals("canais") && metodo.equals("GET")) {
                listarCanais(ex);
            } else if (p.length == 3 && p[2].equals("stats")) {
                stats(ex);
            } else if (p.length == 3 && p[2].equals("stream")) {
                stream(ex);
            } else {
                responder(ex, 404, Map.of("erro", "rota não encontrada"));
            }
        } catch (IllegalArgumentException e) {
            responder(ex, 400, Map.of("erro", e.getMessage()));
        } catch (Exception e) {
            log.error("Erro em {} {}: {}", metodo, ex.getRequestURI(), e.toString());
            responder(ex, 500, Map.of("erro", e.toString()));
        }
    }

    // ------------------------------------------------------------------ escrita assíncrona

    /** POST /api/canais/{canal}/mensagens  {"usuario":"X","texto":"Bom dia"} */
    private static void enviarMensagem(HttpExchange ex, String canal) throws Exception {
        JsonNode corpo = lerJson(ex);
        // Idempotency-Key: se o cliente reenviar a mesma requisição (ex.: timeout), o id é o mesmo
        // e o worker não grava duas vezes.
        String chave = ex.getRequestHeaders().getFirst("Idempotency-Key");
        UUID id = chave != null ? UUID.nameUUIDFromBytes(chave.getBytes(StandardCharsets.UTF_8)) : UUID.randomUUID();

        Mensagem m = new Mensagem(id, canal, corpo.path("usuario").asText(null), corpo.path("texto").asText(null),
                System.currentTimeMillis());
        m.validar();

        try {
            publicarComConfirmacao(Rabbit.EXCHANGE, Rabbit.RK_MENSAGEM, Json.bytes(m));
        } catch (Exception e) {
            // broker fora do ar: avisa o cliente para tentar de novo (com a mesma Idempotency-Key)
            log.warn("Falha ao publicar no RabbitMQ: {}", e.toString());
            ex.getResponseHeaders().add("Retry-After", "2");
            responder(ex, 503, Map.of("erro", "broker indisponível, tente novamente", "id", id));
            return;
        }
        responder(ex, 202, Map.of("id", id, "status", "ACEITA", "gateway", Config.instancia()));
    }

    /**
     * Publica e ESPERA o "confirm" do RabbitMQ (a mensagem foi gravada na fila durável).
     * Sem isso, o gateway poderia responder 202 e a mensagem se perder se o broker caísse logo em seguida.
     */
    private static void publicarComConfirmacao(String exchange, String routingKey, byte[] corpo) throws Exception {
        Channel ch = CANAL_PUBLICACAO.get();
        if (ch == null || !ch.isOpen()) {
            ch = rabbit.createChannel();
            ch.confirmSelect();
            CANAL_PUBLICACAO.set(ch);
        }
        ch.basicPublish(exchange, routingKey, Rabbit.PERSISTENTE, corpo);
        ch.waitForConfirmsOrDie(5_000);
    }

    // ------------------------------------------------------------------ leituras

    /** GET /api/canais/{canal}/mensagens?limite=50 — últimas mensagens em ordem de seq. */
    private static void historico(HttpExchange ex, String canal) throws Exception {
        int limite = Math.min(500, Integer.parseInt(param(ex, "limite", "50")));
        List<Map<String, Object>> lista = new ArrayList<>();
        try (var c = db.getConnection(); PreparedStatement ps = c.prepareStatement("""
                SELECT * FROM (
                  SELECT id, seq, usuario, texto, versao, processada_por, persistida_em, editada_em
                  FROM mensagens WHERE canal = ? ORDER BY seq DESC LIMIT ?) t
                ORDER BY seq""")) {
            ps.setString(1, canal);
            ps.setInt(2, limite);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    Map<String, Object> m = new LinkedHashMap<>();
                    m.put("id", rs.getObject("id"));
                    m.put("seq", rs.getLong("seq"));
                    m.put("usuario", rs.getString("usuario"));
                    m.put("texto", rs.getString("texto"));
                    m.put("versao", rs.getInt("versao"));
                    m.put("processadaPor", rs.getString("processada_por"));
                    m.put("persistidaEm", rs.getTimestamp("persistida_em").toInstant().toString());
                    m.put("editada", rs.getTimestamp("editada_em") != null);
                    lista.add(m);
                }
            }
        }
        responder(ex, 200, lista);
    }

    /**
     * GET /api/stats — contadores + VERIFICAÇÃO DE CONSISTÊNCIA por canal:
     * para estar consistente, cada canal precisa ter seq = 1..N sem buraco nem repetição.
     */
    /**
     * GET /api/canais — lista os canais existentes (para a interface montar o seletor).
     * Não existe "criar canal" separado: o canal nasce junto com a primeira mensagem
     * (INSERT ... ON CONFLICT DO NOTHING no worker de persistência).
     */
    private static void listarCanais(HttpExchange ex) throws Exception {
        List<Map<String, Object>> lista = new ArrayList<>();
        try (var c = db.getConnection(); PreparedStatement ps = c.prepareStatement(
                "SELECT nome, ultimo_seq FROM canais WHERE nome NOT LIKE 'carga-%' AND nome NOT LIKE 'broker-%' ORDER BY nome")) {
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) lista.add(Map.of("nome", rs.getString(1), "mensagens", rs.getLong(2)));
            }
        }
        responder(ex, 200, lista);
    }

    private static void stats(HttpExchange ex) throws Exception {
        Map<String, Object> r = new LinkedHashMap<>();
        List<Map<String, Object>> canais = new ArrayList<>();
        long total = 0;
        boolean tudoOk = true;
        try (var c = db.getConnection(); PreparedStatement ps = c.prepareStatement("""
                SELECT c.nome, c.ultimo_seq, count(m.id) AS qtd, count(DISTINCT m.seq) AS distintos,
                       coalesce(min(m.seq),0) AS min_seq, coalesce(max(m.seq),0) AS max_seq
                FROM canais c LEFT JOIN mensagens m ON m.canal = c.nome
                WHERE c.nome LIKE ? || '%'
                GROUP BY c.nome, c.ultimo_seq ORDER BY c.nome""")) {
            ps.setString(1, param(ex, "prefixo", ""));
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    long qtd = rs.getLong("qtd");
                    boolean ok = qtd == rs.getLong("distintos") && qtd == rs.getLong("max_seq")
                            && qtd == rs.getLong("ultimo_seq") && (qtd == 0 || rs.getLong("min_seq") == 1);
                    tudoOk &= ok;
                    total += qtd;
                    Map<String, Object> m = new LinkedHashMap<>();
                    m.put("canal", rs.getString("nome"));
                    m.put("mensagens", qtd);
                    m.put("ultimoSeq", rs.getLong("ultimo_seq"));
                    m.put("seqSemBuracosNemRepeticao", ok);
                    canais.add(m);
                }
            }
        }
        r.put("totalMensagens", total);
        r.put("consistente", tudoOk);
        r.put("canais", canais);
        try (var c = db.getConnection(); PreparedStatement ps = c.prepareStatement(
                "SELECT processada_por, count(*) FROM mensagens WHERE canal LIKE ? || '%' GROUP BY 1 ORDER BY 1")) {
            ps.setString(1, param(ex, "prefixo", ""));
            Map<String, Long> por = new LinkedHashMap<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) por.put(rs.getString(1), rs.getLong(2));
            }
            r.put("gravadasPorReplica", por);
        }
        try (Channel ch = rabbit.createChannel()) {
            r.put("filaPendentes", ch.queueDeclarePassive(Rabbit.FILA_MENSAGENS).getMessageCount());
            r.put("filaConsumidores", ch.queueDeclarePassive(Rabbit.FILA_MENSAGENS).getConsumerCount());
            r.put("dlq", ch.queueDeclarePassive(Rabbit.FILA_DLQ).getMessageCount());
        } catch (Exception e) {
            r.put("filaPendentes", "broker indisponível");
        }
        responder(ex, 200, r);
    }

    // ------------------------------------------------------------------ edição com conflito

    /**
     * PUT /api/mensagens/{id}  {"texto":"novo", "versao": 1}
     * Controle de concorrência OTIMISTA: só atualiza se a versão no banco ainda for a que o cliente leu.
     * Duas edições simultâneas da mesma versão -> uma vence (200), a outra recebe 409 Conflict
     * e precisa reler a mensagem antes de tentar de novo. Nenhuma edição é sobrescrita "às cegas".
     */
    private static void editarMensagem(HttpExchange ex, String idTxt) throws Exception {
        UUID id = UUID.fromString(idTxt);
        JsonNode corpo = lerJson(ex);
        String texto = corpo.path("texto").asText(null);
        if (texto == null || texto.isBlank()) throw new IllegalArgumentException("texto obrigatório");
        if (!corpo.has("versao")) throw new IllegalArgumentException("versao obrigatória (a que você leu)");
        int versaoLida = corpo.path("versao").asInt();

        try (var c = db.getConnection()) {
            try (PreparedStatement ps = c.prepareStatement("""
                    UPDATE mensagens SET texto = ?, versao = versao + 1, editada_em = now()
                    WHERE id = ? AND versao = ?
                    RETURNING canal, seq, usuario, versao""")) {
                ps.setString(1, texto);
                ps.setObject(2, id);
                ps.setInt(3, versaoLida);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        Map<String, Object> ev = new LinkedHashMap<>();
                        ev.put("tipo", "edicao");
                        ev.put("id", id);
                        ev.put("canal", rs.getString("canal"));
                        ev.put("seq", rs.getLong("seq"));
                        ev.put("usuario", rs.getString("usuario"));
                        ev.put("texto", texto);
                        ev.put("versao", rs.getInt("versao"));
                        publicarEventoSemConfirmacao(ev);
                        log.info("Edição aceita id={} versao {} -> {}", id, versaoLida, rs.getInt("versao"));
                        responder(ex, 200, ev);
                        return;
                    }
                }
            }
            // não atualizou: ou não existe, ou alguém editou antes (versão mudou)
            try (PreparedStatement ps = c.prepareStatement("SELECT versao, texto FROM mensagens WHERE id = ?")) {
                ps.setObject(1, id);
                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next()) {
                        responder(ex, 404, Map.of("erro", "mensagem não existe"));
                    } else {
                        log.info("CONFLITO na edição id={}: cliente leu versao {}, banco está na {}", id, versaoLida, rs.getInt(1));
                        responder(ex, 409, Map.of("erro", "conflito: a mensagem foi editada por outra pessoa",
                                "versaoAtual", rs.getInt(1), "textoAtual", rs.getString(2)));
                    }
                }
            }
        }
    }

    // ------------------------------------------------------------------ relatórios (tarefa paralela)

    /** POST /api/canais/{canal}/relatorios  {"threads":8,"comparar":true} -> 202 com id do relatório */
    private static void pedirRelatorio(HttpExchange ex, String canal) throws Exception {
        JsonNode corpo = lerJson(ex);
        int threads = corpo.path("threads").asInt(Runtime.getRuntime().availableProcessors());
        boolean comparar = corpo.path("comparar").asBoolean(true);
        UUID id = UUID.randomUUID();
        try (var c = db.getConnection(); PreparedStatement ps = c.prepareStatement(
                "INSERT INTO relatorios(id, canal, status, threads) VALUES (?, ?, 'PENDENTE', ?)")) {
            ps.setObject(1, id);
            ps.setString(2, canal);
            ps.setInt(3, threads);
            ps.executeUpdate();
        }
        Map<String, Object> pedido = Map.of("id", id, "canal", canal, "threads", threads, "comparar", comparar);
        publicarComConfirmacao(Rabbit.EXCHANGE, Rabbit.RK_RELATORIO, Json.bytes(pedido));
        responder(ex, 202, Map.of("id", id, "status", "PENDENTE"));
    }

    private static void verRelatorio(HttpExchange ex, String idTxt) throws Exception {
        try (var c = db.getConnection(); PreparedStatement ps = c.prepareStatement(
                "SELECT canal, status, threads, total_msgs, duracao_ms, resultado::text FROM relatorios WHERE id = ?")) {
            ps.setObject(1, UUID.fromString(idTxt));
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    responder(ex, 404, Map.of("erro", "relatório não existe"));
                    return;
                }
                Map<String, Object> r = new LinkedHashMap<>();
                r.put("id", idTxt);
                r.put("canal", rs.getString(1));
                r.put("status", rs.getString(2));
                r.put("threads", rs.getInt(3));
                r.put("totalMensagens", rs.getObject(4));
                r.put("duracaoMs", rs.getObject(5));
                String json = rs.getString(6);
                r.put("resultado", json == null ? null : Json.MAPPER.readTree(json));
                responder(ex, 200, r);
            }
        }
    }

    // ------------------------------------------------------------------ tempo real (SSE)

    /** Cada gateway cria uma fila exclusiva (temporária) ligada ao fanout de eventos. */
    private static void iniciarAssinaturaDeEventos() throws IOException {
        Channel ch = rabbit.createChannel();
        String fila = ch.queueDeclare().getQueue(); // nome aleatório, exclusiva, auto-delete
        ch.queueBind(fila, Rabbit.EXCHANGE_EVENTOS, "");
        ch.basicConsume(fila, true, new DefaultConsumer(ch) {
            @Override
            public void handleDelivery(String t, Envelope e, AMQP.BasicProperties p, byte[] corpo) {
                enviarSse("data: " + new String(corpo, StandardCharsets.UTF_8) + "\n\n");
            }
        });
    }

    private static void publicarEventoSemConfirmacao(Map<String, Object> ev) {
        try (Channel ch = rabbit.createChannel()) {
            ch.basicPublish(Rabbit.EXCHANGE_EVENTOS, "", null, Json.bytes(ev));
        } catch (Exception e) {
            log.warn("Evento de tempo real não publicado: {}", e.getMessage());
        }
    }

    private static void stream(HttpExchange ex) throws IOException {
        ex.getResponseHeaders().add("Content-Type", "text/event-stream; charset=utf-8");
        ex.getResponseHeaders().add("Cache-Control", "no-cache");
        ex.getResponseHeaders().add("X-Accel-Buffering", "no");
        ex.sendResponseHeaders(200, 0);
        OutputStream os = ex.getResponseBody();
        os.write((": conectado ao " + Config.instancia() + "\n\n").getBytes(StandardCharsets.UTF_8));
        os.flush();
        clientesSse.add(os); // a conexão fica aberta; enviarSse escreve nela
    }

    private static void enviarSse(String bloco) {
        byte[] b = bloco.getBytes(StandardCharsets.UTF_8);
        for (OutputStream os : clientesSse) {
            try {
                synchronized (os) {
                    os.write(b);
                    os.flush();
                }
            } catch (IOException e) {
                clientesSse.remove(os); // navegador fechou a aba
            }
        }
    }

    // ------------------------------------------------------------------ utilidades HTTP

    private static void estatico(HttpExchange ex) throws IOException {
        try (InputStream in = GatewayServer.class.getResourceAsStream("/public/index.html")) {
            byte[] b = in.readAllBytes();
            ex.getResponseHeaders().add("Content-Type", "text/html; charset=utf-8");
            ex.sendResponseHeaders(200, b.length);
            try (OutputStream os = ex.getResponseBody()) {
                os.write(b);
            }
        }
    }

    private static JsonNode lerJson(HttpExchange ex) throws IOException {
        byte[] b = ex.getRequestBody().readNBytes(64 * 1024);
        if (b.length == 0) return Json.MAPPER.createObjectNode();
        try {
            return Json.MAPPER.readTree(b);
        } catch (IOException e) {
            throw new IllegalArgumentException("JSON inválido");
        }
    }

    private static String param(HttpExchange ex, String nome, String padrao) {
        String q = ex.getRequestURI().getQuery();
        if (q == null) return padrao;
        for (String par : q.split("&")) {
            String[] kv = par.split("=", 2);
            if (kv[0].equals(nome) && kv.length == 2) return kv[1];
        }
        return padrao;
    }

    private static void responder(HttpExchange ex, int status, Object corpo) {
        try {
            byte[] b = Json.bytes(corpo);
            ex.getResponseHeaders().add("Content-Type", "application/json; charset=utf-8");
            ex.sendResponseHeaders(status, b.length);
            try (OutputStream os = ex.getResponseBody()) {
                os.write(b);
            }
        } catch (IOException e) {
            log.debug("cliente desconectou: {}", e.getMessage());
        } finally {
            ex.close();
        }
    }
}
