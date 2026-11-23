package threads.comum;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Conexão com o RabbitMQ e declaração da topologia (exchanges e filas).
 *
 * Garantias contra perda de mensagem:
 *  - exchanges e filas DURÁVEIS (sobrevivem a reinício do broker);
 *  - mensagens PERSISTENTES (deliveryMode=2, gravadas em disco);
 *  - publisher confirms no produtor (o gateway só responde 202 depois do broker confirmar);
 *  - ack MANUAL no consumidor (só confirma depois de gravar no banco);
 *  - mensagens inválidas vão para uma Dead Letter Queue em vez de sumir.
 *
 *   gateway --(chat / "mensagem.nova")--> [mensagens.novas] --> persistencia (N réplicas)
 *   persistencia --(chat.eventos, fanout)--> [fila exclusiva de cada gateway] --> navegador (SSE)
 *   gateway --(chat / "relatorio.solicitado")--> [relatorios.solicitados] --> analise
 *   rejeitadas --(chat.dlx)--> [mensagens.dlq]
 */
public final class Rabbit {

    private static final Logger log = LoggerFactory.getLogger(Rabbit.class);

    public static final String EXCHANGE = "chat";
    public static final String EXCHANGE_EVENTOS = "chat.eventos";
    public static final String EXCHANGE_DLX = "chat.dlx";

    public static final String FILA_MENSAGENS = "mensagens.novas";
    public static final String FILA_RELATORIOS = "relatorios.solicitados";
    public static final String FILA_DLQ = "mensagens.dlq";

    public static final String RK_MENSAGEM = "mensagem.nova";
    public static final String RK_RELATORIO = "relatorio.solicitado";

    /** Propriedades de mensagem persistente (deliveryMode = 2) em JSON. */
    public static final AMQP.BasicProperties PERSISTENTE = MessageProperties.PERSISTENT_BASIC
            .builder().contentType("application/json").build();

    private Rabbit() {}

    /** Conecta com retry e reconexão automática habilitada. */
    public static Connection conectar(String nomeConexao) {
        ConnectionFactory f = new ConnectionFactory();
        f.setHost(Config.rabbitHost());
        f.setUsername(Config.str("RABBIT_USER", "guest"));
        f.setPassword(Config.str("RABBIT_PASS", "guest"));
        f.setAutomaticRecoveryEnabled(true);   // reconecta sozinho se o broker cair
        f.setTopologyRecoveryEnabled(true);    // e redeclara filas/consumidores
        f.setNetworkRecoveryInterval(2_000);
        f.setRequestedHeartbeat(10);           // detecta conexão morta em ~10-20 s

        long espera = 500;
        while (true) {
            try {
                Connection c = f.newConnection(nomeConexao + "@" + Config.instancia());
                log.info("Conectado ao RabbitMQ em {}", Config.rabbitHost());
                return c;
            } catch (Exception e) {
                log.warn("RabbitMQ indisponível ({}). Tentando de novo em {} ms", e.getMessage(), espera);
                Util.dormir(espera);
                espera = Math.min(espera * 2, 5_000);
            }
        }
    }

    /** Declara tudo de forma idempotente (todo serviço chama ao subir; quem chegar primeiro cria). */
    public static void declararTopologia(Channel ch) throws java.io.IOException {
        ch.exchangeDeclare(EXCHANGE, BuiltinExchangeType.DIRECT, true);
        ch.exchangeDeclare(EXCHANGE_EVENTOS, BuiltinExchangeType.FANOUT, true);
        ch.exchangeDeclare(EXCHANGE_DLX, BuiltinExchangeType.FANOUT, true);

        Map<String, Object> comDlq = Map.of("x-dead-letter-exchange", EXCHANGE_DLX);

        ch.queueDeclare(FILA_MENSAGENS, true, false, false, comDlq);
        ch.queueBind(FILA_MENSAGENS, EXCHANGE, RK_MENSAGEM);

        ch.queueDeclare(FILA_RELATORIOS, true, false, false, comDlq);
        ch.queueBind(FILA_RELATORIOS, EXCHANGE, RK_RELATORIO);

        ch.queueDeclare(FILA_DLQ, true, false, false, null);
        ch.queueBind(FILA_DLQ, EXCHANGE_DLX, "");
    }
}
