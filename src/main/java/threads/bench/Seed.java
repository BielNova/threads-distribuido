package threads.bench;

import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import threads.analise.AnalisadorMensagens;
import threads.analise.GeradorMensagens;
import threads.comum.Db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.util.List;
import java.util.UUID;

/**
 * Popula um canal com N mensagens sintéticas direto no banco (sem passar pela fila),
 * só para termos volume para o relatório pesado.  Uso: seed <canal> <quantidade>
 */
public class Seed {

    private static final Logger log = LoggerFactory.getLogger("seed");

    public static void main(String[] args) throws Exception {
        String canal = args.length > 0 ? args[0] : "historico";
        int n = args.length > 1 ? Integer.parseInt(args[1]) : 500_000;

        HikariDataSource db = Db.criarPool(1);
        List<AnalisadorMensagens.Entrada> msgs = GeradorMensagens.gerar(n, 42);
        long t0 = System.currentTimeMillis();

        try (Connection c = db.getConnection()) {
            c.setAutoCommit(false);
            try (PreparedStatement ps = c.prepareStatement("DELETE FROM mensagens WHERE canal = ?")) {
                ps.setString(1, canal);
                ps.executeUpdate();
            }
            try (PreparedStatement ps = c.prepareStatement("""
                    INSERT INTO canais(nome, ultimo_seq) VALUES (?, ?)
                    ON CONFLICT (nome) DO UPDATE SET ultimo_seq = EXCLUDED.ultimo_seq""")) {
                ps.setString(1, canal);
                ps.setLong(2, n);
                ps.executeUpdate();
            }
            Timestamp agora = new Timestamp(System.currentTimeMillis());
            try (PreparedStatement ps = c.prepareStatement("""
                    INSERT INTO mensagens(id, canal, seq, usuario, texto, enviada_em, processada_por)
                    VALUES (?, ?, ?, ?, ?, ?, 'seed')""")) {
                for (int i = 0; i < n; i++) {
                    AnalisadorMensagens.Entrada e = msgs.get(i);
                    ps.setObject(1, UUID.randomUUID());
                    ps.setString(2, canal);
                    ps.setLong(3, i + 1);
                    ps.setString(4, e.usuario());
                    ps.setString(5, e.texto());
                    ps.setTimestamp(6, agora);
                    ps.addBatch();
                    if ((i + 1) % 5_000 == 0) {
                        ps.executeBatch();
                        if ((i + 1) % 100_000 == 0) log.info("{} / {} mensagens inseridas", i + 1, n);
                    }
                }
                ps.executeBatch();
            }
            c.commit();
        }
        log.info("Canal '{}' populado com {} mensagens em {} ms", canal, n, System.currentTimeMillis() - t0);
        db.close();
    }
}
