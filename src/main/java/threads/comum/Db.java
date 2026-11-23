package threads.comum;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;

/** Pool de conexões com o Postgres. Se o banco cair, o Hikari reabre conexões quando ele voltar. */
public final class Db {

    private static final Logger log = LoggerFactory.getLogger(Db.class);

    private Db() {}

    public static HikariDataSource criarPool(int tamanho) {
        HikariConfig cfg = new HikariConfig();
        cfg.setJdbcUrl(Config.dbUrl());
        cfg.setUsername(Config.dbUser());
        cfg.setPassword(Config.dbPass());
        cfg.setMaximumPoolSize(tamanho);
        cfg.setConnectionTimeout(3_000);   // timeout: não fica pendurado se o banco sumir
        cfg.setValidationTimeout(2_000);
        cfg.setInitializationFailTimeout(-1); // não falha na subida se o banco ainda não estiver pronto
        cfg.setPoolName("db-" + Config.instancia());

        HikariDataSource ds = new HikariDataSource(cfg);
        esperarBanco(ds);
        return ds;
    }

    private static void esperarBanco(HikariDataSource ds) {
        long espera = 500;
        while (true) {
            try (Connection c = ds.getConnection()) {
                log.info("Conectado ao Postgres em {}", Config.dbUrl());
                return;
            } catch (Exception e) {
                log.warn("Postgres indisponível ({}). Tentando de novo em {} ms", e.getMessage(), espera);
                Util.dormir(espera);
                espera = Math.min(espera * 2, 5_000);
            }
        }
    }
}
