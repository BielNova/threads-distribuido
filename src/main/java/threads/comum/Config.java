package threads.comum;

/** Configuração lida de variáveis de ambiente (definidas no docker-compose.yml). */
public final class Config {

    private Config() {}

    public static String str(String nome, String padrao) {
        String v = System.getenv(nome);
        return (v == null || v.isBlank()) ? padrao : v;
    }

    public static int inteiro(String nome, int padrao) {
        return Integer.parseInt(str(nome, String.valueOf(padrao)));
    }

    /** Nome desta instância (hostname do container) — aparece nos logs e no banco. */
    public static String instancia() {
        return str("HOSTNAME", "local");
    }

    public static String rabbitHost() { return str("RABBIT_HOST", "localhost"); }
    public static String dbUrl() { return str("DB_URL", "jdbc:postgresql://localhost:5432/threads"); }
    public static String dbUser() { return str("DB_USER", "threads"); }
    public static String dbPass() { return str("DB_PASS", "threads"); }
}
