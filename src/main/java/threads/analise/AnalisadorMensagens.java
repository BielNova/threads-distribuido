package threads.analise;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.text.Normalizer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.regex.Pattern;

/**
 * TAREFA PESADA DO SISTEMA: relatório de moderação e engajamento de um canal.
 *
 * Para cada mensagem do histórico:
 *   1. normaliza o texto (minúsculas + remove acentos via Unicode NFD)  -> "Olá" vira "ola"
 *   2. quebra em palavras e conta frequência (ignorando stopwords)
 *   3. MODERAÇÃO: detecta palavras proibidas e sinaliza a mensagem
 *   4. SENTIMENTO: pontua com um léxico simples de palavras positivas/negativas
 *   5. SPAM: calcula SHA-256 do texto normalizado para achar mensagens repetidas
 *   6. conta mensagens por usuário
 *
 * Cada mensagem é independente das outras -> o problema é "embaraçosamente paralelo".
 * Por isso dividimos a lista em blocos, cada thread processa um bloco num resultado PARCIAL
 * próprio (sem compartilhar nada, sem lock) e no final juntamos (map-reduce).
 */
public final class AnalisadorMensagens {

    public record Entrada(String usuario, String texto) {}

    private static final Pattern NAO_LETRA = Pattern.compile("[^a-z0-9]+");
    private static final Pattern ACENTOS = Pattern.compile("\\p{M}+");

    static final Set<String> STOPWORDS = Set.of(
            "que", "nao", "para", "com", "uma", "por", "mais", "como", "mas", "foi", "ele", "ela",
            "isso", "esse", "essa", "tem", "ser", "seu", "sua", "dos", "das", "nos", "nas", "pra",
            "voce", "vcs", "aqui", "ai", "tambem", "ja", "so", "eu", "tu", "ate", "sem", "muito");

    static final Set<String> PROIBIDAS = Set.of("golpe", "pirataria", "xingamento", "fraude", "spamlink", "ofensa");

    static final Set<String> POSITIVAS = Set.of("bom", "boa", "otimo", "otima", "massa", "legal", "valeu",
            "obrigado", "parabens", "top", "show", "perfeito", "excelente", "feliz");
    static final Set<String> NEGATIVAS = Set.of("ruim", "pessimo", "erro", "bug", "atrasado", "triste",
            "problema", "falha", "chato", "horrivel", "travou", "caiu");

    /** Resultado parcial de UM bloco. Cada thread tem o seu -> não há disputa por memória compartilhada. */
    static final class Parcial {
        final Map<String, Integer> palavras = new HashMap<>();
        final Map<String, Integer> porUsuario = new HashMap<>();
        final Map<String, Integer> sinalizadasPorUsuario = new HashMap<>();
        final Map<String, Integer> assinaturas = new HashMap<>();
        long totalMensagens;
        long totalPalavras;
        long sinalizadas;
        long positivas;
        long negativas;

        void juntar(Parcial o) {
            o.palavras.forEach((k, v) -> palavras.merge(k, v, Integer::sum));
            o.porUsuario.forEach((k, v) -> porUsuario.merge(k, v, Integer::sum));
            o.sinalizadasPorUsuario.forEach((k, v) -> sinalizadasPorUsuario.merge(k, v, Integer::sum));
            o.assinaturas.forEach((k, v) -> assinaturas.merge(k, v, Integer::sum));
            totalMensagens += o.totalMensagens;
            totalPalavras += o.totalPalavras;
            sinalizadas += o.sinalizadas;
            positivas += o.positivas;
            negativas += o.negativas;
        }
    }

    private AnalisadorMensagens() {}

    // ------------------------------------------------------------------ versão SEQUENCIAL

    public static Map<String, Object> analisarSequencial(List<Entrada> msgs) {
        Parcial p = processarBloco(msgs, 0, msgs.size());
        return montarRelatorio(p);
    }

    // ------------------------------------------------------------------ versão PARALELA

    public static Map<String, Object> analisarParalelo(List<Entrada> msgs, int nThreads) throws Exception {
        if (nThreads <= 1) return analisarSequencial(msgs);

        ExecutorService pool = Executors.newFixedThreadPool(nThreads);
        try {
            // MAP: divide em nThreads blocos contíguos e dispara um Callable por bloco
            int n = msgs.size();
            int tamanhoBloco = (n + nThreads - 1) / nThreads;
            List<Future<Parcial>> futuros = new ArrayList<>();
            for (int inicio = 0; inicio < n; inicio += tamanhoBloco) {
                int ini = inicio;
                int fim = Math.min(n, inicio + tamanhoBloco);
                futuros.add(pool.submit(() -> processarBloco(msgs, ini, fim)));
            }

            // REDUCE: espera cada bloco (Future.get é o "join" da Parte 1) e junta os parciais
            Parcial total = new Parcial();
            for (Future<Parcial> f : futuros) {
                total.juntar(f.get());
            }
            return montarRelatorio(total);
        } finally {
            pool.shutdown();
        }
    }

    // ------------------------------------------------------------------ trabalho de verdade

    static Parcial processarBloco(List<Entrada> msgs, int inicio, int fim) {
        Parcial p = new Parcial();
        MessageDigest sha = sha256(); // MessageDigest não é thread-safe: um por bloco/thread
        HexFormat hex = HexFormat.of();

        for (int i = inicio; i < fim; i++) {
            Entrada e = msgs.get(i);
            String normalizado = normalizar(e.texto());

            boolean sinalizada = false;
            for (String palavra : NAO_LETRA.split(normalizado)) {
                if (palavra.length() < 2) continue;
                p.totalPalavras++;
                if (PROIBIDAS.contains(palavra)) sinalizada = true;
                if (POSITIVAS.contains(palavra)) p.positivas++;
                if (NEGATIVAS.contains(palavra)) p.negativas++;
                if (palavra.length() >= 3 && !STOPWORDS.contains(palavra)) {
                    p.palavras.merge(palavra, 1, Integer::sum);
                }
            }

            String assinatura = hex.formatHex(sha.digest(normalizado.getBytes(StandardCharsets.UTF_8)), 0, 8);
            p.assinaturas.merge(assinatura, 1, Integer::sum);

            p.porUsuario.merge(e.usuario(), 1, Integer::sum);
            if (sinalizada) {
                p.sinalizadas++;
                p.sinalizadasPorUsuario.merge(e.usuario(), 1, Integer::sum);
            }
            p.totalMensagens++;
        }
        return p;
    }

    static String normalizar(String texto) {
        String nfd = Normalizer.normalize(texto.toLowerCase(Locale.ROOT), Normalizer.Form.NFD);
        return ACENTOS.matcher(nfd).replaceAll("");
    }

    private static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private static Map<String, Object> montarRelatorio(Parcial p) {
        Map<String, Object> r = new LinkedHashMap<>();
        r.put("totalMensagens", p.totalMensagens);
        r.put("totalPalavras", p.totalPalavras);
        r.put("mensagensSinalizadas", p.sinalizadas);
        r.put("sentimento", Map.of("positivas", p.positivas, "negativas", p.negativas,
                "saldo", p.positivas - p.negativas));
        r.put("topPalavras", top(p.palavras, 10));
        r.put("topUsuarios", top(p.porUsuario, 10));
        r.put("usuariosMaisSinalizados", top(p.sinalizadasPorUsuario, 5));
        long repetidas = p.assinaturas.values().stream().filter(v -> v >= 5).count();
        r.put("textosRepetidos5xOuMais", repetidas);
        return r;
    }

    private static Map<String, Integer> top(Map<String, Integer> m, int k) {
        Map<String, Integer> r = new LinkedHashMap<>();
        m.entrySet().stream()
                .sorted(Map.Entry.<String, Integer>comparingByValue().reversed().thenComparing(Map.Entry.comparingByKey()))
                .limit(k)
                .forEach(e -> r.put(e.getKey(), e.getValue()));
        return r;
    }
}
