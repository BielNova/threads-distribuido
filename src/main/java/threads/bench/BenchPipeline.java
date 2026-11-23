package threads.bench;

import com.fasterxml.jackson.databind.JsonNode;
import threads.comum.Json;

import java.io.PrintWriter;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * BENCHMARK 2 — pipeline ponta a ponta (HTTP -> gateway -> RabbitMQ -> persistência -> Postgres).
 *
 * É a "simulação da Parte 1" em escala: C usuários concorrentes enviando mensagens,
 * agora pela rede, para K canais.
 *
 * Mede:
 *  - latência de ACEITE (quanto o cliente espera pelo 202) -> p50/p95/p99
 *  - vazão de aceite (msgs/s que o gateway + broker absorvem)
 *  - tempo PONTA A PONTA até a última mensagem estar gravada no banco -> vazão real do sistema
 *
 * Uso:
 *   bench-pipeline <url> <total> <concorrencia> <canais> <rotulo>
 *   bench-pipeline resumo           (agrega o CSV em tabela markdown)
 */
public class BenchPipeline {

    private static final Path SAIDA = Path.of(System.getenv().getOrDefault("RESULTADOS", "resultados"));

    public static void main(String[] args) throws Exception {
        Files.createDirectories(SAIDA);
        if (args.length > 0 && args[0].equals("resumo")) {
            resumo();
            return;
        }
        String url = args.length > 0 ? args[0] : "http://localhost:8080";
        int total = args.length > 1 ? Integer.parseInt(args[1]) : 5_000;
        int conc = args.length > 2 ? Integer.parseInt(args[2]) : 32;
        int canais = args.length > 3 ? Integer.parseInt(args[3]) : 1;
        String rotulo = args.length > 4 ? args[4] : "teste";

        HttpClient http = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(Duration.ofSeconds(5)).build();
        String prefixo = "carga-" + UUID.randomUUID().toString().substring(0, 6);

        long[] latNs = new long[total];
        AtomicInteger proximo = new AtomicInteger();
        AtomicInteger erros = new AtomicInteger();

        long ini = System.nanoTime();
        List<Thread> ths = new ArrayList<>();
        for (int t = 0; t < conc; t++) {
            Thread th = new Thread(() -> {
                int i;
                while ((i = proximo.getAndIncrement()) < total) {
                    String canal = prefixo + "-" + (i % canais);
                    String corpo = "{\"usuario\":\"u" + (i % 50) + "\",\"texto\":\"mensagem de carga numero " + i + "\"}";
                    HttpRequest req = HttpRequest.newBuilder(URI.create(url + "/api/canais/" + canal + "/mensagens"))
                            .timeout(Duration.ofSeconds(10))
                            .header("Content-Type", "application/json")
                            .header("Idempotency-Key", prefixo + "-" + i)
                            .POST(HttpRequest.BodyPublishers.ofString(corpo)).build();
                    long t0 = System.nanoTime();
                    // cliente com retry: se der erro/503, reenvia com a MESMA Idempotency-Key
                    for (int tentativa = 0; tentativa < 5; tentativa++) {
                        try {
                            HttpResponse<Void> r = http.send(req, HttpResponse.BodyHandlers.discarding());
                            if (r.statusCode() == 202) break;
                        } catch (Exception e) { /* tenta de novo */ }
                        if (tentativa == 4) erros.incrementAndGet();
                        try { Thread.sleep(200L * (tentativa + 1)); } catch (InterruptedException ignored) { }
                    }
                    latNs[i] = System.nanoTime() - t0;
                }
            });
            th.start();
            ths.add(th);
        }
        for (Thread th : ths) th.join();   // o mesmo join() da Parte 1
        double aceiteS = (System.nanoTime() - ini) / 1e9;

        // espera tudo ser gravado no banco (consumo assíncrono)
        long gravadas = 0;
        long limite = System.currentTimeMillis() + 300_000;
        while (System.currentTimeMillis() < limite) {
            gravadas = contarGravadas(http, url, prefixo);
            if (gravadas >= total - erros.get()) break;
            Thread.sleep(100);
        }
        double e2eS = (System.nanoTime() - ini) / 1e9;

        Arrays.sort(latNs);
        double p50 = latNs[(int) (total * 0.50)] / 1e6;
        double p95 = latNs[(int) (total * 0.95)] / 1e6;
        double p99 = latNs[Math.min(total - 1, (int) (total * 0.99))] / 1e6;

        String linha = String.format(Locale.ROOT, "%s,%d,%d,%d,%.2f,%.0f,%.2f,%.0f,%.1f,%.1f,%.1f,%d,%d",
                rotulo, canais, total, conc, aceiteS, total / aceiteS, e2eS, gravadas / e2eS, p50, p95, p99, erros.get(), gravadas);
        System.out.println("rotulo,canais,total,concorrencia,aceite_s,vazao_aceite,e2e_s,vazao_e2e,p50_ms,p95_ms,p99_ms,erros,gravadas");
        System.out.println(linha);

        if (System.getenv("SEM_CSV") != null) {   // modo demo: só confere, não entra no benchmark
            System.out.printf("RESULTADO: enviadas=%d gravadas=%d erros_cliente=%d -> %s%n", total, gravadas, erros.get(),
                    gravadas == total ? "NENHUMA MENSAGEM PERDIDA" : "DIVERGÊNCIA");
            return;
        }
        Path csv = SAIDA.resolve("bench-pipeline.csv");
        if (!Files.exists(csv)) {
            Files.writeString(csv, "rotulo,canais,total,concorrencia,aceite_s,vazao_aceite,e2e_s,vazao_e2e,p50_ms,p95_ms,p99_ms,erros,gravadas\n");
        }
        Files.writeString(csv, linha + "\n", StandardOpenOption.APPEND);
    }

    private static long contarGravadas(HttpClient http, String url, String prefixo) {
        try {
            HttpResponse<String> r = http.send(HttpRequest.newBuilder(URI.create(url + "/api/stats?prefixo=" + prefixo)).build(),
                    HttpResponse.BodyHandlers.ofString());
            long soma = 0;
            for (JsonNode c : Json.MAPPER.readTree(r.body()).path("canais")) {
                if (c.path("canal").asText().startsWith(prefixo)) soma += c.path("mensagens").asLong();
            }
            return soma;
        } catch (Exception e) {
            return 0;
        }
    }

    /** Agrupa as repetições por (rótulo, canais) e calcula médias. */
    private static void resumo() throws Exception {
        List<String> linhas = Files.readAllLines(SAIDA.resolve("bench-pipeline.csv"));
        Map<String, List<double[]>> grupos = new TreeMap<>();
        for (String l : linhas.subList(1, linhas.size())) {
            if (l.isBlank()) continue;
            String[] c = l.split(",");
            String chave = c[0] + "|" + c[1];
            double[] v = new double[c.length];
            for (int i = 2; i < c.length; i++) v[i] = Double.parseDouble(c[i]);
            grupos.computeIfAbsent(chave, k -> new ArrayList<>()).add(v);
        }
        try (PrintWriter md = new PrintWriter(Files.newBufferedWriter(SAIDA.resolve("bench-pipeline.md")))) {
            md.println("| Configuração | Canais | Reps | Vazão aceite (msg/s) | Vazão ponta a ponta (msg/s) | Latência p50 (ms) | p95 (ms) | p99 (ms) | Erros |");
            md.println("|---|---:|---:|---:|---:|---:|---:|---:|---:|");
            for (var e : grupos.entrySet()) {
                List<double[]> vs = e.getValue();
                String[] k = e.getKey().split("\\|");
                md.printf(Locale.ROOT, "| %s | %s | %d | %.0f | %.0f | %.1f | %.1f | %.1f | %.0f |%n",
                        k[0], k[1], vs.size(), media(vs, 5), media(vs, 7), media(vs, 8), media(vs, 9), media(vs, 10),
                        vs.stream().mapToDouble(v -> v[11]).sum());
            }
        }
        System.out.println(Files.readString(SAIDA.resolve("bench-pipeline.md")));
    }

    private static double media(List<double[]> vs, int i) {
        return vs.stream().mapToDouble(v -> v[i]).average().orElse(0);
    }
}
