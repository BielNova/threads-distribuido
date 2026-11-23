package threads.bench;

import threads.analise.AnalisadorMensagens;
import threads.analise.GeradorMensagens;

import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * BENCHMARK 1 — tarefa pesada sequencial x paralela (só CPU, sem banco nem rede).
 *
 * Metodologia:
 *  - dados: N mensagens sintéticas geradas com semente fixa (42) -> todo mundo mede a mesma coisa;
 *  - para cada nº de threads: W execuções de aquecimento (JIT do Java) que são descartadas
 *    e R execuções medidas com System.nanoTime();
 *  - reporta média, desvio padrão, mínimo, speedup (T1 / Tp) e eficiência (speedup / p);
 *  - confere que o resultado paralelo é IGUAL ao sequencial (paralelizar não pode mudar a resposta).
 *
 * Uso: bench-analise [N=1000000] [repeticoes=5] [aquecimento=2] [threads=1,2,4,8]
 */
public class BenchAnalise {

    public static void main(String[] args) throws Exception {
        int n = args.length > 0 ? Integer.parseInt(args[0]) : 1_000_000;
        int reps = args.length > 1 ? Integer.parseInt(args[1]) : 5;
        int aquec = args.length > 2 ? Integer.parseInt(args[2]) : 2;
        int cpus = Runtime.getRuntime().availableProcessors();
        int[] threads = args.length > 3
                ? Arrays.stream(args[3].split(",")).mapToInt(Integer::parseInt).toArray()
                : new int[]{1, 2, 4, 8, cpus};
        threads = Arrays.stream(threads).distinct().sorted().toArray();
        Path saida = Path.of(System.getenv().getOrDefault("RESULTADOS", "resultados"));
        Files.createDirectories(saida);

        String ambiente = String.format(Locale.ROOT, "Java %s | %s %s | CPUs visíveis=%d | heap máx=%d MB",
                System.getProperty("java.version"), System.getProperty("os.name"), System.getProperty("os.arch"),
                cpus, Runtime.getRuntime().maxMemory() / 1024 / 1024);
        System.out.println("Ambiente: " + ambiente);
        System.out.printf("Gerando %,d mensagens sintéticas (semente 42)...%n", n);
        List<AnalisadorMensagens.Entrada> msgs = GeradorMensagens.gerar(n, 42);

        Map<String, Object> referencia = AnalisadorMensagens.analisarSequencial(msgs);

        List<String[]> linhas = new ArrayList<>();
        double t1 = 0;
        for (int p : threads) {
            for (int i = 0; i < aquec; i++) AnalisadorMensagens.analisarParalelo(msgs, p);
            double[] tempos = new double[reps];
            boolean igual = true;
            for (int i = 0; i < reps; i++) {
                System.gc();
                long ini = System.nanoTime();
                Map<String, Object> r = AnalisadorMensagens.analisarParalelo(msgs, p);
                tempos[i] = (System.nanoTime() - ini) / 1e6;
                igual &= r.equals(referencia);
            }
            double media = Arrays.stream(tempos).average().orElse(0);
            double dp = Math.sqrt(Arrays.stream(tempos).map(t -> (t - media) * (t - media)).sum() / Math.max(1, reps - 1));
            double min = Arrays.stream(tempos).min().orElse(0);
            if (p == 1) t1 = media;
            double speedup = t1 / media;
            double efic = speedup / p;
            System.out.printf(Locale.ROOT, "threads=%2d  media=%8.1f ms  dp=%6.1f  min=%8.1f  speedup=%5.2fx  eficiencia=%5.1f%%  resultado_igual=%s%n",
                    p, media, dp, min, speedup, efic * 100, igual);
            linhas.add(new String[]{String.valueOf(p), f(media), f(dp), f(min), f2(speedup), f(efic * 100), String.valueOf(igual)});
        }

        try (PrintWriter csv = new PrintWriter(Files.newBufferedWriter(saida.resolve("bench-analise.csv")))) {
            csv.println("threads,media_ms,desvio_ms,min_ms,speedup,eficiencia_pct,resultado_igual_ao_sequencial");
            for (String[] l : linhas) csv.println(String.join(",", l));
        }
        try (PrintWriter md = new PrintWriter(Files.newBufferedWriter(saida.resolve("bench-analise.md")))) {
            md.printf("Ambiente: %s  %nVolume: %,d mensagens | %d repetições medidas + %d de aquecimento por configuração%n%n", ambiente, n, reps, aquec);
            md.println("| Threads | Tempo médio (ms) | Desvio (ms) | Mínimo (ms) | Speedup | Eficiência | Resultado = sequencial |");
            md.println("|---:|---:|---:|---:|---:|---:|:---:|");
            for (String[] l : linhas) {
                md.printf("| %s | %s | %s | %s | %sx | %s%% | %s |%n", l[0], l[1], l[2], l[3], l[4], l[5], l[6].equals("true") ? "sim" : "NÃO");
            }
        }
        System.out.println("Resultados gravados em " + saida.toAbsolutePath());
    }

    private static String f(double v) { return String.format(Locale.ROOT, "%.1f", v); }
    private static String f2(double v) { return String.format(Locale.ROOT, "%.2f", v); }
}
