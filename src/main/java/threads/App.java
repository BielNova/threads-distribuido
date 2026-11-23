package threads;

import threads.analise.AnaliseWorker;
import threads.bench.BenchAnalise;
import threads.bench.BenchPipeline;
import threads.bench.Seed;
import threads.gateway.GatewayServer;
import threads.persistencia.PersistenciaWorker;

/**
 * Ponto de entrada único. O mesmo .jar roda papéis diferentes (um por container),
 * escolhidos pela variável de ambiente ROLE ou pelo primeiro argumento.
 *
 *   gateway       -> API HTTP + página web; publica no RabbitMQ (produtor)
 *   persistencia  -> consome mensagens do RabbitMQ e grava no Postgres (consumidor)
 *   analise       -> consome pedidos de relatório e processa em paralelo
 *   seed          -> gera mensagens sintéticas direto no banco (para o benchmark)
 *   bench-analise -> benchmark sequencial x paralelo da análise
 *   bench-pipeline-> teste de carga ponta a ponta via HTTP
 */
public class App {

    public static void main(String[] args) throws Exception {
        String role = args.length > 0 ? args[0] : System.getenv().getOrDefault("ROLE", "gateway");
        String[] resto = args.length > 1 ? java.util.Arrays.copyOfRange(args, 1, args.length) : new String[0];

        switch (role) {
            case "gateway" -> GatewayServer.main(resto);
            case "persistencia" -> PersistenciaWorker.main(resto);
            case "analise" -> AnaliseWorker.main(resto);
            case "seed" -> Seed.main(resto);
            case "bench-analise" -> BenchAnalise.main(resto);
            case "bench-pipeline" -> BenchPipeline.main(resto);
            default -> {
                System.err.println("ROLE desconhecido: " + role);
                System.exit(2);
            }
        }
    }
}
