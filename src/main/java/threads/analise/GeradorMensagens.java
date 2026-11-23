package threads.analise;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/** Gera mensagens sintéticas (determinísticas: mesma semente = mesmos dados) para seed e benchmark. */
public final class GeradorMensagens {

    private static final String[] PALAVRAS = {
            "olá", "bom", "dia", "pessoal", "reunião", "amanhã", "projeto", "entrega", "código", "função",
            "servidor", "banco", "fila", "mensagem", "canal", "usuário", "thread", "concorrência", "paralelismo",
            "docker", "container", "rabbit", "postgres", "consistência", "falha", "réplica", "teste", "benchmark",
            "ótimo", "massa", "valeu", "obrigado", "ruim", "erro", "bug", "atrasado", "problema", "travou",
            "café", "almoço", "aula", "professor", "prova", "requisitos", "apresentação", "slide", "relatório",
            "você", "também", "não", "então", "porém", "já", "até", "lá", "aí", "isso", "aqui", "agora",
            "sistema", "distribuído", "assíncrono", "latência", "vazão", "gargalo", "lock", "transação",
            "golpe", "pirataria", "fraude", "parabéns", "excelente", "show", "top", "horrível", "caiu",
            "ação", "atenção", "decisão", "informação", "configuração", "execução", "implementação"};

    private static final String[] USUARIOS = {"ana", "bruno", "caio", "duda", "edu", "fabi", "gabi", "hugo",
            "iris", "joao", "karla", "leo", "mari", "nina", "otavio", "paula", "rani", "sofia", "tiago", "vini"};

    private static final String[] SPAM = {
            "Promoção imperdível clique no spamlink agora!!!",
            "Ganhe dinheiro fácil sem esforço, chama no privado",
            "Bom dia grupo, bom dia grupo, bom dia grupo"};

    private GeradorMensagens() {}

    public static List<AnalisadorMensagens.Entrada> gerar(int n, long semente) {
        Random r = new Random(semente);
        List<AnalisadorMensagens.Entrada> lista = new ArrayList<>(n);
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < n; i++) {
            String usuario = USUARIOS[r.nextInt(USUARIOS.length)];
            String texto;
            if (r.nextInt(50) == 0) {
                texto = SPAM[r.nextInt(SPAM.length)];
            } else {
                sb.setLength(0);
                int tamanho = 6 + r.nextInt(25);  // 6 a 30 palavras
                for (int w = 0; w < tamanho; w++) {
                    if (w > 0) sb.append(r.nextInt(8) == 0 ? ", " : " ");
                    String p = PALAVRAS[r.nextInt(PALAVRAS.length)];
                    sb.append(r.nextInt(6) == 0 ? Character.toUpperCase(p.charAt(0)) + p.substring(1) : p);
                }
                sb.append(r.nextBoolean() ? "." : "!");
                texto = sb.toString();
            }
            lista.add(new AnalisadorMensagens.Entrada(usuario, texto));
        }
        return lista;
    }

    public static String[] usuarios() {
        return USUARIOS.clone();
    }
}
