import java.util.ArrayList;

public class Canal {

    private String nome;
    private ArrayList<Mensagem> mensagens;

    public Canal(String nome) {
        this.nome = nome;
        this.mensagens = new ArrayList<>();
    }

    public synchronized void adicionarMensagem(Mensagem mensagem) {
        mensagens.add(mensagem);
        System.out.println("[" + nome + "] " + mensagem);
    }

    public synchronized void mostrarHistorico() {

        System.out.println("\n--- Histórico de " + nome + " ---");

        for (Mensagem mensagem : mensagens) {
            System.out.println(mensagem);
        }
    }
}