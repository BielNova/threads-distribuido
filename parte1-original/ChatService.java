public class ChatService {

    public void enviarMensagem(Usuario usuario, Canal canal, String texto) {

        Mensagem mensagem = new Mensagem(usuario, texto);

        canal.adicionarMensagem(mensagem);
    }
}