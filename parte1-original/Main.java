public class Main {

    public static void main(String[] args) throws InterruptedException {

        Usuario x = new Usuario("X");
        Usuario y = new Usuario("Y");
        Usuario z = new Usuario("Z");

        Canal geral = new Canal("geral");

        ChatService chat = new ChatService();

        Thread t1 = new Thread(() ->
                chat.enviarMensagem(x, geral, "Bom dia"));

        Thread t2 = new Thread(() ->
                chat.enviarMensagem(y, geral, "E aí"));

        Thread t3 = new Thread(() ->
                chat.enviarMensagem(z, geral, "Olá pessoal"));

        t1.start();
        t2.start();
        t3.start();

        t1.join();
        t2.join();
        t3.join();

        geral.mostrarHistorico();
    }
}