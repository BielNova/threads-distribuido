public class Mensagem {

    private final Usuario usuario;
    private final String texto;

    public Mensagem(Usuario usuario, String texto) {
        this.usuario = usuario;
        this.texto = texto;
    }

    @Override
    public String toString() {
        return usuario.getNome() + ": " + texto;
    }
}