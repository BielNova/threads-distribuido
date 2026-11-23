package threads.comum;

import java.util.UUID;

/**
 * Evolução da classe Mensagem da Parte 1. Antes ela vivia só na memória de um processo;
 * agora viaja pela rede (JSON dentro do RabbitMQ), por isso precisa de um id único
 * e do instante em que foi aceita.
 */
public record Mensagem(UUID id, String canal, String usuario, String texto, long enviadaEmMillis) {

    public void validar() {
        if (id == null) throw new IllegalArgumentException("id obrigatório");
        if (canal == null || !canal.matches("[a-zA-Z0-9_-]{1,40}")) throw new IllegalArgumentException("canal inválido");
        if (usuario == null || usuario.isBlank() || usuario.length() > 40) throw new IllegalArgumentException("usuario inválido");
        if (texto == null || texto.isBlank() || texto.length() > 2000) throw new IllegalArgumentException("texto inválido");
    }
}
