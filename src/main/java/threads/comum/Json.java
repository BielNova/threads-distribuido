package threads.comum;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

public final class Json {

    public static final ObjectMapper MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private Json() {}

    public static byte[] bytes(Object o) {
        try {
            return MAPPER.writeValueAsBytes(o);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    public static <T> T ler(byte[] dados, Class<T> tipo) throws java.io.IOException {
        return MAPPER.readValue(dados, tipo);
    }
}
