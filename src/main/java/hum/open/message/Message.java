package hum.open.message;

/**
 * @author hum
 */
public interface Message {
    KeyValue headers();

    KeyValue properties();

    Message putHeaders(String key, String value);

    Message putProperties(String key, String value);


}
