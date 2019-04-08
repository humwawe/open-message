package hum.open_message;

import java.util.Set;

/**
 * @author hum
 */
public interface KeyValue {
    KeyValue put(String key, String value);

    String getString(String store_path);

    Set<String> keySet();
}
