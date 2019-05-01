package hum.open.message;

import java.util.Set;

/**
 * @author hum
 */
public interface KeyValue {
    KeyValue put(String key, String value);

    String getString(String key);

    Set<String> keySet();
}
