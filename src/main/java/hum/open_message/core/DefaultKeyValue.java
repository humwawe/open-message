package hum.open_message.core;

import hum.open_message.KeyValue;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @author hum
 */
public class DefaultKeyValue implements KeyValue {
    private final Map<String, Object> kvs = new HashMap<>();

    @Override
    public KeyValue put(String key, String value) {
        kvs.put(key, value);
        return this;
    }

    @Override
    public String getString(String key) {
        return (String) kvs.getOrDefault(key, null);
    }

    @Override
    public Set<String> keySet() {
        return kvs.keySet();
    }

    public String removeString(String key) {
        return (String) kvs.remove(key);
    }
}
