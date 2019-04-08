package hum.open_message.core;

import hum.open_message.BytesMessage;
import hum.open_message.KeyValue;
import hum.open_message.Message;

/**
 * @author hum
 */
public class DefaultBytesMessage implements BytesMessage {
    private byte[] body;
    private KeyValue headers = new DefaultKeyValue();
    private KeyValue properties = new DefaultKeyValue();

    public DefaultBytesMessage(byte[] body) {
        this.body = body;
    }

    @Override
    public byte[] getBody() {
        return body;
    }

    @Override
    public KeyValue headers() {
        return headers;
    }

    @Override
    public KeyValue properties() {
        return properties;
    }

    @Override
    public Message putHeaders(String key, String value) {
        headers.put(key, value);
        return this;
    }

    @Override
    public Message putProperties(String key, String value) {
        properties.put(key, value);
        return this;
    }
}
