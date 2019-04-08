package hum.open_message.serializer;

import hum.open_message.BytesMessage;
import hum.open_message.KeyValue;
import hum.open_message.Message;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.Set;

/**
 * @author hum
 */
public class MessageSerializer {
    // C-style
    private final static byte NUL = (byte) 0;

    private final static byte KEY_HEADER_KEY = (byte) 0xff;
    private final static byte KEY_PRO_OFFSET = (byte) 0xfe;
    private final static String HEADER_KEY = "MessageId";
    private final static String PRO_OFFSET = "PRO_OFFSET";
    private final static String PRODUCER = "PRODUCER";

    public void write(ByteBuffer buffer, Message message) throws BufferOverflowException {
        byte[] body = ((BytesMessage) message).getBody();
        buffer.put(body).put(NUL);

        write(buffer, message.headers());
        write(buffer, message.properties());

    }

    private void write(ByteBuffer buffer, KeyValue keyValue) throws BufferOverflowException {
        if (keyValue == null) {
            buffer.put((byte) 0);
            return;
        }
        Set<String> keySet = keyValue.keySet();
        int size = keySet.size();
        if (size > 127) {
            throw new RuntimeException("keyValue has too many entries (at most 127 entries), size=" + size);
        }
        buffer.put((byte) size);
        for (String key : keySet) {
            String value = keyValue.getString(key);
            if (HEADER_KEY.equals(key)) {
                buffer.put(KEY_HEADER_KEY);
                writeValue(buffer, value);
            } else if (PRO_OFFSET.equals(key) && value.startsWith(PRODUCER)) {
                buffer.put(KEY_PRO_OFFSET);
                writeValue(buffer, value.substring(PRODUCER.length()));
            } else {
                writeKey(buffer, key);
                writeValue(buffer, value);
            }
        }
    }

    private void writeKey(ByteBuffer buffer, String string) {
        if (string.length() > 127) {
            throw new RuntimeException("key is too long (at most 127 bytes), length=" + string.length());
        }
        byte[] valueBytes = string.getBytes();
        buffer.put((byte) valueBytes.length);
        buffer.put(valueBytes);
    }

    private void writeValue(ByteBuffer buffer, String string) {
        if (string.length() > 32767) {
            throw new RuntimeException("value is too long (at most 32767 bytes), length=" + string.length());
        }
        byte[] valueBytes = string.getBytes();
        buffer.put(valueBytes).put(NUL);
    }
}