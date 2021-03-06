package hum.open.message.consumer;

import hum.open.message.Message;
import hum.open.message.MessageHeader;
import hum.open.message.serializer.MessageSerializer;

import java.nio.ByteBuffer;

/**
 * @author hum
 */
public class MessageReader {
    private final String bucket;
    private final boolean isQueue;
    private final ByteBuffer buffer;
    private final MessageSerializer deserializer;

    public MessageReader(String bucket, boolean isQueue, BufferService bufferService, MessageSerializer deserializer) {
        this.bucket = bucket;
        this.isQueue = isQueue;
        this.buffer = bufferService.getBuffer(bucket);
        this.deserializer = deserializer;
    }

    public Message readMessage() {
        Message message = deserializer.read(buffer);
        if (message == null) {
            return null;
        }
        message.putHeaders(isQueue ? MessageHeader.QUEUE : MessageHeader.TOPIC, bucket);
        return message;
    }
}
