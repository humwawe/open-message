package hum.open.message.producer;

import hum.open.message.Message;
import hum.open.message.serializer.MessageSerializer;

import java.nio.ByteBuffer;

/**
 * @author hum
 */
public class BucketWriter {
    private final String name;
    private final BufferService bufferService;

    private volatile ByteBuffer buffer;
    private final MessageSerializer serializer = new MessageSerializer();

    public BucketWriter(String name, BufferService bufferService) {
        this.name = name;
        this.bufferService = bufferService;
        buffer = bufferService.getBuffer(name, 0);
    }

    public void putMessage(Message message, ByteBuffer localBuffer) {
        localBuffer.clear();
        serializer.write(localBuffer, message);
        localBuffer.flip();

        int messageSize = localBuffer.limit();
        int messageOffset;
        synchronized (this) {
            if (messageSize > buffer.remaining()) {
                buffer = bufferService.getBuffer(name, buffer.position());
            }
            messageOffset = buffer.position();
            buffer.position(messageOffset + messageSize);
        }
        ByteBuffer messageBuffer = buffer.duplicate();
        messageBuffer.position(messageOffset);
        messageBuffer.put(localBuffer);
    }
}
