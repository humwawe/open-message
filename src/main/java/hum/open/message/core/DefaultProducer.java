package hum.open.message.core;

import hum.open.message.*;
import hum.open.message.exception.ClientOMSException;
import hum.open.message.producer.BucketManager;
import hum.open_message.*;

import java.nio.ByteBuffer;

/**
 * @author hum
 */
public class DefaultProducer implements Producer {
    private MessageFactory messageFactory = new DefaultMessageFactory();
    private KeyValue properties;
    private final BucketManager bucketManager;
    private final ByteBuffer localBuffer = ByteBuffer.allocate(105 * 1024);

    public DefaultProducer(KeyValue properties) {
        this.properties = properties;
        this.bucketManager = BucketManager.getInstance(properties.getString("STORE_PATH"));
    }

    @Override
    public void flush() {
        // do nothing
    }

    @Override
    public void send(Message message) {
        if (message == null) {
            throw new ClientOMSException("Message should not be null");
        }
        DefaultKeyValue headers = (DefaultKeyValue) message.headers();
        String topic = headers.removeString(MessageHeader.TOPIC);
        String queue = headers.removeString(MessageHeader.QUEUE);
        if ((topic == null && queue == null) || (topic != null && queue != null)) {
            throw new ClientOMSException(String.format("Queue:%s Topic:%s should put one and only one", true, queue));
        }
        String bucket = topic == null ? queue : topic;
        bucketManager.putMessage(bucket, message, localBuffer);
    }

    @Override
    public BytesMessage createBytesMessageToTopic(String topic, byte[] body) {
        return messageFactory.createBytesMessageToTopic(topic, body);
    }

    @Override
    public BytesMessage createBytesMessageToQueue(String queue, byte[] body) {
        return messageFactory.createBytesMessageToQueue(queue, body);
    }
}
