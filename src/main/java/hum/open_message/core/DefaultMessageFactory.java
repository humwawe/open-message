package hum.open_message.core;

import hum.open_message.BytesMessage;
import hum.open_message.MessageFactory;
import hum.open_message.MessageHeader;

/**
 * @author hum
 */
public class DefaultMessageFactory implements MessageFactory {

    @Override
    public BytesMessage createBytesMessageToTopic(String topic, byte[] body) {
        DefaultBytesMessage defaultBytesMessage = new DefaultBytesMessage(body);
        defaultBytesMessage.putHeaders(MessageHeader.TOPIC, topic);
        return defaultBytesMessage;
    }

    @Override
    public BytesMessage createBytesMessageToQueue(String queue, byte[] body) {
        DefaultBytesMessage defaultBytesMessage = new DefaultBytesMessage(body);
        defaultBytesMessage.putHeaders(MessageHeader.QUEUE, queue);
        return defaultBytesMessage;
    }
}
