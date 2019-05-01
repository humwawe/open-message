package hum.open.message;

/**
 * @author hum
 */
public interface MessageFactory {

    BytesMessage createBytesMessageToTopic(String topic, byte[] body);
    BytesMessage createBytesMessageToQueue(String queue, byte[] body);
}
