package hum.open_message;

/**
 * @author hum
 */
public interface Producer extends MessageFactory {
    void flush();

    void send(Message message);
}

