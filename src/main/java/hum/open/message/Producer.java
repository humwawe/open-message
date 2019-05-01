package hum.open.message;

/**
 * @author hum
 */
public interface Producer extends MessageFactory {
    void flush();

    void send(Message message);
}

