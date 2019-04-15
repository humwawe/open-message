package hum.open_message;

import java.util.Collection;

/**
 * @author hum
 */
public interface PullConsumer {
    Message poll();

    void attachQueue(String queueName, Collection<String> topics);
}
