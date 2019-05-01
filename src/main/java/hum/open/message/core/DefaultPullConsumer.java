package hum.open.message.core;

import hum.open.message.consumer.MessageReader;
import hum.open.message.serializer.MessageSerializer;
import hum.open.message.KeyValue;
import hum.open.message.Message;
import hum.open.message.PullConsumer;
import hum.open.message.consumer.BufferService;
import hum.open.message.consumer.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;

/**
 * @author hum
 */
public class DefaultPullConsumer implements PullConsumer {
    private static final Logger logger = LoggerFactory.getLogger(DefaultPullConsumer.class);
    private ArrayList<MessageReader> readers = new ArrayList<>();
    private BufferService bufferService;
    private KeyValue properties;

    private int pollIndex = 0;
    private int count = 0;

    // thread local
    private MessageSerializer deserializer = new MessageSerializer();

    public DefaultPullConsumer(KeyValue properties) {
        this.properties = properties;
        this.bufferService = BufferService.getInstance(properties.getString("STORE_PATH"));
    }

    @Override
    public  Message poll() {
        MessageReader reader = readers.get(pollIndex);
        Message message = reader.readMessage();
        while (message == null) {
            readers.remove(pollIndex);
            if (readers.isEmpty()) {
                return null;
            }
            pollIndex = pollIndex % readers.size();
            reader = readers.get(pollIndex);
            message = reader.readMessage();
        }
        // change buffer every 64 messages
        if ((++count & 0x3f) == 0) {
            pollIndex = (pollIndex + 1) % readers.size();

            if (Constants.ENABLE_MESSAGE_SAMPLING) {
                // FOR DEBUG: Sample and print message every 2^17 (~131K) messages
                if ((count & 0x1ffff) == 0) {
                    logger.info("Sampled message: {}", message.toString());
                }
            }
        }
        return message;
    }

    @Override
    public synchronized void attachQueue(String queueName, Collection<String> topics) {
        readers.add(new MessageReader(queueName, true, bufferService, deserializer));
        for (String topic : topics) {
            readers.add(new MessageReader(topic, false, bufferService, deserializer));
        }

    }
}
