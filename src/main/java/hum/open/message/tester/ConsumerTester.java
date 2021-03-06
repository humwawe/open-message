package hum.open.message.tester;

import hum.open.message.BytesMessage;
import hum.open.message.KeyValue;
import hum.open.message.MessageHeader;
import hum.open.message.PullConsumer;
import hum.open.message.constant.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author hum
 */
public class ConsumerTester {
    static Logger logger = LoggerFactory.getLogger(ConsumerTester.class);

    static class ConsumerTask extends Thread {
        String queue;
        List<String> topics;
        PullConsumer consumer;
        Map<String, Map<String, Integer>> offsets = new HashMap<>();
        int pullNum;

        public ConsumerTask(String queue, List<String> topics) {
            this.queue = queue;
            this.topics = topics;
            init();
        }

        private void init() {
            try {
                Class kvClass = Class.forName("hum.open.message.core.DefaultKeyValue");
                KeyValue keyValue = (KeyValue) kvClass.getConstructor().newInstance();
                keyValue.put("STORE_PATH", Constants.STORE_PATH);
                Class consumerClass = Class.forName("hum.open.message.core.DefaultPullConsumer");
                consumer = (PullConsumer) consumerClass.getConstructor(new Class[]{KeyValue.class}).newInstance(new Object[]{keyValue});
                if (consumer == null) {
                    throw new InstantiationException("Init Producer Failed");
                }
                consumer.attachQueue(queue, topics);
            } catch (Exception e) {
                logger.error("please check the package name and class name:", e);
            }
            offsets.put(queue, new HashMap<>());
            for (String topic : topics) {
                offsets.put(topic, new HashMap<>());
            }
            for (Map<String, Integer> map : offsets.values()) {
                for (int i = 0; i < Constants.PRO_NUM; i++) {
                    map.put(Constants.PRO_PRE + i, 0);
                }
            }
        }

        @Override
        public void run() {
            while (true) {
                try {
                    BytesMessage message = (BytesMessage) consumer.poll();
                    if (message == null) {
                        break;
                    }

                    String queueOrTopic;
                    if (message.headers().getString(MessageHeader.QUEUE) != null) {
                        queueOrTopic = message.headers().getString(MessageHeader.QUEUE);
                    } else {
                        queueOrTopic = message.headers().getString(MessageHeader.TOPIC);
                    }
                    if (queueOrTopic == null || queueOrTopic.length() == 0) {
                        throw new Exception("Queue or Topic name is empty");
                    }
                    String body = new String(message.getBody());
                    int index = body.lastIndexOf("_");
                    if (index == -1) {
                        logger.error("Format error");
                        break;
                    }
                    String producer = body.substring(0, index);
                    final int expectedOffset = offsets.get(queueOrTopic).get(producer);
                    int offset = Integer.parseInt(body.substring(index + 1));
                    if (offset != expectedOffset) {
                        logger.error("Offset not equal expected:{} actual:{} producer:{} queueOrTopic:{}",
                                expectedOffset, offset, producer, queueOrTopic);
                        break;
                    } else {
                        offsets.get(queueOrTopic).put(producer, offset + 1);
                    }
                    boolean headerCheckOK = message.headers().getString("HEADER_KEY").equals("fifogtb7y5") &&
                            message.headers().getString("094").equals("xlbo0dx") &&
                            message.properties().getString("PRO_OFFSET").equals("PRODUCER4_920") &&
                            message.properties().getString("yyd").equals("j8jn2j0");
                    if (!headerCheckOK) {
                        logger.error("header check failed");
                        break;
                    }
                    pullNum++;

                } catch (Exception e) {
                    logger.error("Error occurred in the consuming process", e);
                    break;
                }
            }

            logger.info("Consuming Over  consumer={}", queue);
        }

        public int getPullNum() {
            return pullNum;
        }
    }

    public static void main(String[] args) throws Exception {

        Thread[] ts = new Thread[Constants.CON_NUM];
        for (int i = 0; i < ts.length; i++) {
            ts[i] = new ConsumerTask(Constants.QUEUE_PRE + i, Collections.singletonList(Constants.TOPIC_PRE + i));
        }
        long start = System.currentTimeMillis();
        for (int i = 0; i < ts.length; i++) {
            ts[i].start();
        }
        for (int i = 0; i < ts.length; i++) {
            ts[i].join();
        }
        int pullNum = 0;
        for (int i = 0; i < ts.length; i++) {
            pullNum += ((ConsumerTask) ts[i]).getPullNum();
        }
        long end = System.currentTimeMillis();
        logger.info("Consumer Finished, Cost {} ms, Num {}", end - start, pullNum);
    }
}
