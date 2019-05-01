package hum.open.message.tester;

import hum.open.message.KeyValue;
import hum.open.message.Message;
import hum.open.message.Producer;
import hum.open.message.constant.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * @author hum
 */
public class ProducerTester {
    static Logger logger = LoggerFactory.getLogger(ProducerTester.class);

    private static final int NUM_TOPIC_QUEUE = 10;

    static class ProducerTask extends Thread {
        String label = null;
        Random random = new Random();
        Producer producer = null;
        int sendNum = 0;
        Map<String, Integer> offsets = new HashMap<>();

        public ProducerTask(String label) {
            this.label = label;
            init();
        }

        public void init() {
            try {
                Class kvClass = Class.forName("hum.open.message.core.DefaultKeyValue");
                KeyValue keyValue = (KeyValue) kvClass.getConstructor().newInstance();
                keyValue.put("STORE_PATH", Constants.STORE_PATH);
                Class producerClass = Class.forName("hum.open.message.core.DefaultProducer");
                producer = (Producer) producerClass.getConstructor(new Class[]{KeyValue.class}).newInstance(new Object[]{keyValue});
                if (producer == null) {
                    throw new InstantiationException("init producer failed");
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

            for (int i = 0; i < NUM_TOPIC_QUEUE; i++) {
                offsets.put("TOPIC_" + i, 0);
                offsets.put("QUEUE_" + i, 0);
            }
        }

        @Override
        public void run() {
            while (true) {
                String queueOrTopic;
                if (sendNum % 10 == 0) {
                    queueOrTopic = "QUEUE_" + random.nextInt(NUM_TOPIC_QUEUE);
                } else {
                    queueOrTopic = "TOPIC_" + random.nextInt(NUM_TOPIC_QUEUE);
                }
                Message message = producer.createBytesMessageToQueue(queueOrTopic, (label + "_" + offsets.get(queueOrTopic)).getBytes());
                message.putHeaders("HEADER_KEY", "fifogtb7y5");
                message.putHeaders("094", "xlbo0dx");
                message.putProperties("PRO_OFFSET", "PRODUCER4_920");
                message.putProperties("yyd", "j8jn2j0");
                logger.debug("queueOrTopic:{} offset:{}", queueOrTopic, label + "_" + offsets.get(queueOrTopic));
                offsets.put(queueOrTopic, offsets.get(queueOrTopic) + 1);
                producer.send(message);
                sendNum++;
                if (sendNum >= Constants.PRO_MAX) {
                    break;
                }
            }
            producer.flush();
        }
    }

    public static void main(String[] args) throws Exception {

        Thread[] ts = new Thread[Constants.PRO_NUM];
        for (int i = 0; i < ts.length; i++) {
            ts[i] = new ProducerTask(Constants.PRO_PRE + i);
        }
        long start = System.currentTimeMillis();
        for (int i = 0; i < ts.length; i++) {
            ts[i].start();
        }
        for (int i = 0; i < ts.length; i++) {
            ts[i].join();
        }
        long end = System.currentTimeMillis();
        logger.info("Produce Finished, Cost {} ms", end - start);
    }

}
