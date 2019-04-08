package hum.open_message.producer;

import hum.open_message.Message;

import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.HashMap;

/**
 * @author hum
 */
public class BucketManager {
    private volatile static BucketManager instance;
    private final BufferService bufferService;
    private final HashMap<String, BucketWriter> bucketMap = new HashMap<>();

    public BucketManager(String storePath) {
        bufferService = new BufferService(storePath);

        // In case the storePath does not exist
        Paths.get(storePath).toFile().mkdirs();
    }

    public static BucketManager getInstance(String storePath) {
        if (instance == null) {
            synchronized (BucketManager.class) {
                if (instance == null) {
                    instance = new BucketManager(storePath);
                }
            }
        }
        return instance;
    }

    public void putMessage(String bucket, Message message, ByteBuffer localBuffer) {
        BucketWriter store = bucketMap.get(bucket);
        if (store == null) {
            synchronized (bucketMap) {
                if (!bucketMap.containsKey(bucket)) {
                    bucketMap.put(bucket, new BucketWriter(bucket, bufferService));
                }
            }
            store = bucketMap.get(bucket);
        }

        store.putMessage(message, localBuffer);
    }
}
