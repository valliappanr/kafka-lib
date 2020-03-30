package consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

public class BasicConsumeLoop<K, V> implements Runnable {
    public static final String DEFAULT_GROUP_ID = "test-k";
    public static final String OFFSET_STRATEGY = "latest";
    public static final String DEFAULT_OFFSET_STRATEGY = OFFSET_STRATEGY;
    private final KafkaConsumer<K, V> consumer;
    private final List<String> topics;
    private final AtomicBoolean shutdown;
    private final CountDownLatch shutdownLatch;
    private Consumer<ConsumerRecord<K, V>> callback;
    private ExecutorService executorService;

    public BasicConsumeLoop(String brokersList, List<String> topics, Consumer<ConsumerRecord<K, V>> callback) {
        this(brokersList, topics, DEFAULT_GROUP_ID, DEFAULT_OFFSET_STRATEGY, callback);
    }
    public BasicConsumeLoop(String brokersList, List<String> topics, String groupId,
                            String offsetStrategy, Consumer<ConsumerRecord<K, V>> callback) {
        Properties props = createPropertiesForKafka(brokersList, groupId, offsetStrategy);
        executorService = Executors.newFixedThreadPool(4);
        this.callback = callback;
        this.consumer = new KafkaConsumer<>(props);
        this.topics = topics;
        this.shutdown = new AtomicBoolean(false);
        this.shutdownLatch = new CountDownLatch(1);

    }

    private Properties createPropertiesForKafka(String brokersList, String groupId, String offsetStrategy) {
        Properties props = new Properties();
        props.put("bootstrap.servers", brokersList);
        props.put("group.id", groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetStrategy);
        props.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        return props;
    }

    public void run() {
        try {
            consumer.subscribe(topics);

            while (!shutdown.get()) {
                ConsumerRecords<K, V> records = consumer.poll(Duration.ofMillis(500));
                executorService.submit(() -> {
                    records.forEach(record -> callback.accept(record));
                });
            }
        } finally {
            consumer.close();
            shutdownLatch.countDown();
        }
    }

    public void shutdown() throws InterruptedException {
        shutdown.set(true);
        shutdownLatch.await();
    }
}