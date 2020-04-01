import consumer.DefaultKafkaConsumerImpl;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import producer.SimpleKafkaProducer;
import producer.SimpleKafkaProducerImpl;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

import static org.junit.Assert.assertTrue;

public class KConsumerTest {

    public static final String TEST_TOPIC = "test-topic";
    public static final String BROKER_LIST_PROPERTY = "BROKER_LIST";
    public static final String TOPIC_GROUP_1 = "t1";
    public static final String TOPIC_GROUP_2 = "t2";
    public static final String EARLIEST_OFFSET_STRATEGY = "earliest";
    public static final String LATEST_OFFSET_STRATEGY = "latest";
    public static final String TOPIC_KEY = "topic_key";
    public static final String TEST_MESSAGE = "test-message";
    public static final int DURATION_IN_MILLIS = 1000;
    public static final int RETRY_LIMIT = 5;
    private AdminClient kafkaAdminClient;
    private Map<String, String> resultMap = new ConcurrentHashMap<>();
    private DefaultKafkaConsumerImpl defaultKafkaConsumer;
    private SimpleKafkaProducer simpleKafkaProducer;

    @Before
    public void setup() throws InterruptedException {
        String brokerList = System.getenv(BROKER_LIST_PROPERTY);
        defaultKafkaConsumer = new DefaultKafkaConsumerImpl(
                brokerList);
        simpleKafkaProducer = new SimpleKafkaProducerImpl(brokerList);
        Properties config = new Properties();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        kafkaAdminClient = KafkaAdminClient.create(config);
        kafkaAdminClient.createTopics(Collections.singletonList(new NewTopic(TEST_TOPIC, 1, (short)1)));
        Thread.sleep(DURATION_IN_MILLIS);
    }

    @After()
    public void cleanUp() {
        defaultKafkaConsumer.stop();
        simpleKafkaProducer.close();
        resultMap.clear();
        kafkaAdminClient.deleteTopics(Collections.singletonList(TEST_TOPIC));
    }

    public void consumeRecord(ConsumerRecord<String, String> record) {
        resultMap.putIfAbsent(record.topic(), record.key() + record.value());
    }

    @Test
    public void testKConsumer() throws InterruptedException, ExecutionException {
        createConsumerThread(defaultKafkaConsumer, TOPIC_GROUP_1, LATEST_OFFSET_STRATEGY);
        Thread.sleep(DURATION_IN_MILLIS);
        sendMessage(TEST_TOPIC, TOPIC_KEY, TEST_MESSAGE);
        pollAndCheckMessageArrival(DURATION_IN_MILLIS, RETRY_LIMIT, () -> resultMap.size() == 1);
    }



    @Test
    public void testKConsumerWithEarliestOffset() throws InterruptedException, ExecutionException {
        sendMessage(TEST_TOPIC, TOPIC_KEY, TEST_MESSAGE);
        createConsumerThread(defaultKafkaConsumer, TOPIC_GROUP_2, EARLIEST_OFFSET_STRATEGY);
        pollAndCheckMessageArrival(DURATION_IN_MILLIS, RETRY_LIMIT, () -> resultMap.size() == 1);
    }

    private void sendMessage(String topic, String key, String message) throws ExecutionException, InterruptedException {
            simpleKafkaProducer.send(topic, key, message);
    }

    private void createConsumerThread(DefaultKafkaConsumerImpl defaultKafkaConsumer, String groupId, String offsetStrategy) throws InterruptedException {
        defaultKafkaConsumer.createAndStartConsumer(Arrays.asList(TEST_TOPIC), groupId, offsetStrategy,
                record -> {
                        consumeRecord(record);
                }
        );
    }

    private void pollAndCheckMessageArrival(int interval, int noOfTries, Supplier<Boolean> predicate) throws InterruptedException {
        for (int i = 0; i< noOfTries; i++) {
            if (predicate.get()) {
                return;
            }
            Thread.sleep(interval);
        }
        throw new IllegalStateException("Condition failed");
    }
}