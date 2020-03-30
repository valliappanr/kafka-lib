package consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.List;
import java.util.function.Consumer;

public class DefaultKafkaConsumerImpl {
    private BasicConsumeLoop<String, String> kConsumer;
    private Thread consumerThread;

    public DefaultKafkaConsumerImpl(String brokerList, List<String> topics,
                                    Consumer<ConsumerRecord<String, String>> callback) {
        kConsumer = new BasicConsumeLoop<>(
                brokerList, topics,
                callback);
        consumerThread = new Thread(kConsumer);
    }

    public DefaultKafkaConsumerImpl(String brokerList, List<String> topics, String groupId, String offsetStrategy,
                                    Consumer<ConsumerRecord<String, String>> callback) {
        kConsumer = new BasicConsumeLoop<>(
                brokerList, topics, groupId, offsetStrategy,
                callback);
        consumerThread = new Thread(kConsumer);
    }

    public void start() throws InterruptedException {
        consumerThread.start();
        consumerThread.join();
    }
    public void stop() throws InterruptedException {
        kConsumer.shutdown();
    }
}
