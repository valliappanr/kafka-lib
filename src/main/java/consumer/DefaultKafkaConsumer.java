package consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.List;
import java.util.function.Consumer;

public interface DefaultKafkaConsumer {
    void createAndStartConsumer(List<String> topics, String groupId, String offsetStrategy,
                                Consumer<ConsumerRecord<String, String>> callback) throws InterruptedException;

    void createAndStartConsumerAndJoinConsumer(List<String> topics, String groupId, String offsetStrategy,
                                               Consumer<ConsumerRecord<String, String>> callback) throws InterruptedException;

    void stop() throws InterruptedException;
}
