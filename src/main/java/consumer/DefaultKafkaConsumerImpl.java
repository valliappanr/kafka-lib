package consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.List;
import java.util.function.Consumer;

public class DefaultKafkaConsumerImpl implements DefaultKafkaConsumer {
    private String brokerList;
    private BasicConsumeLoop<String, String> kConsumer;

    public DefaultKafkaConsumerImpl(String brokerList) {
        this.brokerList = brokerList;

    }

    @Override
    public void createAndStartConsumer(List<String> topics, String groupId, String offsetStrategy,
                                       Consumer<ConsumerRecord<String, String>> callback) throws InterruptedException {
        Thread consumerThread = createKConsumerThread(topics, groupId, offsetStrategy, callback);
        consumerThread.start();
    }

    @Override
    public void createAndStartConsumerAndJoinConsumer(List<String> topics, String groupId, String offsetStrategy,
                                                      Consumer<ConsumerRecord<String, String>> callback) throws InterruptedException {
        Thread consumerThread = createKConsumerThread(topics, groupId, offsetStrategy, callback);
        consumerThread.start();
        consumerThread.join();
    }

    private Thread createKConsumerThread(List<String> topics, String groupId, String offsetStrategy, Consumer<ConsumerRecord<String, String>> callback) {
        kConsumer = new BasicConsumeLoop<>(brokerList, topics, groupId, offsetStrategy,
                callback);
        addShutdownHook();
        return new Thread(kConsumer);
    }

    @Override
    public void stop() {
        try {
            kConsumer.shutdown();
        } catch(InterruptedException iex) {
            iex.printStackTrace();
        }

    }

    private void addShutdownHook(){
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                stop();
            }
        }));
    }
}
