package mock.consumer;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import consumer.DefaultKafkaConsumer;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import mock.producer.KafkaMockData;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class MockConsumerImpl implements DefaultKafkaConsumer {

    private Consumer<ConsumerRecord<String, String>> callback;
    private Multimap<String, KafkaMockData> kafkaMockDataMap = HashMultimap.create();
    private Map<KafkaMockDataKey, Integer> offsetMap = new HashMap<>();
    private final AtomicBoolean shutdown;
    private final CountDownLatch shutdownLatch;
    private ExecutorService executorService;

    public MockConsumerImpl() {
        this.shutdown = new AtomicBoolean(false);
        this.shutdownLatch = new CountDownLatch(1);
        this.executorService = Executors.newFixedThreadPool(4);
    }


    @Override
    public void createAndStartConsumer(List<String> topics, String groupId, String offsetStrategy,
                                       Consumer<ConsumerRecord<String, String>> callback) throws InterruptedException {
        Thread consumerThread = createConsumerThread(topics, groupId, offsetStrategy, callback);
        consumerThread.start();
    }

    private Thread createConsumerThread(List<String> topics, String groupId, String offsetStrategy, Consumer<ConsumerRecord<String, String>> callback) {
        synchronized (this) {
            topics.stream().forEach(topic -> {
                int offset = 0;
                if (offsetStrategy.equals("latest")) {
                    Collection<KafkaMockData> kafkaMockDataCollection = kafkaMockDataMap.get(topic);
                    if (!kafkaMockDataCollection.isEmpty()) {
                        List<KafkaMockData> kafkaMockDataList = new ArrayList<>(kafkaMockDataCollection);
                        if (!kafkaMockDataList.isEmpty()) {
                            offset = kafkaMockDataList.size();
                        }
                    }
                }
                KafkaMockDataKey kafkaMockDataKey = new KafkaMockDataKey(topic, groupId);
                offsetMap.put(kafkaMockDataKey, offset);
            });
        }
        return new Thread(() -> {
            while(!shutdown.get()) {
                List<ConsumerRecord> consumerRecords = checkAndExtractMessage(topics, groupId);
                executorService.submit(() -> {
                    consumerRecords.stream().forEach(consumerRecord -> {
                        callback.accept(consumerRecord);
                    });
                });
            }
        });
    }

    @Override
    public void createAndStartConsumerAndJoinConsumer(
            List<String> topics, String groupId, String offsetStrategy,
            Consumer<ConsumerRecord<String, String>> callback) throws InterruptedException {
        Thread consumerThread = createConsumerThread(topics, groupId, offsetStrategy, callback);
        consumerThread.start();
        //consumerThread.join();
    }

    private List<ConsumerRecord> checkAndExtractMessage(List<String> topics, String groupId) {
        synchronized(this) {
            return topics.stream().map(topic -> {
                List<KafkaMockData> kafkaMockDataList = new ArrayList<>(kafkaMockDataMap.get(topic));
                KafkaMockDataKey kafkaMockDataKey = new KafkaMockDataKey(topic, groupId);
                int currentOffset = 0;
                if (offsetMap.get(kafkaMockDataKey) != null) {
                    currentOffset = offsetMap.get(kafkaMockDataKey);
                }
                offsetMap.put(kafkaMockDataKey, kafkaMockDataList.size());
                return IntStream.range(currentOffset, kafkaMockDataList.size()).mapToObj(index -> {
                    KafkaMockData kafkaMockData = kafkaMockDataList.get(index);
                    return new ConsumerRecord(topic, 0, index, kafkaMockData.getKey(), kafkaMockData.getValue());
                }).collect(Collectors.toList());
            }).flatMap(x -> x.stream()).collect(Collectors.toList());
        }
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    private class KafkaMockDataKey {
        private String topic;
        private String groupId;
    }

    @Override
    public void stop() throws InterruptedException {
        shutdown.set(Boolean.TRUE);
        shutdownLatch.await();
    }
    public void sendMessage(String topic, String key, String value) {
        synchronized (this) {
            kafkaMockDataMap.put(topic, new KafkaMockData(key, value));
        }

    }
}
