package mock.producer;

import producer.SimpleKafkaProducer;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class MockKafkaProducerImpl implements SimpleKafkaProducer {
    Map<String, KafkaMockData> kafkaMockDataMap = new HashMap<>();

    @Override
    public void send(String topic, String key, String value) throws ExecutionException, InterruptedException {
        kafkaMockDataMap.put(topic, new KafkaMockData(key, value));
    }

    @Override
    public void close() {
        kafkaMockDataMap.clear();
    }
}
