package producer;

import java.util.concurrent.ExecutionException;

public interface SimpleKafkaProducer {
    void send(String topic, String key, String value) throws ExecutionException, InterruptedException;
    void close();
}
