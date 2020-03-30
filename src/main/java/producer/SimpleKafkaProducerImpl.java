package producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class SimpleKafkaProducerImpl implements SimpleKafkaProducer {
    private KafkaProducer<String, String> kafkaProducer;
    public SimpleKafkaProducerImpl(String brokerList) {
        Properties config = new Properties();
        config.put("client.id", "producer");
        config.put("bootstrap.servers", brokerList);
        config.put("key.serializer", StringSerializer.class.getName());
        config.put("value.serializer", StringSerializer.class.getName());
        config.put("acks", "all");
        this.kafkaProducer = new KafkaProducer<String, String>(config);
    }

    public void send(String topic, String key, String value) throws ExecutionException, InterruptedException {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
        Future<RecordMetadata> future = kafkaProducer.send(record);
        future.get();
    }
}
