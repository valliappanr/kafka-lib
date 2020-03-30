import consumer.DefaultKafkaConsumerImpl;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Test;

import java.util.Arrays;

public class KConsumerTest {
    public void consumeRecord(ConsumerRecord<String, String> record) {
        System.out.println(String.format("topic = %s, partition = %d, offset = %d," +
                        "customer = %s, country = %s\n", record.topic(), record.partition(), record.offset(),
                record.key(), record.value()));
    }
    @Test
    public void testKConsumer() throws InterruptedException {
        DefaultKafkaConsumerImpl defaultKafkaConsumer = new DefaultKafkaConsumerImpl(
                "meenasoft.co.uk:9092", Arrays.asList("test-topic"), "test1", "latest",
                record -> consumeRecord(record)
        );
        defaultKafkaConsumer.start();
    }

    @Test
    public void testKConsumerWithEarliestOffset() throws InterruptedException {
        DefaultKafkaConsumerImpl defaultKafkaConsumer = new DefaultKafkaConsumerImpl(
                "meenasoft.co.uk:9092", Arrays.asList("test-topic"), "test2", "earliest",
                record -> consumeRecord(record)
        );
        defaultKafkaConsumer.start();
    }

}