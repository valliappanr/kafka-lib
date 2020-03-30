import org.junit.Test;
import producer.SimpleKafkaProducerImpl;

import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;

public class SimpleKafkaProducerTest {
    @Test
    public void testSimpleKafkaProducerTest() throws ExecutionException, InterruptedException {
        SimpleKafkaProducerImpl simpleKafkaProducer = new SimpleKafkaProducerImpl("meenasoft.co.uk:9092");
        IntStream.range(0,10).forEach(index -> {
            try {
                simpleKafkaProducer.send("test-topic", "hello", "hello-world: " + index );
            } catch (ExecutionException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });
    }
}
