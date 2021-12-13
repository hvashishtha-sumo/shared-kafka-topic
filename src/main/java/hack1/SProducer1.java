package hack1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class SProducer1 {


    public static void main(String[] args) throws Exception {
        insertRecords();
    }

    private static void insertRecords() throws Exception {
        Producer<SRecord, String> producer = new KafkaProducer<>(initProps());
        int counter = 0;
        Map<String, AtomicInteger> recordsPerCustId = new HashMap<>();
        for (int i = 0; i < 10; i++) {
            final int id = i % 2;
            Future<RecordMetadata> send =
                    producer.send(new ProducerRecord<SRecord, String>(CommonConfiguration.topic, null, new SRecord(id, "Hello").toString()));
            recordsPerCustId.putIfAbsent("cid-" + id, new AtomicInteger(0));
            recordsPerCustId.get("cid-" + id).incrementAndGet();
            counter++;
            send.get(10000, TimeUnit.MILLISECONDS);
        }
        System.out.println("Successfully sent " + counter + " records, to topic " + CommonConfiguration.topic + " with id and count:" + recordsPerCustId);
        producer.flush();
        producer.close();
    }

    private static Properties initProps() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9091");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 1024);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 1024*1024);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return props;
    }



}
