package hack;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class SProducer {
    public static void main(String[] args) throws Exception {
        insertRecords();
    }

    private static void insertRecords() throws Exception {
        String topic = "t1";
        Producer<String, String> producer = new KafkaProducer<>(initProps());
        for (int i = 0; i < 100; i++) {
            Future<RecordMetadata> send = producer.send(new ProducerRecord<String, String>(topic, null, "hello " + i));
            send.get(10000, TimeUnit.MILLISECONDS);
        }
        System.out.println("Successfully sent");
        producer.flush();
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


    /**
     * //import util.properties packages
     * import java.util.Properties;
     *
     * //import simple producer packages
     * import org.apache.kafka.clients.producer.Producer;
     *
     * //import KafkaProducer packages
     * import org.apache.kafka.clients.producer.KafkaProducer;
     *
     * //import ProducerRecord packages
     * import org.apache.kafka.clients.producer.ProducerRecord;
     *
     * //Create java class named “SimpleProducer”
     * public class SimpleProducer {
     *
     *    public static void main(String[] args) throws Exception{
     *
     *       // Check arguments length value
     *       if(args.length == 0){
     *          System.out.println("Enter topic name");
     *          return;
     *       }
     *
     *       //Assign topicName to string variable
     *       String topicName = args[0].toString();
     *
     *       // create instance for properties to access producer configs
     *       Properties props = new Properties();
     *
     *       //Assign localhost id
     *       props.put("bootstrap.servers", “localhost:9092");
     *
     *       //Set acknowledgements for producer requests.
     *       props.put("acks", “all");
     *
     *       //If the request fails, the producer can automatically retry,
     *       props.put("retries", 0);
     *
     *       //Specify buffer size in config
     *       props.put("batch.size", 16384);
     *
     *       //Reduce the no of requests less than 0
     *       props.put("linger.ms", 1);
     *
     *       //The buffer.memory controls the total amount of memory available to the producer for buffering.
     *       props.put("buffer.memory", 33554432);
     *
     *       props.put("key.serializer",
     *          "org.apache.kafka.common.serializa-tion.StringSerializer");
     *
     *       props.put("value.serializer",
     *          "org.apache.kafka.common.serializa-tion.StringSerializer");
     *
     *       Producer<String, String> producer = new KafkaProducer
     *          <String, String>(props);
     *
     *       for(int i = 0; i < 10; i++)
     *          producer.send(new ProducerRecord<String, String>(topicName,
     *             Integer.toString(i), Integer.toString(i)));
     *                System.out.println(“Message sent successfully”);
     *                producer.close();
     *    }
     * }
     */
}
