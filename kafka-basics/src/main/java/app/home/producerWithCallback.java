package app.home;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class producerWithCallback {

    public static void main(String[] args) {

        String bootstrapServer = "localhost:9092";
        final Logger logger = LoggerFactory.getLogger(producerWithCallback.class);
        // create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        // create producer record
        final ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>("first_topic", "Hello from producer");

        // send data - async
        producer.send(producerRecord, (recordMetadata, e) -> {
            if ( e == null) {
                logger.info("data sent \n" + "topic:" + recordMetadata.topic()
                 + "\nPartition:" + recordMetadata.partition()
                 + "\nOffset: " + recordMetadata.hasOffset());
            } else {
                logger.error("Error while consuming: ", e);
            }
        });

        // flush producer
//        producer.flush();

        // flush and close
        producer.close();
    }

}
