package app.home;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class consumerWithThread {

    public static void main(String[] args) {

        new consumerWithThread().run();

    }
    private consumerWithThread() {
        // empty constructor
    }

    public void run() {
        String bootstrapServer = "localhost:9092";
        String topic = "first_topic";
        String groupId = "first_app";
        Logger logger = LoggerFactory.getLogger(consumerWithThread.class.getName());

        //  latch for multiple threads
        CountDownLatch latch = new CountDownLatch(1);

        //  create runnable instance
        consumerRunnable myConsumerRunnable = new consumerRunnable(bootstrapServer, topic, groupId, latch);

        // create thread
        Thread myConsumerThread = new Thread(myConsumerRunnable);
        myConsumerThread.start();

        //  add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread( () -> {
            logger.info("Caught shutdown hook");
            myConsumerRunnable.shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("Application has exited ");
        }));

        //  until interrupted
        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.info("Application interrupted", e);
        } finally {
            logger.info("Application is closing.");
        }
    }

    public class consumerRunnable implements Runnable {
        private CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;
        private Logger logger = LoggerFactory.getLogger(consumerRunnable.class.getName());

        public consumerRunnable(
                String bootstrapServer,
                String topic,
                String groupId,
                CountDownLatch latch
        ) {

            this.latch = latch;

            // create consumer properties
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);

            // create consumer
            consumer = new KafkaConsumer<String, String>(properties);

            // subscribe to topic
            consumer.subscribe(Arrays.asList(topic));

        }

        @Override
        public void run() {
            // poll
            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                    for (ConsumerRecord<String, String> record : records) {
                        logger.info("Topic: " + record.topic());
                        logger.info("Key: " + record.key() + ", Value: " + record.value());
                    }
                }
            } catch (WakeupException e) {
                logger.info("Received Shutdown signal");
            } finally {
                logger.info("consumer closing");
                consumer.close();
            }
        }

        public void shutdown() {
            consumer.wakeup();
            latch.countDown();
        }
    }
}
