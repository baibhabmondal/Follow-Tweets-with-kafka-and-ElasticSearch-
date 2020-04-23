package app.twitter.producer;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

    String consumerKey = System.getenv("TWITTER_CONSUMER_KEY");
    String consumerSecret = System.getenv("TWITTER_CONSUMER_SECRET");
    String token = System.getenv("TWITTER_TOKEN");
    String tokenSecret = System.getenv("TWITTER_TOKEN_SECRET");
    Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());

    // Twitter terms to follow
    List<String> terms = Lists.newArrayList("bitcoin");

    BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);

    public TwitterProducer() {}

    public static void main(String[] args) {
        new TwitterProducer().run();
    }

    public void run() {

        logger.info("Setup");

        // set up twitter client
        Client client = createTwitterClient();

        // Attempts to establish a connection.
        client.connect();

        // set up producer
        KafkaProducer<String, String> producer = createKafkaProducer();

        //  shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutting down app.");
            client.stop();
            producer.close();
        }));

        // loop to send tweets to kafka
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }
            if(msg != null) {

                //  create record to send
                ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(
                        "twitter_tweets",
                        null, msg);

                // send data with callback
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (e != null) {
                            logger.error("Oops! ", e);
                        }
//                        logger.info("Topic:" + recordMetadata.topic() + ", Partition:" + recordMetadata.partition());
                    }
                });
                logger.info(msg);
            }
        }
        logger.info("End of App");
    }

    public Client createTwitterClient() {
        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        HttpHosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        // Optional: set up some followings and track terms

        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        logger.info(consumerKey);
        logger.info(consumerSecret);
        logger.info(token);
        logger.info(tokenSecret);
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, tokenSecret);

        ClientBuilder builder = new ClientBuilder()
                .name("Twitter-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        Client hosebirdClient = builder.build();
        return hosebirdClient;
    }

    public KafkaProducer<String, String> createKafkaProducer() {

        String bootstrapServer = "localhost:9092";

        // create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create safe producer
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));

        // create high throughput producer
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024)); // 32K


        // create producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        return  producer;
    }
}
