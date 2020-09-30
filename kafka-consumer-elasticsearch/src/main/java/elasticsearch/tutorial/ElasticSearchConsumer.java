package elasticsearch.tutorial;

import com.google.gson.JsonParser;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;

@Slf4j
public class ElasticSearchConsumer {

    private static JsonParser jsonParser = new JsonParser();

    public static RestHighLevelClient createClient(){
        String hostname = "kafka-course-9567812382.eu-central-1.bonsaisearch.net";
        String username = "yqy5ngz4us";
        String password = "vtsduewec7";

        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));

        RestClientBuilder builder = RestClient
                .builder(new HttpHost(hostname, 443, "https"))
                .setHttpClientConfigCallback(httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider));

        RestHighLevelClient client = new RestHighLevelClient(builder);
        return client;
    }

    public static KafkaConsumer createConsumer(String topic) {
        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "kafka-demo-elasticsearch";

        //create consumer properties
        Properties properties= new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");  //disable auto commit of offsets
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");

        //create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singletonList(topic));

        return consumer;
    }
    public static void main(String[] args) throws IOException {

        RestHighLevelClient client = createClient();

        KafkaConsumer<String, String> kafkaConsumer = createConsumer("twitter_tweets");

        //poll for new data
        while(true) {

            ConsumerRecords<String, String> records =
                    kafkaConsumer.poll(Duration.ofMillis(100));

            Integer recordsCount = records.count();
            log.info("Received " + records.count() + " records");

            BulkRequest bulkRequest = new BulkRequest();
            for (ConsumerRecord<String, String> record: records) {
                //2 strategies
                //Kafka Generic ID
                //String id = record.topic() + "_" + record.partition() + "_" + record.offset();

                try {
                    //twitter feed specific id
                    String id = extractIdFromTwitter(record.value());

                    log.info("Key: {}, Value: {}", record.key(), record.value());
                    log.info("Partition: {}, Offset: {}", record.partition(), record.offset());

                    IndexRequest indexRequest = new IndexRequest("twitter", "tweets", id)
                            .source(record.value(), XContentType.JSON);

                    bulkRequest.add(indexRequest);

//                IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
//                log.info("Id: " + indexResponse.getId());
//                try {
//                    Thread.sleep(10);
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
                } catch (NullPointerException e) {
                    log.warn("skipping bad data: {}", record.value());
                }
            }

            if (recordsCount > 0) {
                BulkResponse bulkItemResponses = client.bulk(bulkRequest, RequestOptions.DEFAULT);

                log.info("Committing offsets...");
                kafkaConsumer.commitSync();
                log.info("Offsets have been commited");
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        //client.close();
    }

    private static String extractIdFromTwitter(String json) {
        //gson library
        return jsonParser.parse(json).getAsJsonObject().get("id_string").getAsString();
    }
}
