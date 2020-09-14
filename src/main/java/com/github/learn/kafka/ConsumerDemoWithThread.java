package com.github.learn.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

@Slf4j
public class ConsumerDemoWithThread {
    public static void main(String[] args) {
        new ConsumerDemoWithThread().run();
    }

    public void run() {
        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "my-sixth-application";
        String topic = "first_topic";

        // latch for dealing tih multiple threads
        CountDownLatch latch = new CountDownLatch(1);

        //create the consumer runnable
        log.info("Creating the consumer thread");
        Runnable myConsumerRunnable = new ConsumerThread(bootstrapServers, groupId, topic, latch);

        //start the thread
        Thread myThread = new Thread(myConsumerRunnable);
        myThread.start();

        //add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Caught shutdown hook");
            ((ConsumerThread) myConsumerRunnable).shutdown();
        }));

        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public class ConsumerThread implements Runnable {

        private CountDownLatch latch;
        KafkaConsumer<String, String> consumer;

        public ConsumerThread(String bootstrapServers, String groupId, String topic, CountDownLatch latch) {
            this.latch = latch;

            //create consumer properties
            Properties properties= new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            //create consumer
            consumer = new KafkaConsumer<>(properties);

            //subscribe consumer to our topic
            consumer.subscribe(Collections.singletonList(topic));
        }

        @Override
        public void run() {
            //poll for new data
            try {
                while(true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record: records) {
                        log.info("Key: {}, Value: {}", record.key(), record.value());
                        log.info("Partition: {}, Offset: {}", record.partition(), record.offset());
                    }
                }
            } catch (WakeupException e) {
                log.error("Received shutdown signal.");
            } finally {
                consumer.close();
                //tell our main code we're done with the consumer
                latch.countDown();
            }
        }

        public void shutdown() {
            //the wakeup() method is a special method to interrupt the consumer.poll()
            //it will throw the exception WakeUpException
            consumer.wakeup();
        }
    }
}
