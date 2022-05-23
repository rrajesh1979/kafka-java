package org.rrajesh1979.kafka.demo;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

@Slf4j
public class ConsumerDemo {
    static final String BOOTSTRAP_SERVERS = "localhost:9092";
    static final String TOPIC = "java_topic";
    static final String GROUP_ID = "java-app";

    public static void main(String[] args) {
        log.info("Starting the consumer demo");

        //Create Consumer Properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Cooperative mode
        properties.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName());

        //Create Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // Get reference to current thread
        final Thread mainThread = Thread.currentThread();

        // Add shutdown hook to close consumer
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutting down consumer");
            consumer.wakeup();

            // Wait for consumer to shutdown
            try {
                mainThread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }));

        try {
            //Subscribe to topic
            consumer.subscribe(Arrays.asList(TOPIC));

            //Read messages
            while (true) {
                log.info("Waiting for messages...");

                //Read records
                ConsumerRecords<String, String> messages =
                        consumer.poll(Duration.ofMillis(1000));

                //Iterate through the records
                for (ConsumerRecord<String, String> message : messages) {
                    log.info("Topic : " + message.topic() +
                            "\nKey : " + message.key() +
                            "\nValue : " + message.value() +
                            "\nPartition : " + message.partition() +
                            "\nOffset : " + message.offset() +
                            "\nTimestamp : " + message.timestamp()
                    );
                }

                //Commit the offset
//                consumer.commitAsync();
            }
        } catch (WakeupException e) {
            log.info("Wake up exception! Expected!");
        } catch (Exception e) {
            log.error("Exception while consuming", e);
        } finally {
            consumer.close();
            log.info("Consumer closed gracefully");
        }

        //Unsubscribe from topic

        //Close Consumer

    }
}
