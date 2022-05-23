package org.rrajesh1979.kafka.demo;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

@Slf4j
public class ProducerDemoKeys {
    public static void main(String[] args) {
        log.info("Starting the producer demo");

        //Create Producer Properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        final String TOPIC = "java_topic";

        //Create Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        //Create Producer Record
        //Produce Record
        for (int i = 0; i < 100; i++) {
            String key = "key_" + i;
            String value = "Hello World! " + i;

            ProducerRecord<String, String> record =
                    new ProducerRecord<>(TOPIC, key, value);

            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    log.error("Exception while producing", exception);
                }
                log.info("Successfully produced record and received metadata : \n" +
                        "Topic : " + metadata.topic() + "\n" +
                        "Key : " + record.key() + "\n" +
                        "Partition : " + metadata.partition() + "\n" +
                        "Offset : " + metadata.offset() + "\n" +
                        "Timestamp : " + metadata.timestamp()
                );
            });

            // Sleep for 1 second
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        //Flush Producer
        producer.flush();

        //Close Producer
        producer.close();
    }
}
