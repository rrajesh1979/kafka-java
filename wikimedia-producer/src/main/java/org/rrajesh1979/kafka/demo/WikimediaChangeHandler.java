package org.rrajesh1979.kafka.demo;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

@Slf4j
public class WikimediaChangeHandler implements EventHandler {

    KafkaProducer<String, String> kafkaProducer;
    String topic;

    public WikimediaChangeHandler(KafkaProducer<String, String> kafkaProducer, String topic){
        this.kafkaProducer = kafkaProducer;
        this.topic = topic;
    }

    @Override
    public void onOpen() {
        // nothing here
    }

    @Override
    public void onClosed() {
        kafkaProducer.close();
    }

    @Override
    public void onMessage(String event, MessageEvent messageEvent) {
        log.info("onMessage event : {}", event);
        log.info("onMessage messageEvent.getData : {}", messageEvent.getData());
        // asynchronous
//        kafkaProducer.send(new ProducerRecord<>(topic, messageEvent.getData()));

        kafkaProducer.send(new ProducerRecord<>(topic, messageEvent.getData()), (metadata, exception) -> {
            if (exception != null) {
                log.error("Exception while producing", exception);
            }
            log.info("Successfully produced record and received metadata : \n" +
                    "Topic : " + metadata.topic() + "\n" +
                    "Key : " + event + "\n" +
                    "Partition : " + metadata.partition() + "\n" +
                    "Offset : " + metadata.offset() + "\n" +
                    "Timestamp : " + metadata.timestamp()
            );
        });

    }

    @Override
    public void onComment(String comment) {
        // nothing here
    }

    @Override
    public void onError(Throwable t) {
        log.error("Error in Stream Reading", t);
    }
}