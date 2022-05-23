package org.rrajesh1979.kafka.demo;

import com.google.gson.JsonParser;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.InsertOneModel;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.bson.Document;
import org.bson.json.JsonObject;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

@Slf4j
public class WikimediaMongoDBConsumer {
    static final String MONGODB_URL = "mongodb://localhost:27017";
    static final String MONGODB_DATABASE = "wikimedia";
    static final String MONGODB_COLLECTION = "data";

    static final String BOOTSTRAP_SERVERS = "localhost:9092";
    static final String TOPIC = "wikimedia";

    private static KafkaConsumer<String, String> createKafkaConsumer(){

        String groupId = "consumer-mongodb";

        // create consumer configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        // create consumer
        return new KafkaConsumer<>(properties);

    }

    private static String extractId(String json){
        // gson library
        return JsonParser.parseString(json)
                .getAsJsonObject()
                .get("meta")
                .getAsJsonObject()
                .get("id")
                .getAsString();
    }

    public static void main(String[] args) {
        log.info("Starting MongoDB consumer");

        // Create MongoDB Client
        MongoClient mongoClient = MongoClients.create(MONGODB_URL);
        MongoDatabase database = mongoClient.getDatabase(MONGODB_DATABASE);
        MongoCollection<Document> collection = database.getCollection(MONGODB_COLLECTION);

        // create consumer
        KafkaConsumer<String, String> consumer = createKafkaConsumer();

        // subscribe consumer to topic
        consumer.subscribe(Arrays.asList(TOPIC));

        // poll for new data
        while (true) {
            // poll for new data
            ConsumerRecords<String, String> messages = consumer.poll(Duration.ofMillis(1000));

            int messageCount = messages.count();
            log.info("Received {} record(s)", messageCount);

            List<InsertOneModel<Document>> documents = new ArrayList<>();
            for (ConsumerRecord<String, String> message : messages) {
                String id = extractId(message.value());

                // create document
                Document document = new Document("_id", id)
                        .append("data", new JsonObject(message.value()));

                // add document to list
                documents.add(new InsertOneModel<>(document));
            }

            // bulk insert
            if (!documents.isEmpty()) {
                BulkWriteResult result = collection.bulkWrite(documents);
                log.info("Inserted {} documents", result.getInsertedCount());

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                // commit offsets after the batch is consumed
                consumer.commitSync();
                log.info("Offsets have been committed!");
            }

        }

    }
}
