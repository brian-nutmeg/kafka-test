
package io.confluent.examples.producer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Clock;
import java.util.Properties;
import java.util.Random;

public class ProducerExample {

    private final static String TOPIC_NAME = "page_visits";
    private final static String SCHEMA_REGISTRATION_URL = "http://localhost:8081";
    private final static String BOOTSTRAP_SERVERS = "http://localhost:9092";
    private final Clock clock;
    private final String[] users = {"user1", "user2", "user3", "user4", "user5"};


    public ProducerExample() {
        this.clock = Clock.systemDefaultZone();
    }

    private Producer<String, GenericRecord> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroSerializer");
        props.put("schema.registry.url", SCHEMA_REGISTRATION_URL);
        return new KafkaProducer<>(props);
    }

    private Schema createSchema() {
        String schemaString = "{\"namespace\": \"example.avro\", \"type\": \"record\", " +
                "\"name\": \"page_visit\"," +
                "\"fields\": [" +
                "{\"name\": \"time\", \"type\": \"long\"}," +
                "{\"name\": \"site\", \"type\": \"string\"}," +
                "{\"name\": \"ip\", \"type\": \"string\"}" +
                "]}";

        Schema.Parser parser = new Schema.Parser();
        return parser.parse(schemaString);
    }


    private void sendMessages(long numEvents) {

        Producer<String, GenericRecord> producer = createProducer();
        Schema schema = createSchema();

        Random rnd = new Random();
        for (long nEvents = 0; nEvents < numEvents; nEvents++) {
//            long runtime = new Date().getTime();
            String site = "www.whatever.com";
            String ip = "1.1.1." + rnd.nextInt(255);

            GenericRecord page_visit = new GenericData.Record(schema);
            page_visit.put("time", clock.millis());
            page_visit.put("site", site);
            page_visit.put("ip", ip);

            ProducerRecord<String, GenericRecord> data = new ProducerRecord<>(
                    TOPIC_NAME, users[rnd.nextInt(5)], page_visit);
            producer.send(data);
        }

        producer.close();
    }


    public static void main(String[] args) {

        ProducerExample producerExample = new ProducerExample();
        producerExample.sendMessages(50);

    }



}