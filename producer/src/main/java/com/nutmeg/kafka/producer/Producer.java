package com.nutmeg.kafka.producer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Clock;
import java.util.Random;

public class Producer {

    private final static String TOPIC_NAME = "mytopic123";
    private final static String SCHEMA_REGISTRATION_URL = "http://localhost:8081";
    private final static String BOOTSTRAP_SERVERS = "http://localhost:9092";
    private final Clock clock;
    private final String[] users = {"user1", "user2", "user3", "user4", "user5", "user6", "user7", "user8", "user9", "user10"};

    public Producer() {
        this.clock = Clock.systemDefaultZone();
    }

    private void sendMessages(int numEvents) {

        org.apache.kafka.clients.producer.Producer messageProducer =
                new KafkaProducer<>(ProducerProperties.createProducer(BOOTSTRAP_SERVERS, SCHEMA_REGISTRATION_URL));
        Schema schema = ProducerSchema.createSchema();

        Random rnd = new Random();
        for (int events = 0; events < numEvents; events++) {

            String user = users[rnd.nextInt(8)];
            GenericRecord record = new GenericData.Record(schema);
            record.put("time", clock.millis());
            record.put("action", "whatever");
            record.put("email", user + "@nutmeg.com");
            record.put("index", (long)events);

            messageProducer.send(new ProducerRecord<>(
                    TOPIC_NAME, user, record));
        }
        messageProducer.close();
    }

    public static void main(String[] args) {
        System.out.println("start");
        long time = Clock.systemDefaultZone().millis();
        Producer producerExample = new Producer();
        producerExample.sendMessages(10000);

        System.out.println("finish: " + (Clock.systemUTC().millis() - time));

    }

}