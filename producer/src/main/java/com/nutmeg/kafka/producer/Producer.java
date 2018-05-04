package com.nutmeg.kafka.producer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Clock;
import java.util.Random;

public class Producer {

    private final static String TOPIC_NAME = "gc";
    private final static String SCHEMA_REGISTRATION_URL = "http://localhost:8081";
    private final static String BOOTSTRAP_SERVERS = "http://localhost:9092";
    private final Clock clock;
    private final String[] users = {"user1", "user2", "user3", "user4", "user5"};

    public Producer() {
        this.clock = Clock.systemDefaultZone();
    }

    private void sendMessages(long numEvents) {

        org.apache.kafka.clients.producer.Producer producer =
                new KafkaProducer<>(ProducerProperties.createProducer(BOOTSTRAP_SERVERS, SCHEMA_REGISTRATION_URL));
        Schema schema = ProducerSchema.createSchema();

        Random rnd = new Random();
        for (long nEvents = 0; nEvents < numEvents; nEvents++) {

            String user = users[rnd.nextInt(5)];
            GenericRecord page_visit = new GenericData.Record(schema);
            page_visit.put("time", clock.millis());
            page_visit.put("action", "add/remove/whatever");
            page_visit.put("email", user + "@nutmeg.com");
            page_visit.put("data", "data: " + rnd.nextInt(500));

            ProducerRecord<String, GenericRecord> data = new ProducerRecord<>(
                    TOPIC_NAME, user, page_visit);
            producer.send(data);
        }
        producer.close();
    }

    public static void main(String[] args) {

        Producer producerExample = new Producer();
        producerExample.sendMessages(50);

    }

}