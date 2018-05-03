package com.nutmeg.kafka.consumer.group;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Properties;

public class ConsumerThread implements Runnable {

    private final KafkaConsumer<String, GenericRecord> kafkaConsumer;

    private int threadId;

    public ConsumerThread(Properties properties, String topic, int threadId) {

        this.threadId = threadId;
        kafkaConsumer = new KafkaConsumer<>(properties);
        kafkaConsumer.subscribe(Collections.singletonList(topic));
    }

    @Override
    public void run() {
        try {
            while (true) {
                ConsumerRecords<String, GenericRecord> records = kafkaConsumer.poll(100);
                if (!records.isEmpty()) {
                    System.out.println(String.format("ThreadId: %s, GOT %s records",
                            threadId, records.count()));
                }
                for (ConsumerRecord<String, GenericRecord> record : records) {

                    System.out.println("ThreadId: " + threadId +
                            ", Partition: " + record.partition() +
                            " Received message:- Key: " + record.key() +
                            " Value: " + record.value() +
                            ", Offset: " + record.offset() +
                            ", by ThreadID: " + Thread.currentThread().getId());
                }
                kafkaConsumer.commitAsync();
            }
        } finally {
            kafkaConsumer.close();
        }
    }
}
