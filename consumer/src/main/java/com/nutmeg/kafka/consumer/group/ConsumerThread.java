package com.nutmeg.kafka.consumer.group;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class ConsumerThread implements Runnable {

    private final KafkaConsumer<String, GenericRecord> kafkaConsumer;

    private int threadId;
    private final String INDEX = "index";
    private Map<Integer, Long> partitionIndices;

    public ConsumerThread(Properties properties, String topic, int threadId) {

        this.threadId = threadId;
        kafkaConsumer = new KafkaConsumer<>(properties);
        kafkaConsumer.subscribe(Collections.singletonList(topic));
        partitionIndices = new HashMap<>();
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

                    long messageIndex = (long)(record.value().get(INDEX));
                    checkOrder(record.partition(), messageIndex);
                    partitionIndices.put(record.partition(), messageIndex);
                    System.out.println("Thread: " + threadId +
                            ", Partition: " + record.partition() +
                            " Received:- Key: " + record.key() +
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

    private void checkOrder(int partition, long messageIndex) {

        if (partitionIndices.getOrDefault(partition, 0L) != 0 &&
                partitionIndices.get(partition) > messageIndex) {
            throw new RuntimeException(
                    String.format("message out of order current index: %s, last index: %s partition: %s",
                            messageIndex, partitionIndices.get(partition), partition));
        }
    }
}
