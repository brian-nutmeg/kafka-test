package io.confluent.examples.consumer;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Properties;

public class NmConsumerThread implements Runnable {

    private final KafkaConsumer<String, GenericRecord> kafkaConsumer;

    private int threadId;

    public NmConsumerThread(String brokers, String groupId, String topic, String url, String zookeeperUrl, int threadId) {

        kafkaConsumer = new KafkaConsumer<>(createProperties(brokers, groupId, url, zookeeperUrl));
        this.threadId = threadId;
        this.kafkaConsumer.subscribe(Collections.singletonList(topic));
    }

    private Properties createProperties(String brokers, String groupId, String url, String zookeeperUrl) {
        Properties props = new Properties();
        props.put("bootstrap.servers", brokers);
        props.put("zookeeper.connect", zookeeperUrl);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put("schema.registry.url", url);

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroDeserializer");
        return props;
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

                    System.out.println("ThreadId: " + threadId + " Received message:- Key: " + record.key() +
                            " Value: " + record.value() +
                            ", Partition: " + record.partition() + ", Offset: " + record.offset() +
                            ", by ThreadID: " + Thread.currentThread().getId());
                }
                kafkaConsumer.commitAsync();
            }
        } finally {
            kafkaConsumer.close();
        }
    }
}
