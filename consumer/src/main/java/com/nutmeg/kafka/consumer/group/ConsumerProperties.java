package com.nutmeg.kafka.consumer.group;

import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

public class ConsumerProperties {

    public static Properties createProperties(String brokers, String groupId, String url, String zookeeperUrl) {

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

}
