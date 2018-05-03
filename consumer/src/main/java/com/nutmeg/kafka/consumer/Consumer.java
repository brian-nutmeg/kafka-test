package com.nutmeg.kafka.consumer;

import com.nutmeg.kafka.consumer.group.ConsumerGroup;

public class Consumer {


    public static void main(String[] args) {

        String brokers = "http://localhost:9092";
        String schemaRegUrl = "http://localhost:8081";
        String groupId = "group01";
        String topic = "gc";
        int numberOfConsumer = 3;
        String zookeeperUrl = "localhost:2181";

        new ConsumerGroup(brokers, groupId, topic, numberOfConsumer, schemaRegUrl, zookeeperUrl)
                .execute();

        try {
            Thread.sleep(100000);
        } catch (InterruptedException ie) {

        }
    }


}
