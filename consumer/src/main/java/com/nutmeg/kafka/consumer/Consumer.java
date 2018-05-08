package com.nutmeg.kafka.consumer;

import com.nutmeg.kafka.consumer.group.ConsumerGroup;

public class Consumer {


    public static void main(String[] args) {

        String brokers = "http://localhost:9092";
        String schemaRegUrl = "http://localhost:8081";
        String groupId = "group01";
        String topic = "mytopic123";
        int numberOfConsumer = 6;
        String zookeeperUrl = "localhost:2181";

        new ConsumerGroup(brokers, groupId, topic, numberOfConsumer, schemaRegUrl, zookeeperUrl)
                .execute();

        try {
            Thread.sleep(10000);
        } catch (InterruptedException ie) {

        }
    }


}
