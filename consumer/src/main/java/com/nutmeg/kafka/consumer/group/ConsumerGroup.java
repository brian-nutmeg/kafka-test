package com.nutmeg.kafka.consumer.group;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class ConsumerGroup {

    private List<ConsumerThread> consumers;

    public ConsumerGroup(String brokers, String groupId, String topic,
                         int numberOfConsumers, String url, String zookeeperUrl) {

        Properties consumerProps = ConsumerProperties.createProperties(brokers, groupId, url, zookeeperUrl);
        consumers = new ArrayList<>();
        for (int i = 0; i < numberOfConsumers; i++) {
            ConsumerThread ncThread =
                    new ConsumerThread(consumerProps, topic, i);
            consumers.add(ncThread);
        }
    }

    public void execute() {
        for (ConsumerThread ncThread : consumers) {
            Thread t = new Thread(ncThread);
            t.start();
        }
    }

}
