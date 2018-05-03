package io.confluent.examples.consumer;

import java.util.ArrayList;
import java.util.List;

public class NmConsumerGroup {

    private List<NmConsumerThread> consumers;

    public NmConsumerGroup(String brokers, String groupId, String topic,
                           int numberOfConsumers, String url, String zookeeperUrl) {

        consumers = new ArrayList<>();
        for (int i = 0; i < numberOfConsumers; i++) {
            NmConsumerThread ncThread =
                    new NmConsumerThread(brokers, groupId, topic, url, zookeeperUrl, i);
            consumers.add(ncThread);
        }
    }

    public void execute() {
        for (NmConsumerThread ncThread : consumers) {
            Thread t = new Thread(ncThread);
            t.start();
        }
    }

}
