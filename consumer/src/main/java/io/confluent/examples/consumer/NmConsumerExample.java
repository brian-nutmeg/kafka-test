package io.confluent.examples.consumer;

public class NmConsumerExample {


    public static void main(String[] args) {

        String brokers = "http://localhost:9092";
        String groupId = "group01";
        String topic = "gc";
        int numberOfConsumer = 3;
        String zookeeperUrl = "localhost:2181";

        NmConsumerGroup nmConsumerGroup =
                new NmConsumerGroup(brokers, groupId, topic, numberOfConsumer, "http://localhost:8081", zookeeperUrl);
        nmConsumerGroup.execute();

        try {
            Thread.sleep(100000);
        } catch (InterruptedException ie) {

        }
    }


}
