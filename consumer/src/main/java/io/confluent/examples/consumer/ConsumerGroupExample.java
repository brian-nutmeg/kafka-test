/**
 * Copyright 2015 Confluent Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.confluent.examples.consumer;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

//import kafka.consumer.ConsumerConfig;

public class ConsumerGroupExample {
//    private final ConsumerConnector consumer;
    private final String topic;
    private ExecutorService executor;
    private final String ZOOKEEPER_URL = "localhost:2181";
    private String groupId;
    private String url;
    private KafkaConsumer<String, GenericRecord> kafkaConsumer;

    public ConsumerGroupExample(String groupId, String topic, String url) {
//        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(
//                new ConsumerConfig(createConsumerConfig(groupId, url)));
        this.topic = topic;
//        this.zookeeper = zookeeper;
        this.groupId = groupId;
        this.url = url;
    }


    private Properties createProperties(String groupId, String url) {
        Properties props = new Properties();
        props.put("zookeeper.connect", ZOOKEEPER_URL);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "smallest");
        props.put("schema.registry.url", url);

        return props;
    }

//    private KafkaConsumer<String, GenericRecord> createConsumerConfig(String groupId, String url) {
//        Properties props = new Properties();
//        props.put("zookeeper.connect", ZOOKEEPER_URL);
//        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
//        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
//        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "smallest");
//        props.put("schema.registry.url", url);
//
////        return props;
//
//
//
////        Properties props = new Properties();
////        props.put("bootstrap.servers", "localhost:9092");
////        props.put("group.id", "test");
////        props.put("enable.auto.commit", "true");
////        props.put("auto.commit.interval.ms", "1000");
////        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
////        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//
////        KafkaConsumer<String, GenericRecord> consumer = new KafkaConsumer<>(props);
////        consumer.subscribe(Arrays.asList("foo", "bar"));
////        while (true) {
////            ConsumerRecords<String, String> records = consumer.poll(100);
////            for (ConsumerRecord<String, String> record : records)
////                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
////        }
//
//        return new KafkaConsumer<>(props);
//
//    }









    public void run() {
        while (true) {
            ConsumerRecords<String, GenericRecord> records = kafkaConsumer.poll(100);
            for (ConsumerRecord<String, GenericRecord> record : records) {
                System.out.println("Receive message:- Key: " + record.key() + " Value: " + record.value() +
                        ", Partition: " + record.partition() + ", Offset: " + record.offset() +
                        ", by ThreadID: " + Thread.currentThread().getId());
            }
        }

    }

//    public void run(int numThreads) {
//        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
//        topicCountMap.put(topic, numThreads);
//
//        Properties props = createProperties( groupId, url);
//        VerifiableProperties vProps = new VerifiableProperties(props);
//
//        // Create decoders for key and value
//        KafkaAvroDecoder avroDecoder = new KafkaAvroDecoder(vProps);
//
//        kafkaConsumer = new KafkaConsumer<>(props);
//
////        Map<String, List<KafkaStream<Object, Object>>> consumerMap =
////                consumer.createMessageStreams(topicCountMap, avroDecoder, avroDecoder);
////        List<KafkaStream<Object, Object>> streams = consumerMap.get(topic);
//
//        // Launch all the threads
//        executor = Executors.newFixedThreadPool(numThreads);
//
//        // Create ConsumerLogic objects and bind them to threads
//        int threadNumber = 0;
////        for (final KafkaStream stream : streams) {
////            executor.submit(new ConsumerLogic(stream, threadNumber));
////            threadNumber++;
////        }
//
//
//
//        kafkaConsumer.subscribe(Arrays.asList("foo", "bar"));
//        while (true) {
//            ConsumerRecords<String, GenericRecord> records = kafkaConsumer.poll(100);
//            for (ConsumerRecord<String, GenericRecord> record : records)
//                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
//        }
//    }

    public void shutdown() {
        if (kafkaConsumer != null) kafkaConsumer.close();
        if (executor != null) executor.shutdown();
        try {
            if (!executor.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
                System.out.println(
                        "Timed out waiting for consumer threads to shut down, exiting uncleanly");
            }
        } catch (InterruptedException e) {
            System.out.println("Interrupted during shutdown, exiting uncleanly");
        }
    }

    public static void main(String[] args) {
//    if (args.length != 5) {
//      System.out.println("Please provide command line arguments: "
//                         + "zookeeper groupId topic threads schemaRegistryUrl");
//      System.exit(-1);
//    }

//        String zooKeeper = ;
        String groupId = "group";
        String topic = "page_visits";
        int threads = 4;
        String url = "http://localhost:8081";

//        ConsumerGroupExample example = new ConsumerGroupExample(groupId, topic, url);
//        example.run(threads);


        for (int i=0; i< 3; i++) {
            ConsumerGroupExample consumerGroupExample = new ConsumerGroupExample(groupId, topic, url);
//            consumerGroupExample.
        }

        try {
            Thread.sleep(10000);
        } catch (InterruptedException ie) {

        }
//        example.shutdown();
    }
}
