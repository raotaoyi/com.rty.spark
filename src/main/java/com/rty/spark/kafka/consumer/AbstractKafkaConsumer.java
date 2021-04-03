package com.rty.spark.kafka.consumer;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public abstract class AbstractKafkaConsumer {
    private ExecutorService executorService = Executors.newSingleThreadExecutor();
    ;
    private boolean isStop = true;
    private String groupId;
    private String[] topics;


    public AbstractKafkaConsumer(String groupId, String... topics) {
        this.groupId = groupId;
        this.topics = topics;
    }

    private Properties getProperties() {
        Properties properties = new Properties();
        return properties;
    }

    private String getBootStrapServers() {
        return null;
    }

    private void stop() {

    }

    public void start() {
        if (!isStop) {
            throw new RuntimeException("consumer is running");
        }
        isStop = false;
        executorService.execute(this::consumer);
    }

    private void consumer() {
        ConsumerRecords<String, String> records = null;
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(getProperties());
        consumer.subscribe(Collections.singletonList(""));
        Map<TopicPartition, OffsetAndMetadata> currOffsets = new HashMap<>();
        while (true) {
            //kafka拉取数据
            records = consumer.poll(500);
            records.forEach(record -> {
                TopicPartition topicPartition = new TopicPartition(record.topic(), record.partition());
                long offSet = record.offset() + 1;
                currOffsets.put(topicPartition, new OffsetAndMetadata(offSet, ""));
            });
            process(records);
            //拉取之后手动提交kafka的位移
            submitAsyncOffset(consumer, currOffsets);
        }


    }

    private void submitAsyncOffset(KafkaConsumer<String, String> consumer, Map<TopicPartition, OffsetAndMetadata> currOffsets) {
        consumer.commitAsync(currOffsets, (Map<TopicPartition, OffsetAndMetadata> offsets, Exception e) -> {
                    if (e != null) {
                        e.printStackTrace();
                    }
                }
        );
    }

    protected abstract void process(ConsumerRecords<String, String> records );

    private void sleep(long sleepTime) {
        try {
            Thread.sleep(sleepTime);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }
}
