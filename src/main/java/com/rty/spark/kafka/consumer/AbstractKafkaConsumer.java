package com.rty.spark.kafka.consumer;


import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.util.Properties;

public abstract class AbstractKafkaConsumer {
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
        consumer();

    }

    private void consumer() {

    }

    private void submitOffset() {

    }

    protected abstract void process(ConsumerRecords<String, String> records);

    private void sleep(long sleepTime) {
        try {
            Thread.sleep(sleepTime);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }
}
