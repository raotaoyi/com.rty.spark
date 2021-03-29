package com.rty.spark.kafka.consumer;


import org.apache.kafka.clients.consumer.ConsumerRecords;

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
        //kafka拉取数据
        process(records);
        //拉取之后手动提交kafka的位移
        submitOffset();
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
