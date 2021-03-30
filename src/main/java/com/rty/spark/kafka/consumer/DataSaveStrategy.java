package com.rty.spark.kafka.consumer;

public interface DataSaveStrategy {
    public <T extends Map<String,Object>> void save(T result);
}
