package com.rty.spark.kafka.consumer;

import java.util.Map;

public interface DataSaveStrategy {
    public <T extends Map<String,Object>> void save(T result);
}
