package com.rty.spark.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.util.HashMap;
import java.util.List;

public class KafkaSaveTableConsumer extends AbstractKafkaConsumer {

    private List<Preprocess> preprocesses;

    private List<DataSaveStrategy> dataSaveStrategies;

    public KafkaSaveTableConsumer(String groupId,
                                  List<Preprocess> preprocesses,
                                  List<DataSaveStrategy> dataSaveStrategies,
                                  String... topics) {
        super(groupId, topics);
        this.preprocesses = preprocesses;
        this.dataSaveStrategies = dataSaveStrategies;
    }

    @Override
    protected void process(ConsumerRecords<String, String> records) {
        //获取数据,组装入表的数据
        Map<String,String> result=new HashMap<>();
        records.forEach(record->{

        });
        //选择策略，数据的保存
        dataSaveStrategies.stream().forEach(DataSaveStrategy::save(result));
    }

}
