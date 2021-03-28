package com.rty.spark.kafka.consumer;


import com.rty.spark.bean.KafkaSaveBean;

import javax.annotation.PostConstruct;

/**
 * 使用java8的集合的聚合函数
 */
public class KafkaSaveTableConsumerFactory {

    @PostConstruct
    public void init() {
        KafkaSaveBean kafkaSaveBean = KafkaSaveBean.loadConfig("/saveDb.yaml");
        kafkaSaveBean.getKafkaSaveDbs().stream()
                .filter(KafkaSaveBean.KafkaSaveDb::isEnable)
                .map(kafkaSaveDb ->
                        new KafkaSaveTableConsumer(kafkaSaveDb.getGroupId(),
                                null,
                                null,
                                kafkaSaveDb.getTopic()))
                .forEach(KafkaSaveTableConsumer::start);
    }

}
