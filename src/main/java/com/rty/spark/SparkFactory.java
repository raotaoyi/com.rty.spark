package com.rty.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

/**
 * spark分为单机模式(local)和多机模式(spark on yarn)，不管你Spark跑在什么上面，它的代码都是一样的，
 * 其中Spark on YARN是工作中或生产上用的非常多的一种运行模式。
 */
public class SparkFactory {
    private static SparkFactory sparkFactory;
    private SparkFactory(){}

    public static SparkFactory getInstance(){
        if(sparkFactory==null){
            sparkFactory=new SparkFactory();
        }
        return sparkFactory;
    }
    public SparkSession javaLocalSparkContext(){
        SparkConf conf=new SparkConf();
        conf.set("spark.sql.shuffle.partitions","1");
        conf.set("spark.executor.cores","1"); //
        conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer");//spark的序列化器
        conf.setMaster("local[4]").setAppName("rty-task");
        SparkSession session= SparkSession.builder().config(conf).getOrCreate();
        return session;
    }
}
