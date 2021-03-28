package com.rty.spark;

import com.rty.spark.util.PropertiesUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.collection.JavaConversions;

import java.util.Arrays;
import java.util.Properties;

/**
 * 本地的spark的计算
 */
public class SparkCalLocal {
    public static void main(String[] args) {
        SparkSession sparkSession=SparkFactory.getInstance().javaLocalSparkContext();
        Properties properties= PropertiesUtil.loadConfig();
        Dataset<Row> c1=sparkSession.read().format("jdbc").
                option("url",properties.get("db.url").toString()).
                option("driver",properties.get("db.driver").toString()).
                option("user",properties.get("db.userName").toString()).
                option("password",properties.get("db.password").toString()).
                option("dbtable","(select * from t_mc_job_info) a").load();
        Dataset<Row> c2=sparkSession.read().format("jdbc").
                option("url",properties.get("db.url").toString()).
                option("driver",properties.get("db.driver").toString()).
                option("user",properties.get("db.userName").toString()).
                option("password",properties.get("db.password").toString()).
                option("dbtable","(select * from t_mc_proj_info) a").load();
        Dataset<Row>  c3=c1.join(c2, JavaConversions.asScalaBuffer(Arrays.asList(new String[]{"REGISTER_KEY","AREA"})),"left");
        c3.collectAsList().stream().forEach(row->{

        });
    }
}
