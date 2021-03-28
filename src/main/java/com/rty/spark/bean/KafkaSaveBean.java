package com.rty.spark.bean;


import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

/**
 * yaml的使用,集成到spring中，可以使用springboot的配置进行读取
 */
public class KafkaSaveBean {
    private List<KafkaSaveDb> kafkaSaveDbs;

    public static KafkaSaveBean loadConfig(String config) {
        Yaml yaml = new Yaml();
        try (InputStream is = KafkaSaveBean.class.getResourceAsStream("/saveDb.yaml")) {
            return yaml.loadAs(is, KafkaSaveBean.class);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return new KafkaSaveBean();
    }

    public static class KafkaSaveDb {
        private String topic;
        private String groupId;
        public boolean enable;

        public String getTopic() {
            return topic;
        }

        public void setTopic(String topic) {
            this.topic = topic;
        }

        public String getGroupId() {
            return groupId;
        }

        public void setGroupId(String groupId) {
            this.groupId = groupId;
        }

        public boolean isEnable() {
            return enable;
        }

        public void setEnable(boolean enable) {
            this.enable = enable;
        }
    }


    public List<KafkaSaveDb> getKafkaSaveDbs() {
        return kafkaSaveDbs;
    }

    public void setKafkaSaveDbs(List<KafkaSaveDb> kafkaSaveDbs) {
        this.kafkaSaveDbs = kafkaSaveDbs;
    }
}
