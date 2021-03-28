package com.rty.spark.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertiesUtil {

    public static Properties loadConfig(){
            Properties properties = new Properties();
            InputStream inputStream = PropertiesUtil.class.getResourceAsStream("/application.properties");
            try {
                properties.load(inputStream);
            } catch (IOException e) {
                e.printStackTrace();
            }
            return  properties;
        }
}
