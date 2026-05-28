package com.flink_forge.common.config;


import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

@Component
public class ConfigUtil {

    private static Environment environment;

    public ConfigUtil(Environment environment) {
        ConfigUtil.environment = environment;
    }

    public static String get(String key) {
        String value = environment.getProperty(key);

        if (value == null) {
            throw new RuntimeException("Missing required property: " + key);
        }

        return value;
    }
}
