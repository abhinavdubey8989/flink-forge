package com.flink_forge.common.config;

import java.io.InputStream;
import java.util.Properties;


public class ConfigUtil {

    private static final String ENV_KEY = "APP_ENV";
    private static final Properties PROPERTIES = loadProperties();

    private ConfigUtil() {
        // prevent instantiation
    }

    private static Properties loadProperties() {
        String env = System.getenv().get(ENV_KEY);

        if (env == null || env.isEmpty()) {
            throw new RuntimeException("Unable to load application.properties file, environment variable is not set");
        }

        String fileName = "application-" + env + ".properties";
        Properties props = new Properties();

        try (InputStream input = ConfigUtil.class
                .getClassLoader()
                .getResourceAsStream(fileName)) {

            if (input == null) {
                throw new RuntimeException(fileName + " not found in classpath");
            }

            props.load(input);

        } catch (Exception e) {
            throw new RuntimeException("Failed to load properties file in ConfigUtil", e);
        }

        return props;
    }


    public static String get(String key) {
        String value = PROPERTIES.getProperty(key);
        if (value == null) {
            throw new RuntimeException("Missing required property: " + key);
        }
        return value;
    }


    public static String getDefaultOrJobSpecificConfig(
            String defaultConfigKey,
            String jobConfigKey) {
        String defaultConfigVal = (defaultConfigKey == null || defaultConfigKey.isBlank()) ? null : get(defaultConfigKey);
        String jobConfigVal = (jobConfigKey == null || jobConfigKey.isBlank()) ? null : get(jobConfigKey);
        return (jobConfigVal == null) ? defaultConfigVal : jobConfigVal;
    }
}

