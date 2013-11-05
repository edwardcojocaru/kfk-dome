package com.francetelecom.dome.consumer;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * User: Eduard.Cojocaru
 * Date: 11/4/13
 */
public enum KafkaConfigManager {

    INSTANCE;

    private Properties configurationMap;

    private KafkaConfigManager() {
    }

    public void init(Map<String, Object> config) {
        this.configurationMap = new Properties();
        this.configurationMap.putAll(config);
    }

    public void init(InputStream inputStream) throws IOException {
        final Properties properties = new Properties();
        properties.load(inputStream);

        this.configurationMap = properties;
    }

    public void init(String filePath) throws IOException {

        final FileInputStream inputStream = new FileInputStream(new File(filePath));
        final Properties properties = new Properties();
        properties.load(inputStream);

        this.configurationMap = properties;
    }

    public Properties getConfiguration() {
        if (this.configurationMap == null) {
            throw new IllegalArgumentException("Configuration not initialised. Try getInstance(Map) to init.");
        }
        return clone(this.configurationMap);
    }

    private Properties clone(Properties properties) {
        final Map<Object, Object> map = new HashMap<>();
        for (Map.Entry<Object,Object> entry : properties.entrySet()) {
            map.put(entry.getKey(), entry.getValue());
        }

        final Properties propertiesToReturn = new Properties();
        propertiesToReturn.putAll(map);

        return propertiesToReturn;
    }

    public Map<String, Object> getConfigurationAsMap() {
        Map<String, Object> conf = new HashMap<>();

        String key;
        for (Object keyObject : this.configurationMap.keySet()) {
            key = (String)keyObject;
            conf.put(key, this.configurationMap.getProperty(key));
        }

        return Collections.unmodifiableMap(conf);
    }

}
