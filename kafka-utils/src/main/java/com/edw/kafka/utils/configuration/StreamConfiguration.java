package com.edw.kafka.utils.configuration;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

/**
 * User: Eduard.Cojocaru
 * Date: 11/5/13
 */
public class StreamConfiguration extends AbstractConfiguration {

    private final Properties properties;

    public StreamConfiguration(InputStream inputStream) throws IOException {
        this.properties = new Properties();
        properties.load(inputStream);
    }

    protected StreamConfiguration() {
        this.properties = new Properties();
    }

    @Override
    public Object getProperty(String key) {
        return properties.getProperty(key);
    }

    @Override
    public Properties getProperties() {
        return properties;
    }

    @Override
    public Map<String, Object> getPropertiesAsMap() {
        Map<String, Object> conf = new HashMap<>();

        String key;
        for (Object keyObject : this.properties.keySet()) {
            key = (String)keyObject;
            conf.put(key, this.properties.getProperty(key));
        }

        return Collections.unmodifiableMap(conf);
    }

    @Override
    public Set<String> getPropertyNames() {
        return properties.stringPropertyNames();
    }
}
