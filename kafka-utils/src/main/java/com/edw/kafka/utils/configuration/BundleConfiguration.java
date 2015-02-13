package com.edw.kafka.utils.configuration;

import java.util.*;

/**
 * User: Eduard.Cojocaru
 * Date: 11/5/13
 */
public class BundleConfiguration extends AbstractConfiguration {

    private final ResourceBundle bundle;

    public BundleConfiguration(ResourceBundle bundle) {
        this.bundle = bundle;
    }

    @Override
    public Object getProperty(String key) {
        return bundle.getString(key);
    }

    @Override
    public Properties getProperties() {
        final Properties properties = new Properties();
        properties.putAll(getPropertiesAsMap());
        return properties;
    }

    @Override
    public Map<String, Object> getPropertiesAsMap() {
        Map<String, Object> conf = new HashMap<>();

        for (String key : this.bundle.keySet()) {
            conf.put(key, this.bundle.getObject(key));
        }

        return Collections.unmodifiableMap(conf);
    }

    @Override
    public Set<String> getPropertyNames() {
        return bundle.keySet();
    }
}
