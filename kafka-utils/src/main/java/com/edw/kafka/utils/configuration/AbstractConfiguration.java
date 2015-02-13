package com.edw.kafka.utils.configuration;

import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * User: Eduard.Cojocaru
 * Date: 11/5/13
 */
public abstract class AbstractConfiguration implements Configurable {

    @Override
    public String getStringProperty(String key) {
        return getStringProperty(key, null);
    }

    @Override
    public String getStringProperty(String key, String defaultValue) {
        try {
            String property = (String) getProperty(key);
            return (StringUtils.isBlank(property)? defaultValue : property);
        } catch (Exception ex) {
            return defaultValue;
        }
    }

    @Override
    public int getIntProperty(String key) {
        return getIntProperty(key, 0);
    }

    @Override
    public int getIntProperty(String key, int defaultValue) {
        try {
            return Integer.valueOf(getStringProperty(key));
        } catch (Exception ex) {
            return defaultValue;
        }
    }

    @Override
    public boolean getBooleanProperty(String key) {
        return Boolean.parseBoolean(getStringProperty(key));
    }

    @Override
    public Map<String, Object> getConfigProperties(String baseKey) {
        Map<String, Object> config = new HashMap<>();

        for (String key : getPropertyNames()) {
            if (key.startsWith(baseKey)) {
                String configKey = key.replace(baseKey, "");
                config.put(configKey, getProperty(key));
            }
        }

        return config;
    }
}
