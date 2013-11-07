package com.francetelecom.dome.consumer.configuration;

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
            return (String)getProperty(key);
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
}
