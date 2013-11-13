package com.francetelecom.dome.utils.configuration;

import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * User: Eduard.Cojocaru
 * Date: 11/5/13
 */
public interface Configurable {

    Object getProperty(String key);
    String getStringProperty(String key);
    String getStringProperty(String key, String defaultValue);
    int getIntProperty(String key);
    int getIntProperty(String key, int defaultValue);
    boolean getBooleanProperty(String key);

    Properties getProperties();
    Map<String, Object> getPropertiesAsMap();
    Set<String> getPropertyNames();
    Map<String, Object> getConfigProperties(String baseKey);
}
