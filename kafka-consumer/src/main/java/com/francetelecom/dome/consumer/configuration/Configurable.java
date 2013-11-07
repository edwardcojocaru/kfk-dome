package com.francetelecom.dome.consumer.configuration;

import java.util.Map;
import java.util.Properties;

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

    Properties getProperties();
    Map<String, Object> getPropertiesAsMap();

}
