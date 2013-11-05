package com.francetelecom.dome.consumer.configuration;

/**
 * User: Eduard.Cojocaru
 * Date: 11/5/13
 */
public interface Configurable {

    Object getProperty(String key);
    String getStringProperty(String key);
    Integer getIntProperty(String key);
}
