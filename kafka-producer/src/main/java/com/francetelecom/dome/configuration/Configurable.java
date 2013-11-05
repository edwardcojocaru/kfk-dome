package com.francetelecom.dome.configuration;

/**
 * User: Eduard.Cojocaru
 * Date: 11/5/13
 */
public interface Configurable {

    Object getProperty(String key);
    String getStringProperty(String key);
    int getIntProperty(String key);
}
