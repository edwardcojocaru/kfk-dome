package com.francetelecom.dome.configuration;

/**
 * User: Eduard.Cojocaru
 * Date: 11/5/13
 */
public abstract class AbstractConfiguration implements Configurable {

    @Override
    public String getStringProperty(String key) {
        try {
            return (String)getProperty(key);
        } catch (Exception ex) {
            return null;
        }
    }

    @Override
    public int getIntProperty(String key) {
        try {
            return Integer.valueOf(getStringProperty(key));
        } catch (Exception ex) {
            return 0;
        }
    }
}
