package com.francetelecom.dome.configuration;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.Set;

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

    @Override
    public Object getProperty(String key) {
        return properties.getProperty(key);
    }

    @Override
    public Set<String> getPropertyNames() {
        return properties.stringPropertyNames();
    }
}
