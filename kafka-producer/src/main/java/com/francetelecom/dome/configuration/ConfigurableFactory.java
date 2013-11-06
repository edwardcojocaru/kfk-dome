package com.francetelecom.dome.configuration;

import com.francetelecom.dome.util.Constants;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ResourceBundle;

/**
 * User: Eduard.Cojocaru
 * Date: 11/6/13
 */
public final class ConfigurableFactory {

    private ConfigurableFactory() {
    }

    public static Configurable getConfigurable(String configurationPath) throws IOException {
        if (configurationPath != null) {
            return new StreamConfiguration(new FileInputStream(configurationPath));
        } else {
            return new BundleConfiguration(ResourceBundle.getBundle(Constants.CONFIGURATION_FILE_BASE_NAME));
        }
    }
}
