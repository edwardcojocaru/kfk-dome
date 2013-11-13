package com.francetelecom.dome.utils.configuration;

import com.francetelecom.dome.utils.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ResourceBundle;

/**
 * User: Eduard.Cojocaru
 * Date: 11/6/13
 */
public final class ConfigurableFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigurableFactory.class);
    private ConfigurableFactory() {
    }

    public static Configurable getConfigurable(String configurationPath) throws IOException {
        if (configurationPath != null) {
            LOGGER.info("Loading stream utils from {}", configurationPath);
            return new StreamConfiguration(new FileInputStream(configurationPath));
        } else {
            LOGGER.info("Loading bundle utils from jar.");
            return new BundleConfiguration(ResourceBundle.getBundle(Constants.CONFIGURATION_FILE_BASE_NAME));
        }
    }
}
