package com.edw.kafka.utils.configuration;

import com.francetelecom.dome.utils.Constants;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
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

        try {
            LOGGER.info("Try loading config bundle from {}", Constants.CONFIGURATION_FILE_BASE_NAME);
            if (StringUtils.isBlank(configurationPath)) {
                return new BundleConfiguration(ResourceBundle.getBundle(Constants.CONFIGURATION_FILE_BASE_NAME));
            }

            LOGGER.info("Try loading config bundle from {}", configurationPath);
            try {
                return new BundleConfiguration(ResourceBundle.getBundle(configurationPath));
            } catch (RuntimeException ex) {
                LOGGER.debug("Bundle not found.", ex);
            }

            LOGGER.info("Try loading config as stream from {}", configurationPath);
            try {
                return new StreamConfiguration(ConfigurableFactory.class.getResourceAsStream(configurationPath));
            } catch (RuntimeException ex) {
                LOGGER.debug("resource stream not found", ex);
            }

            LOGGER.info("Try loading config file from {}", configurationPath);
            return new StreamConfiguration(new FileInputStream(configurationPath));

        } catch (Exception e) {
            LOGGER.error("Configuration not available.");
            throw InvalidConfigurationPathException.INSTANCE;
        }
    }

    public static Configurable getConfigurable(InputStream inputStream) throws IOException {
        return new StreamConfiguration(inputStream);
    }

    public static Configurable getConfigurable(Map<String, Object> configuration) {
        return new MapConfiguration(configuration);
    }

    public static Configurable getConfigurable(ResourceBundle bundle) {
        return new BundleConfiguration(bundle);
    }

}
