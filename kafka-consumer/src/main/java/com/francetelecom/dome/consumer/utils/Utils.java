package com.francetelecom.dome.consumer.utils;

import com.francetelecom.dome.utils.configuration.Configurable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * User: Eduard.Cojocaru
 * Date: 11/13/13
 */
public class Utils {

    private static final Logger LOGGER = LoggerFactory.getLogger(Utils.class);

    private Utils() {
    }

    public static Map<String, Object> getStormConfiguration(Configurable configurable) {
        final Map<String, Object> stormConf = new HashMap<>();

        appendKafkaConfiguration(stormConf, configurable.getPropertiesAsMap());

        LOGGER.debug("Storm config: {}", stormConf);

        return stormConf;
    }

    public static void appendKafkaConfiguration(Map<String, Object> destinationConfigMap, Map<String, Object> kafkaConfiguration) {
        if (destinationConfigMap == null) {
            return;
        }
        
        destinationConfigMap.put(Constants.KAFKA_CONFIG_KEY, kafkaConfiguration);
    }
}
