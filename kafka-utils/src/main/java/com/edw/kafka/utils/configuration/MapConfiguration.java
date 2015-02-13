package com.edw.kafka.utils.configuration;

import java.util.Map;

/**
 * @author Eduard.Cojocaru
 *         Date: 5/6/14
 */
public class MapConfiguration extends StreamConfiguration {

    public MapConfiguration(Map<String, Object> configuration) {
        super();
        getProperties().putAll(configuration);
    }

}
