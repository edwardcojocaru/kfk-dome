package com.edw.kafka.consumer;

import com.edw.kafka.utils.configuration.Configurable;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.javaapi.consumer.ConsumerConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * User: Eduard.Cojocaru
 * Date: 11/4/13
 */
public class ConsumerConnectorManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerConnectorManager.class);

    private final Map<String, ConsumerConnector> connectorsMap = new HashMap<>();

    private ConsumerConnectorManager() {
    }

    private static class ConnectorHolder {
        private static final ConsumerConnectorManager CONSUMER_CONNECTOR = new ConsumerConnectorManager();
    }

    public static ConsumerConnectorManager getInstance() {
        return ConnectorHolder.CONSUMER_CONNECTOR;
    }

    public ConsumerConnector getConnector(Configurable configurable, String group) {

        if (!connectorsMap.containsKey(group)) {
            LOGGER.info("Create consumer");
            connectorsMap.put(group, Consumer.createJavaConsumerConnector(createConsumerConfig(group, configurable.getPropertiesAsMap())));
        }

        return connectorsMap.get(group);
    }

    public ConsumerConnector getConnector(String group, Map<String, Object> config) {

        if (!connectorsMap.containsKey(group)) {
            LOGGER.debug("Create consumer for group: {}", group);
            connectorsMap.put(group, Consumer.createJavaConsumerConnector(createConsumerConfig(group, config)));
        }

        return connectorsMap.get(group);
    }

    private ConsumerConfig createConsumerConfig(String group, Map<String, Object> config) {
        LOGGER.debug("Config: " + config);
        final Properties properties = new Properties();
        properties.putAll(config);
        properties.put("group.id", group);

        return new ConsumerConfig(properties);
    }

}
