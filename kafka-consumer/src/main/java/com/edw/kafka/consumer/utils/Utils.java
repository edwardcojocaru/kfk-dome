package com.edw.kafka.consumer.utils;

import com.francetelecom.dome.consumer.ConsumerConnectorManager;
import com.edw.kafka.consumer.exception.ConfigurationException;
import com.edw.kafka.consumer.sync.ImportanceSelector;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * User: Eduard.Cojocaru
 * Date: 11/13/13
 */
public class Utils {

    private static final Logger LOGGER = LoggerFactory.getLogger(Utils.class);

    private static final int NUMBER_OF_STREAMS_TO_REGISTER = 1;
    private static final int NUMBER_OF_TRIES = 3;
    private static final int WAITING_TIME = 10;

    private Utils() {
    }

    public static void appendKafkaConfiguration(Map<String, Object> destinationConfigMap, Map<String, Object> kafkaConfiguration) {
        if (destinationConfigMap == null) {
            return;
        }

        destinationConfigMap.put(Constants.KAFKA_CONFIG_KEY, kafkaConfiguration);
    }

    public static List<KafkaStream<byte[], byte[]>> getKafkaStream(Map<String, Object> kafkaConfig, String topic) {
            return getKafkaStream(kafkaConfig, topic, Constants.KAFKA_CONSUMER_GROUP + "-" + topic);
    }

    public static List<KafkaStream<byte[], byte[]>> getKafkaStream(Map<String, Object> kafkaConfig, String topic, String groupId) {
        LOGGER.info("Getting connector...");
        for (int i = 0; i < NUMBER_OF_TRIES; i++) {
            try {
                ConsumerConnector consumerConnector = ConsumerConnectorManager.getInstance().getConnector(groupId, kafkaConfig);

                Map<String, List<KafkaStream<byte[], byte[]>>> messageStreams = consumerConnector.createMessageStreams(getTopicCountMap(topic));
                final List<KafkaStream<byte[], byte[]>> kafkaStreams = messageStreams.get(topic);
                LOGGER.debug("Received {} streams.", kafkaStreams.size());
                return kafkaStreams;
            } catch (RuntimeException ex) {

                String msg = "Exception during kafka stream retrieval.";
                if (i == NUMBER_OF_TRIES - 1) {
                    LOGGER.error(msg, ex);
                    throw ex;
                } else {
                    LOGGER.warn(msg + " Waiting {} seconds.", WAITING_TIME, ex);
                    try {
                        TimeUnit.SECONDS.sleep(WAITING_TIME);
                    } catch (InterruptedException e) {
                        LOGGER.error("Thread Interrupted.");
                    }
                }
            }
        }

        return Collections.emptyList();
    }

    private static Map<String, Integer> getTopicCountMap(String topic) {
        final Map<String, Integer> topicCountMap = new HashMap<>();
        topicCountMap.put(topic, NUMBER_OF_STREAMS_TO_REGISTER);
        return topicCountMap;
    }

    public static List<String> getBatch(BlockingQueue<String> queue, int batchSize) {
        List<String> batch = new ArrayList<>();
        int i = 0;
        try {
            String poll;
            while ((i++ < batchSize && (poll = queue.poll(1, TimeUnit.SECONDS)) != null)) {
                batch.add(poll);
            }

        } catch (InterruptedException e) {
            LOGGER.error("Thread interrupted. Emitting remaining elements.");
        }

        LOGGER.debug("Batch created with {} elements", batch.size());
        return batch;
    }

    public static double getAverage(ImportanceSelector selector, Collection<String> collection) {

        double average = 0;
        int t = 0;
        try {
            for (String line : collection) {
                final Long importance = selector.getImportance(line);
                if (importance > 0) {
                    average += (importance - average) / ++t;
                }
            }
            LOGGER.debug("Average calculated: {}." + average);
        } catch (Exception ex) {
            LOGGER.warn("Exception thrown in average calculation. Returning '0'.", ex);
            average = 0D;
        }

        return average;
    }

    @SuppressWarnings("unchecked")
    public static Map<String, Object> getConfigMap(Map configurationMap) {
        final Map<String, Object> kafkaConfig = (Map<String, Object>) configurationMap.get(Constants.KAFKA_CONFIG_KEY);
        if (kafkaConfig == null || kafkaConfig.isEmpty()) {
            LOGGER.error("Kafka configuration not provided.");
            throw new ConfigurationException("Kafka configuration not provided.");
        }

        return kafkaConfig;
    }
}
