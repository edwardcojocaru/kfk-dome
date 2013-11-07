package com.francetelecom.dome.consumer.utils;

/**
 * User: Eduard.Cojocaru
 * Date: 11/4/13
 */
public final class Constants {

    private Constants() {
    }

    public static final String SENTENCE = "sentence";
    public static final String KAFKA_CONSUMER_GROUP = "MasterGroup";
    public static final int BATCH_SIZE = 200;

    public static final String CLUSTER_MODE = "clusterMode";

    public static final String CONFIGURATION_FILE_BASE_NAME = "consumer";

    public static final String KAFKA_CONFIG_KEY = "KafkaConfig";

    public static final String TOPICS = "topics";

    public static final int SPOUT_PARALLELISM_DEFAULT_VALUE = 3;
}
