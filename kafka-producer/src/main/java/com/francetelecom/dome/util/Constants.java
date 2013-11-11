package com.francetelecom.dome.util;

import java.util.concurrent.TimeUnit;

/**
 * User: eduard.cojocaru
 * Date: 10/29/13
 */
public final class Constants {

    private Constants() {
    }

    public static final String GZIP_FILE = "application/x-gzip";
    public static final String TAR_FILE = "application/x-gtar";

    public static final String BASE_PRODUCER_CONFIG = "producer.config.";

    public static final int MANAGERS_TIMEOUT = 10;
    public static final TimeUnit MANAGERS_TIMEOUT_UNITS = TimeUnit.SECONDS;
    public static final int WORKER_EXECUTOR_TIMEOUT = 10;
    public static final TimeUnit WORKER_EXECUTOR_TIMEOUT_UNITS = TimeUnit.MINUTES;

    public static final String NUMBER_OF_MESSAGES_IN_BATCH = "500";
    public static final String TOPICS = "topics";

    public static final String WATCHED_DIRECTORY = "watched.directory";

    public static final String MANAGEMENT_PORT = "management.port";

    public static final int DEFAULT_MANAGEMENT_PORT = 12321;
    public static final String PORT_SUFFIX = ".port";

    public static final String ACCEPT_SUFFIX = ".accept";

    public static final String ADDRESS_SUFFIX = ".address";
    public static final String BROKERS_SUFFIX = ".brokers";
    public static final String TOPIC_FILE_PREFIX = ".filePrefix";
    public static final String CONFIGURATION_FILE_BASE_NAME = "configTest";

    public static final String THREADS_NUMBER = "threadsNumber";
    public static final int DEFAULT_THREADS_NUMBER = 15;
}
