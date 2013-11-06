package com.francetelecom.dome.util;

import java.util.concurrent.TimeUnit;

/**
 * User: eduard.cojocaru
 * Date: 10/29/13
 */
public final class Constants {

    public static final String WATCHED_DIRECTORY = "watched.directory";
    public static final int MANAGERS_TIMEOUT = 10;
    public static final TimeUnit MANAGERS_TIMEOUT_UNITS = TimeUnit.SECONDS;

    private Constants() {
    }

    public static final String NUMBER_OF_MESSAGES_IN_BATCH = "500";

    public static final String TOPICS = "topics";

    public static final String PORT_SUFFIX = ".port";

    public static final String ACCEPT_SUFFIX = ".accept";
    public static final String ADDRESS_SUFFIX = ".address";
    public static final String BROKERS_SUFFIX = ".brokers";
    public static final String TOPIC_FILE_PREFIX = ".filePrefix";

    public static final String CONFIGURATION_FILE_BASE_NAME = "configTest";
    public static final String THREADS_NUMBER = "threadsNumber";
}
