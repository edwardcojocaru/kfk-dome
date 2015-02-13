package com.edw.kafka.utils.configuration;

/**
 * @author Eduard.Cojocaru
 *         Date: 5/9/14
 */
public class InvalidConfigurationPathException extends RuntimeException {
    public static final InvalidConfigurationPathException INSTANCE = new InvalidConfigurationPathException();
}
