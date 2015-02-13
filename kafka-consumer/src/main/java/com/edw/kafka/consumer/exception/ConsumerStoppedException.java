package com.edw.kafka.consumer.exception;

/**
 * User: Eduard.Cojocaru
 * Date: 12/5/13
 */
public class ConsumerStoppedException extends RuntimeException {
    public ConsumerStoppedException(String message) {
        super(message);
    }
}
