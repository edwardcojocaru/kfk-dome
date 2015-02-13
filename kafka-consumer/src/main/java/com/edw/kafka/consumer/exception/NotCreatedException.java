package com.edw.kafka.consumer.exception;

/**
 * User: eduard.cojocaru
 * Date: 12/17/13
 */
public class NotCreatedException extends Exception {

    public NotCreatedException(String message) {
        super(message);
    }

    public NotCreatedException(Throwable cause) {
        super(cause);
    }
}
