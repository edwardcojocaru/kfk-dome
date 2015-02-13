package com.edw.kafka.producer.beans;

/**
 * User: eduard.cojocaru
 * Date: 10/29/13
 */
public enum ProducerType {
    ASYNC("async"),
    SYNC("sync");

    private String value;

    private ProducerType(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
