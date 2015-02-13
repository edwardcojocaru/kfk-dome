package com.edw.kafka.producer.beans;

/**
 * User: eduard.cojocaru
 * Date: 10/29/13
 */
public enum Acknowledge {

    NO_ACK("0"),
    LEADER_ACK("1"),
    ACK_MANY("2");

    private String value;

    private Acknowledge(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
