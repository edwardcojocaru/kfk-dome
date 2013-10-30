package com.francetelecom.dome.beans;

/**
 * User: eduard.cojocaru
 * Date: 10/29/13
 */
public class Topic {

    private String brokerList;

    private String name;

    private Acknowledge acknowledge = Acknowledge.LEADER_ACK;

    public Topic(String name, String brokerList) {
        this.brokerList = brokerList;
        this.name = name;
    }

    public String getBrokerList() {
        return brokerList;
    }

    public String getName() {
        return name;
    }

    public String getAcknowledge() {
        return acknowledge.getValue();
    }

    @Override
    public String toString() {
        return "Topic{" +
                "brokerList='" + brokerList + '\'' +
                ", name='" + name + '\'' +
                ", acknowledge=" + acknowledge +
                '}';
    }
}
