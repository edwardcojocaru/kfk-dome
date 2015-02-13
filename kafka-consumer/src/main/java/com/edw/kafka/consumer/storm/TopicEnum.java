package com.edw.kafka.consumer.storm;

/**
 * User: eduard.cojocaru
 * Date: 12/17/13
 */
public enum TopicEnum {

    GN_TOPIC("gn.kafka.topic", "StreamGN"),
    GI_TOPIC("gi.kafka.topic", "StreamGI");


    private String configurationTopicKey;
    private String streamName;

    private TopicEnum(String configurationTopicKey, String streamName) {
        this.configurationTopicKey = configurationTopicKey;
        this.streamName = streamName;
    }

    public String getConfigurationTopicKey() {
        return configurationTopicKey;
    }

    public String getStreamName() {
        return streamName;
    }
}
