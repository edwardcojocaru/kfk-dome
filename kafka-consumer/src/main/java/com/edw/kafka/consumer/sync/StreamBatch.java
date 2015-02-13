package com.edw.kafka.consumer.sync;

import java.util.List;

/**
 * User: eduard.cojocaru
 * Date: 12/13/13
 */
public class StreamBatch {

    private String streamName;

    private List<String> batch;

    public StreamBatch(String streamName, List<String> batch) {
        this.streamName = streamName;
        this.batch = batch;
    }

    public String getStreamName() {
        return streamName;
    }

    public List<String> getBatch() {
        return batch;
    }
}
