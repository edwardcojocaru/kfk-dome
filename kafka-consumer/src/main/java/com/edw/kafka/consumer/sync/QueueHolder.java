package com.edw.kafka.consumer.sync;

import com.edw.kafka.consumer.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.BlockingQueue;

/**
 * User: eduard.cojocaru
 * Date: 12/16/13
 */
public class QueueHolder {

    private static final Logger LOGGER = LoggerFactory.getLogger(QueueHolder.class);

    private BlockingQueue<String> queue;

    private int batchSize;

    private double average;

    private ImportanceSelector selector;

    private String streamName;

    private int delay;

    public QueueHolder(ImportanceSelector selector, BlockingQueue<String> queue, int batchSize, String streamName) {
        this.selector = selector;
        this.queue = queue;
        this.batchSize = batchSize;
        this.streamName = streamName;
    }

    public QueueHolder(ImportanceSelector selector, BlockingQueue<String> queue, int batchSize, String streamName, int delay) {
        this.selector = selector;
        this.queue = queue;
        this.batchSize = batchSize;
        this.streamName = streamName;
        this.delay = delay;
    }

    /**
     * Returns the next batch elements extracted from wrapped queue.
     *
     * @return the batch of elements from queue
     */
    public List<String> getNextBatch() {
        LOGGER.debug("Getting batch.");
        List<String> batch = Utils.getBatch(queue, batchSize);
        LOGGER.debug("Calculating average for extracted batch.");
        average = Utils.getAverage(selector, batch);

        if (average < 1389398400 && average > 0) {
            LOGGER.info("Lines for {} with calculated average {}: \n {} \n", new Object[]{streamName, average, batch});
        }

        return batch;
    }

    /**
     * Returns the average of the previous batch returned. If the queue data have to be delayed the average will
     * contain also the delay.
     *
     * @return the calculated average of the previous batch emitted.
     */
    public double getAverage() {
        return average + delay;
    }

    public String getStreamName() {
        return streamName;
    }

    public int getDelay() {
        return delay;
    }
}
