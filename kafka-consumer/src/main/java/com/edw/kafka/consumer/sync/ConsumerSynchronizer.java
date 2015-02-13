package com.edw.kafka.consumer.sync;

import com.francetelecom.dome.utils.Constants;
import com.edw.kafka.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * User: eduard.cojocaru
 * Date: 12/13/13
 */
public class ConsumerSynchronizer {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerSynchronizer.class);

    private List<QueueHolder> queueHolders = new ArrayList<>();
    private double error = Constants.DEFAULT_ERROR_VALUE;
    private long lastPrinted;

    public ConsumerSynchronizer() {
    }

    public ConsumerSynchronizer(double error) {
        this.error = error;
    }

    public List<StreamBatch> getNextBatch() {

        List<StreamBatch> batches = new ArrayList<>();

        double holdersAverage = getHoldersAverage();

        for (QueueHolder holder : queueHolders) {
            if (holder.getAverage() <= (holdersAverage + this.error)) {
                final String streamName = holder.getStreamName();
                LOGGER.debug("Adding batch for stream {} ", streamName);
                batches.add(new StreamBatch(streamName, holder.getNextBatch()));
            }
        }

        if (LOGGER.isInfoEnabled() && Utils.isLogTime(lastPrinted)) {
            logAverages(holdersAverage);
        }

        LOGGER.debug("Sending {} batches to be emitted.", batches.size());
        return batches;
    }

    private double getHoldersAverage() {
        double average = 0;
        int t = 0;
        for (QueueHolder holder : queueHolders) {
            double holderAverage = holder.getAverage();
            if (holderAverage > 0) {
                average += (holderAverage - average) / ++t;
            }
        }

        return average;
    }

    public void addStreamQueue(QueueHolder holder) {
        queueHolders.add(holder);
    }

    private void logAverages(double holdersAverage) {
        StringBuilder message = new StringBuilder("\n============================================\n");
        message.append("== Holders average: ").append(holdersAverage).append("  ==\n");
        message.append("==                                        ==\n");
        message.append("== AVERAGES: [Stream Name]: [AverageTime] ==\n");
        message.append("==                                        ==\n");
        for (QueueHolder holder : queueHolders) {
            message.append("==  ").append(String.format("%12s", holder.getStreamName())).append(": ")
                    .append(holder.getAverage() - holder.getDelay()).append("   ==\n");
        }
        message.append("==                                        ==\n");
        message.append("============================================\n");

        lastPrinted = System.currentTimeMillis();
        LOGGER.info(message.toString());
    }

    private String getRelativeDate(double doubleValue) {
        SimpleDateFormat dt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        final Date averageHoldersDate = new Date();
        averageHoldersDate.setTime(Double.valueOf(doubleValue).longValue() * 1000);
        return dt.format(averageHoldersDate);
    }
}
