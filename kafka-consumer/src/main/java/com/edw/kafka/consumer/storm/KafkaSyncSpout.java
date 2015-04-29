package com.edw.kafka.consumer.storm;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.edw.kafka.consumer.exception.ConfigurationException;
import com.edw.kafka.consumer.sync.*;
import com.edw.kafka.consumer.utils.Constants;
import com.edw.kafka.consumer.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

import static com.edw.kafka.utils.Constants.ONE_HOUR_IN_SECONDS;

/**
 * User: eduard.cojocaru
 * Date: 12/16/13
 */
public class KafkaSyncSpout extends BaseRichSpout {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaSyncSpout.class);

    private final Map<TopicEnum, String> topicsMap;

    private final int batchSize;

    transient private SpoutOutputCollector collector;

    transient private ConsumerSynchronizer consumerSynchronizer;

    private Map<TopicEnum, Integer> delaysMap;

    public KafkaSyncSpout(Map<TopicEnum, String> topicsMap, int batchSize) {
        this(topicsMap, batchSize, null);
    }

    public KafkaSyncSpout(Map<TopicEnum, String> topicsMap, int batchSize, Map<TopicEnum, Integer> delaysMap) {
        this.topicsMap = topicsMap;
        this.batchSize = batchSize;
        if (batchSize == 0 || topicsMap == null || topicsMap.isEmpty()) {
            LOGGER.error("Batch size or topics not set. Check configuration.");
            throw new ConfigurationException("Batch size or topics not set. Check configuration.");
        }

        this.delaysMap = delaysMap;
        if (delaysMap == null) {
            this.delaysMap = Collections.emptyMap();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        for (TopicEnum topic : topicsMap.keySet()) {
            declarer.declareStream(topic.getStreamName(), new Fields(Constants.LINE_EVENT));
        }
    }

    @Override
    public void open(Map configurationMap, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
        this.consumerSynchronizer = new ConsumerSynchronizer(ONE_HOUR_IN_SECONDS);

        final Map<String, Object> kafkaConfig = Utils.getConfigMap(configurationMap);
        final ConsumerManager consumerManager = new ConsumerManager(topicsMap.values(), new ConsumerFactory(kafkaConfig));
        final Map<String, BlockingQueue<String>> workingQueues = consumerManager.getWorkingQueues();

        for (Map.Entry<TopicEnum, String> topic : topicsMap.entrySet()) {
            final TopicEnum topicEnum = topic.getKey();
            final ImportanceSelector importanceSelector = ImportanceSelectorFactory.getImportanceSelector(topicEnum);
            final BlockingQueue<String> queue = workingQueues.get(topic.getValue());
            final String streamName = topicEnum.getStreamName();
            consumerSynchronizer.addStreamQueue(new QueueHolder(importanceSelector, queue, batchSize, streamName, getDelay(topicEnum)));
        }
    }

    /**
     * This method will return the delay for processed queues.<br/>
     * <b>Note:</b> The delay should be added to the provided calculation error.
     *
     * @param topicEnum the topic that will get the delay
     * @return {@code 0} if no delay was specified otherwise {@code delay + error}
     */
    private Integer getDelay(TopicEnum topicEnum) {
        int delay = 0;
        if (delaysMap.get(topicEnum) != null) {
            delay = delaysMap.get(topicEnum) + ONE_HOUR_IN_SECONDS;
        }

        return delay;
    }

    @Override
    public void nextTuple() {

        final List<StreamBatch> nextBatch = consumerSynchronizer.getNextBatch();

        for (StreamBatch streamBatch : nextBatch) {
            collector.emit(streamBatch.getStreamName(), new Values(streamBatch.getBatch()));
        }
    }

}
