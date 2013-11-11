package com.francetelecom.dome.consumer.storm;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.francetelecom.dome.consumer.ConsumerConnectorManager;
import com.francetelecom.dome.consumer.utils.Constants;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * User: Eduard.Cojocaru
 * Date: 11/4/13
 */
public class KafkaSpout extends BaseRichSpout {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaSpout.class);

    private static final int NUMBER_OF_STREAMS_TO_REGISTER = 1;
    private static final int QUEUE_CAPACITY = 2 * Constants.BATCH_SIZE;

    private SpoutOutputCollector collector;

    private final String topic;

    transient private ExecutorService executor;
    transient private BlockingQueue<String> queue;
    transient private ConsumerConnector consumerConnector;

    public KafkaSpout(String topic) {
        this.topic = topic;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(Constants.SENTENCE));
    }

    @Override
    public void open(Map configurationMap, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
        this.queue = new ArrayBlockingQueue<>(QUEUE_CAPACITY);
        executor = Executors.newFixedThreadPool(NUMBER_OF_STREAMS_TO_REGISTER);
        for (KafkaStream<byte[], byte[]> stream : getKafkaStream((Map<String, Object>) configurationMap.get(Constants.KAFKA_CONFIG_KEY))) {
            executor.submit(new KafkaConsumer(this.queue, stream));
        }
    }

    private List<KafkaStream<byte[], byte[]>> getKafkaStream(Map<String, Object> kafkaConfig) {
        LOGGER.info("Getting connector...");
        this.consumerConnector = ConsumerConnectorManager.getInstance().getConnector(Constants.KAFKA_CONSUMER_GROUP + "-" + this.topic, kafkaConfig);

        Map<String, List<KafkaStream<byte[], byte[]>>> messageStreams = consumerConnector.createMessageStreams(getTopicCountMap());
        final List<KafkaStream<byte[], byte[]>> kafkaStreams = messageStreams.get(this.topic);
        LOGGER.debug("Received {} streams.", kafkaStreams.size());

        return kafkaStreams;
    }

    private Map<String, Integer> getTopicCountMap() {
        final Map<String, Integer> topicCountMap = new HashMap<>();
        topicCountMap.put(topic, NUMBER_OF_STREAMS_TO_REGISTER);
        return topicCountMap;
    }

    @Override
    public void nextTuple() {

        List<String> batch = new ArrayList<>();
        int i = 0;

        try {
            String poll;
            while (i++ < Constants.BATCH_SIZE && (poll = queue.poll(1, TimeUnit.SECONDS)) != null) {
                batch.add(poll);
            }
            emit(batch);
        } catch (InterruptedException e) {

            LOGGER.error("Thread interrupted.");

            if (!batch.isEmpty()) {
                emit(batch);
            }
        }
    }

    private void emit(List<String> batch) {
        collector.emit(new Values(batch));
    }

}
