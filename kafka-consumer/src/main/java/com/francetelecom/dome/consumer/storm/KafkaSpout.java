package com.francetelecom.dome.consumer.storm;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.francetelecom.dome.consumer.ConsumerConnectorManager;
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

    private static final String SENTENCE = "sentence";
    private static final String KAFKA_CONSUMER_GROUP = "MasterGroup";

    private static final int NUMBER_OF_STREAMS_TO_REGISTER = 1;
    private static final int BATCH_SIZE = 200;
    private static final int QUEUE_CAPACITY = 2 * BATCH_SIZE;

    private final ExecutorService executor;
    private final String topic;

    private BlockingQueue<String> queue;

    private ConsumerConnector consumerConnector;
    private SpoutOutputCollector collector;

    private List<String> batch;
    private KafkaConsumer kafkaConsumer;

    public KafkaSpout(String topic) {
        this.topic = topic;
        executor = Executors.newFixedThreadPool(NUMBER_OF_STREAMS_TO_REGISTER);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(SENTENCE));
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
        this.queue = new ArrayBlockingQueue<>(QUEUE_CAPACITY);

        for (KafkaStream<byte[], byte[]> stream : getKafkaStream(map)) {
            executor.submit(new KafkaConsumer(this.queue, stream));
        }
    }

    private List<KafkaStream<byte[], byte[]>> getKafkaStream(Map map) {
        LOGGER.info("Getting connector...");
        final Map<String, Object> kafkaConfig = (Map<String, Object>) map.get("KafkaConfig");

        this.consumerConnector = ConsumerConnectorManager.getInstance().getConnector(KAFKA_CONSUMER_GROUP, kafkaConfig);

        Map<String, List<KafkaStream<byte[], byte[]>>> messageStreams = consumerConnector.createMessageStreams(getTopicCountMap());
        return messageStreams.get(this.topic);
    }

    private Map<String, Integer> getTopicCountMap() {
        final Map<String, Integer> topicCountMap = new HashMap<>();
        topicCountMap.put(topic, NUMBER_OF_STREAMS_TO_REGISTER);
        return topicCountMap;
    }

    @Override
    public void nextTuple() {

        batch = new ArrayList<>();
        int i = 0;

        try {
            String poll;
            while (i++ < BATCH_SIZE && (poll = queue.poll(1, TimeUnit.SECONDS)) != null) {
                batch.add(poll);
            }
            emit(batch);
        } catch (InterruptedException e) {

            LOGGER.error("Thread interrupted.");

            if (!batch.isEmpty()) {
                emit(batch);
            }
        } finally {
            batch = null;
        }
    }

    private void emit(List<String> batch) {
        collector.emit(new Values(batch));
    }

}
