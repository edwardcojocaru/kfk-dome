package com.edw.kafka.consumer.storm;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.edw.kafka.consumer.exception.ConfigurationException;
import com.edw.kafka.consumer.exception.ConsumerStoppedException;
import com.edw.kafka.consumer.utils.Constants;
import com.edw.kafka.consumer.utils.Utils;
import kafka.consumer.KafkaStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * User: Eduard.Cojocaru
 * Date: 11/4/13
 */
public class KafkaRichSpout extends BaseRichSpout {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaRichSpout.class);

    private static final int NUMBER_OF_STREAMS_TO_REGISTER = 1;

    public static final int BUFFER_LIMIT = 50;

    public static final int NO_OF_RETRY_FAILED_TUPLE = 3;

    private final String topic;

    private final int batchSize;

    transient private SpoutOutputCollector collector;

    transient private BlockingQueue<String> queue;

    transient private AtomicBoolean working;

    transient private Map<String, List<String>> lastMessages;

    transient private Map<String, Integer> fails;

    public KafkaRichSpout(String topic, int batchSize) {
        this.topic = topic;
        this.batchSize = batchSize;
        if (batchSize == 0) {
            throw new ConfigurationException("Batch size set to 0 for kafka spout. Please change it to be greater.");
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(Constants.LINE_EVENT));
    }

    @Override
    public void open(Map configurationMap, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
        this.queue = new ArrayBlockingQueue<>(2 * batchSize);
        this.working = new AtomicBoolean(true);

        this.lastMessages = new HashMap<>(BUFFER_LIMIT);
        this.fails = new HashMap<>();

        ExecutorService executor = Executors.newFixedThreadPool(NUMBER_OF_STREAMS_TO_REGISTER);

        final Map<String, Object> kafkaConfig = Utils.getConfigMap(configurationMap);
        for (KafkaStream<byte[], byte[]> stream : Utils.getKafkaStream(kafkaConfig, topic)) {
            executor.submit(new KafkaConsumer(this.queue, stream, this.working));
        }

    }

    @Override
    public void nextTuple() {

        if (lastMessages.size() >= BUFFER_LIMIT) {
            return;
        }

        List<String> batch = Utils.getBatch(queue, batchSize);

        if (!batch.isEmpty()) {
            LOGGER.debug("Emitting batch with size: {}", batch.size());

            String messageId = UUID.randomUUID().toString();
            collector.emit(new Values(batch), messageId);
            if (LOGGER.isInfoEnabled() && lastMessages.size() > BUFFER_LIMIT) {
                LOGGER.info("Buffer size reached. No message will be stored.");
            } else {
                lastMessages.put(messageId, batch);
            }
        }

        if (!working.get() && queue.isEmpty()) {
            throw new ConsumerStoppedException("Kafka consumer stopped working");
        }
    }

    @Override
    public void ack (Object tuple) {
        String messageId = (String) tuple;
        lastMessages.remove(messageId);
        fails.remove(messageId);
    }

    @Override
    public void fail (Object tuple) {
        String messageId = (String) tuple;

        List<String> batch = lastMessages.get(messageId);
        boolean sendAgain = true;

        if (!fails.containsKey(messageId)) {
            fails.put(messageId, 1);
        } else {
            Integer failures = fails.get(messageId) + 1;
            if (failures <= NO_OF_RETRY_FAILED_TUPLE) {
                fails.put(messageId, failures);
            } else {
                fails.remove(messageId);
                sendAgain = false;
            }
        }

        if (sendAgain && batch != null && !batch.isEmpty()) {
            collector.emit(new Values(batch), messageId);
        }
    }
}
