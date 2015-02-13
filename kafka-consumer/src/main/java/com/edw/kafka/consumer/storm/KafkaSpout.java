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

import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * User: Eduard.Cojocaru
 * Date: 11/4/13
 */
public class KafkaSpout extends BaseRichSpout {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaSpout.class);

    private static final int NUMBER_OF_STREAMS_TO_REGISTER = 1;

    private final String topic;

    private final int batchSize;

    transient private SpoutOutputCollector collector;

    transient private BlockingQueue<String> queue;

    transient private AtomicBoolean working;

    public KafkaSpout(String topic, int batchSize) {
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

        ExecutorService executor = Executors.newFixedThreadPool(NUMBER_OF_STREAMS_TO_REGISTER);

        final Map<String, Object> kafkaConfig = Utils.getConfigMap(configurationMap);
        for (KafkaStream<byte[], byte[]> stream : Utils.getKafkaStream(kafkaConfig, topic)) {
            executor.submit(new KafkaConsumer(this.queue, stream, this.working));
        }
    }

    @Override
    public void nextTuple() {

        List<String> batch = Utils.getBatch(queue, batchSize);

        if (!batch.isEmpty()) {
            LOGGER.debug("Emitting batch with size: {}", batch.size());
            collector.emit(new Values(batch), System.currentTimeMillis());
        }

        if (!working.get() && queue.isEmpty()) {
            throw new ConsumerStoppedException("Kafka consumer stopped working");
        }
    }

}
