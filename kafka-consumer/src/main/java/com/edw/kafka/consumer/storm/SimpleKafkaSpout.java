package com.edw.kafka.consumer.storm;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.edw.kafka.consumer.utils.Constants;
import com.edw.kafka.consumer.utils.Utils;
import kafka.consumer.ConsumerIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * User: Eduard.Cojocaru
 * Date: 11/4/13
 */
public class SimpleKafkaSpout extends BaseRichSpout {

    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleKafkaSpout.class);

    private final String topic;

    private final int batchSize;

    transient private SpoutOutputCollector collector;

    transient private ConsumerIterator<byte[], byte[]> consumerIterator;

    public SimpleKafkaSpout(String topic, int batchSize) {
        this.topic = topic;
        this.batchSize = batchSize;
        if (batchSize == 0) {
            throw new IllegalArgumentException("Batch size set to 0 for kafka spout. Please change it to be greater.");
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(Constants.LINE_EVENT));
    }

    @Override
    public void open(Map configurationMap, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;

        final Map<String, Object> kafkaConfig = Utils.getConfigMap(configurationMap);
        this.consumerIterator = Utils.getKafkaStream(kafkaConfig, this.topic).get(0).iterator();
    }

    @Override
    public void nextTuple() {

        List<String> batch = new ArrayList<>();
        int i = 0;

        try {
            while (consumerIterator.hasNext()) {

                final String line = new String(consumerIterator.next().message(), Constants.ENCODING);
                batch.add(line);
                LOGGER.debug(topic + " spout reads ; total this round: " + batch.size());

                if (++i >= batchSize) {
                    break;
                }
            }
        } catch (RuntimeException e) {
            LOGGER.error("Thread interrupted. Emitting remaining elements.");
        } finally {
            if (!batch.isEmpty()) {
                LOGGER.debug("Emitting batch with size: {}", batch.size());
                collector.emit(new Values(batch));
            }
        }
    }
}
