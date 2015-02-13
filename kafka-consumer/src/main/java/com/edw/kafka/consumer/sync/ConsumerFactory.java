package com.edw.kafka.consumer.sync;

import com.edw.kafka.consumer.exception.ConfigurationException;
import com.edw.kafka.consumer.exception.NotCreatedException;
import com.edw.kafka.consumer.storm.KafkaConsumer;
import com.edw.kafka.consumer.utils.Utils;
import kafka.consumer.KafkaStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

/**
 * User: eduard.cojocaru
 * Date: 12/17/13
 */
public class ConsumerFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerFactory.class);

    public static final String NO_STREAM_RETURNED_CHECK_CONFIGURATION = "No Stream returned. Check configuration.";

    private Map<String, Object> kafkaConfig;

    public ConsumerFactory(Map<String, Object> kafkaConfig) {
        this.kafkaConfig = kafkaConfig;
    }

    public KafkaConsumer createConsumer(String topic, BlockingQueue<String> queue) throws NotCreatedException {

        validateTopic(topic);

        LOGGER.debug("Create consumer with existing queue for topic {}", topic);
        try {
            final List<KafkaStream<byte[], byte[]>> kafkaStream = Utils.getKafkaStream(kafkaConfig, topic);
            if (!kafkaStream.isEmpty()) {
                return new KafkaConsumer(queue, kafkaStream.get(0));
            }
        } catch (RuntimeException ex) {
            LOGGER.error("Exception thrown during stream retrieval. Rethrowing NotCreatedException.", ex);
            throw new NotCreatedException(ex);
        }

        throw new NotCreatedException(NO_STREAM_RETURNED_CHECK_CONFIGURATION);
    }

    public KafkaConsumer createConsumer(String topic) throws NotCreatedException {
        validateTopic(topic);
        LOGGER.debug("Create consumer for topic {}", topic);
        try {
            final List<KafkaStream<byte[], byte[]>> kafkaStream = Utils.getKafkaStream(kafkaConfig, topic);
            if (!kafkaStream.isEmpty()) {
                return new KafkaConsumer(kafkaStream.get(0));
            }
        } catch (RuntimeException ex) {
            LOGGER.error("Exception thrown during stream retrieval. Rethrowing NotCreatedException.", ex);
            throw new NotCreatedException(ex);
        }

        throw new NotCreatedException(NO_STREAM_RETURNED_CHECK_CONFIGURATION);
    }

    private boolean validateTopic(String topic) {
        if (topic == null || "".equals(topic)) {
            throw new ConfigurationException("topic not provided");
        }

        return true;
    }

}
