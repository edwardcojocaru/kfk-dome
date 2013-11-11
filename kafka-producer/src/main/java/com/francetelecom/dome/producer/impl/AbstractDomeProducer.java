package com.francetelecom.dome.producer.impl;

import com.francetelecom.dome.beans.ProducerType;
import com.francetelecom.dome.beans.Topic;
import com.francetelecom.dome.util.Constants;
import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Map;
import java.util.Properties;

/**
 * User: Eduard.Cojocaru
 * Date: 11/11/13
 */
public abstract class AbstractDomeProducer implements DomeProducer {
    private static final Logger LOGGER = LoggerFactory.getLogger(StreamDomeProducer.class);

    private final ProducerType producerType = ProducerType.ASYNC;

    protected final Producer<String, String> producer;

    private final Properties properties = new Properties();

    protected final Topic topic;

    protected final InputStream inputStream;

    public AbstractDomeProducer(Topic topic, InputStream inputStream) {
        this(topic, inputStream, null);
    }

    public AbstractDomeProducer(ProducerContext producerContext) {
        this(producerContext.getTopic(), producerContext.getInputStream(), producerContext.getProducerConfig());
    }

    public AbstractDomeProducer(Topic topic, InputStream inputStream, Map<String, Object> additionalConfig) {

        LOGGER.info("Initializing producer...");
        LOGGER.info("Topic name: " + topic.getName());

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Topic: " + topic);
            LOGGER.debug("Producer type: " + producerType);
        }

        final ProducerConfig producerConfig = getProducerConfig(topic, additionalConfig);
        this.producer = new Producer<>(producerConfig);
        this.topic = topic;
        this.inputStream = inputStream;
    }

    private ProducerConfig getProducerConfig(Topic topic, Map<String, Object> additionalConfig) {
        properties.put("serializer.class", "kafka.serializer.StringEncoder");
        properties.put("metadata.broker.list", topic.getBrokerList());
        properties.put("request.required.acks", topic.getAcknowledge());

        // TODO may be externalize the producer type and the number of messages in batch
        properties.put("producer.type", producerType.getValue());
        properties.put("batch.num.messages", Constants.NUMBER_OF_MESSAGES_IN_BATCH);
        properties.put("compression.codec", "none"/*, "gzip" and "snappy"*/);

        if (additionalConfig != null) {
            properties.putAll(additionalConfig);
        }

        return new ProducerConfig(properties);
    }
    @Override
    public String call() throws IOException {

        LOGGER.info("Starting data injection to kafka...");
        // TODO check if the IOException should be handled in thread and not outside
        final long initialTime = System.nanoTime();

        processStream(inputStream);

        final long endTime = System.nanoTime();

        NumberFormat formatter = new DecimalFormat("#0.0000");
        final String timeTaken = formatter.format((endTime - initialTime) / (1000d * 1000d * 1000d));
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Execution time is " + timeTaken + " seconds");
        }

        return timeTaken;
    }

    protected abstract void processStream(InputStream inputStream) throws IOException;

}
