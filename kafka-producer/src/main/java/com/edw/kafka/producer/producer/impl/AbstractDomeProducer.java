package com.edw.kafka.producer.producer.impl;

import com.edw.kafka.producer.beans.ProducerType;
import com.edw.kafka.producer.beans.Topic;
import com.edw.kafka.producer.util.Constants;
import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.Files;
import java.nio.file.Path;
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

    private ProducerContext producerContext;

    public AbstractDomeProducer(Topic topic, InputStream inputStream) {
        this(topic, inputStream, null);
    }

    public AbstractDomeProducer(ProducerContext producerContext) {
        this(producerContext.getTopic(), producerContext.getInputStream(), producerContext.getProducerConfig());
        this.producerContext = producerContext;
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
    public Long call() throws IOException {

        LOGGER.info("Starting data injection to kafka...");
        // TODO check if the IOException should be handled in thread and not outside
        final long initialTime = System.nanoTime();

        final long numberOfLines = processStream(inputStream);

        final long endTime = System.nanoTime();

        NumberFormat formatter = new DecimalFormat("#0.0000");
        final String timeTaken = formatter.format((endTime - initialTime) / (1000d * 1000d * 1000d));
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Execution time is " + timeTaken + " seconds");
        }

        handleProcessed();

        return numberOfLines;
    }

    private void handleProcessed() {
        if (producerContext != null && producerContext.getFilePath() != null) {

            final Path filePath = producerContext.getFilePath();
            LOGGER.info("Removing file: {}", filePath);

            try {
                final boolean deleted = Files.deleteIfExists(filePath);
                LOGGER.info("File deleted: {}", deleted);
            } catch (DirectoryNotEmptyException dne) {
                LOGGER.warn("Deletion failed. Not an empty directory.");
            } catch (IOException e) {
                LOGGER.error("Could not delete the file {}", filePath);
            } catch (SecurityException se) {
                LOGGER.error("Not enough privileges.");
            }

        }
    }

    protected abstract long processStream(InputStream inputStream) throws IOException;

}
