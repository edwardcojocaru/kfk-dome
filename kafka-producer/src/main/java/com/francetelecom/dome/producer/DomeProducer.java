package com.francetelecom.dome.producer;

import com.francetelecom.dome.beans.ProducerType;
import com.francetelecom.dome.beans.Topic;
import com.francetelecom.dome.util.Constants;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Properties;
import java.util.concurrent.Callable;

/**
 * User: eduard.cojocaru
 * Date: 10/29/13
 */
public class DomeProducer implements Callable<String> {

    private static final Logger LOGGER = LoggerFactory.getLogger(DomeProducer.class);

    private final ProducerType producerType = ProducerType.ASYNC;

    private final Producer<String, String> producer;

    private final Properties properties = new Properties();

    private final Topic topic;

    private final InputStream inputStream;

    public DomeProducer(Topic topic, InputStream inputStream) {

        LOGGER.info("Initializing producer...");
        LOGGER.info("Topic name: " + topic.getName());

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Topic: " + topic);
            LOGGER.debug("Producer type: " + producerType);
        }

        final ProducerConfig producerConfig = getProducerConfig(topic);
        this.producer = new Producer<>(producerConfig);
        this.topic = topic;
        this.inputStream = inputStream;
    }

    private ProducerConfig getProducerConfig(Topic topic) {
        properties.put("serializer.class", "kafka.serializer.StringEncoder");
        properties.put("metadata.broker.list", topic.getBrokerList());
        properties.put("request.required.acks", topic.getAcknowledge());

        // TODO may be externalize the producer type and the number of messages in batch
        properties.put("producer.type", producerType.getValue());
        properties.put("batch.num.messages", Constants.NUMBER_OF_MESSAGES_IN_BATCH);

        return new ProducerConfig(properties);
    }

    @Override
    public String call() throws IOException {

        LOGGER.info("Starting data injection to kafka...");
        // TODO check if the IOException should be handled in thread and not outside
        final long initialTime = System.nanoTime();

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            final String topicName = topic.getName();
            String line;

            while ((line = reader.readLine()) != null) {
                producer.send(new KeyedMessage<String, String>(topicName, line));
            }
        } finally {
            producer.close();
        }

        final long endTime = System.nanoTime();

        NumberFormat formatter = new DecimalFormat("#0.0000");
        final String timeTaken = formatter.format((endTime - initialTime) / (1000d * 1000d * 1000d));
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Execution time is " + timeTaken + " seconds");
        }

        return timeTaken;
    }
}
