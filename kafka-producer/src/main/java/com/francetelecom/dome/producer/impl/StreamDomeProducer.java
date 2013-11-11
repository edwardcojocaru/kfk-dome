package com.francetelecom.dome.producer.impl;

import com.francetelecom.dome.beans.Topic;
import kafka.producer.KeyedMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * User: eduard.cojocaru
 * Date: 10/29/13
 */
public class StreamDomeProducer extends AbstractDomeProducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(StreamDomeProducer.class);

    public StreamDomeProducer(Topic topic, InputStream inputStream) {
        super(topic, inputStream);
    }

    public StreamDomeProducer(ProducerContext producerContext) {
        super(producerContext);
    }

    protected void processStream(InputStream inputStream) throws IOException {
        LOGGER.info("Start processing...");
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            final String topicName = topic.getName();
            String line;

            while ((line = reader.readLine()) != null) {
                producer.send(new KeyedMessage<String, String>(topicName, line));
            }
        } finally {
            producer.close();
        }
    }
}
