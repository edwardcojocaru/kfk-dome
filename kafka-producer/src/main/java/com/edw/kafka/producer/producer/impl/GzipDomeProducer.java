package com.edw.kafka.producer.producer.impl;

import com.edw.kafka.producer.beans.Topic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;

/**
 * User: eduard.cojocaru
 * Date: 10/29/13
 */
public class GzipDomeProducer extends StreamDomeProducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(GzipDomeProducer.class);

    public GzipDomeProducer(ProducerContext producerContext) {
        super(producerContext);
        LOGGER.debug("Create gzip producer");
    }

    public GzipDomeProducer(Topic topic, InputStream inputStream) {
        super(topic, inputStream);
    }

    protected long processStream(InputStream inputStream) throws IOException {

        try (GZIPInputStream gzipInputStream = new GZIPInputStream(inputStream)) {
            return super.processStream(gzipInputStream);
        }
    }
}
