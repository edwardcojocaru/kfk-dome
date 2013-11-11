package com.francetelecom.dome.producer.impl;

import com.francetelecom.dome.beans.Topic;
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
    }

    public GzipDomeProducer(Topic topic, InputStream inputStream) {
        super(topic, inputStream);
    }

    protected void processStream(InputStream inputStream) throws IOException {

        try (GZIPInputStream gzipInputStream = new GZIPInputStream(inputStream)) {
            super.processStream(gzipInputStream);
        }
    }
}
