package com.francetelecom.dome.producer.impl;

import kafka.producer.KeyedMessage;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

/**
 * User: eduard.cojocaru
 * Date: 10/29/13
 */
public class TgzDomeProducer extends AbstractDomeProducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(TgzDomeProducer.class);

    public TgzDomeProducer(ProducerContext producerContext) {
        super(producerContext);
    }

    protected void processStream(InputStream inputStream) throws IOException {
        final BufferedReader bufferedReader;

        try (TarArchiveInputStream tarArchiveInputStream = new TarArchiveInputStream(new BufferedInputStream(new GzipCompressorInputStream(inputStream)))) {

            TarArchiveEntry tarEntry = tarArchiveInputStream.getNextTarEntry();

            final String topicName = topic.getName();

            bufferedReader = new BufferedReader(new InputStreamReader(tarArchiveInputStream));
            while(tarEntry != null) {
                LOGGER.info("processing {}", tarEntry.getName());
                if (!tarEntry.isDirectory()) {
                    String line;
                    while ((line = bufferedReader.readLine()) != null) {
                        producer.send(new KeyedMessage<String, String>(topicName, line));
                    }
                }

                tarEntry = tarArchiveInputStream.getNextTarEntry();
            }
        } finally {
            producer.close();
        }
    }
}
