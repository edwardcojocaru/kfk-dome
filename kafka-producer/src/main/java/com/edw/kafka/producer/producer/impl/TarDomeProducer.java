package com.edw.kafka.producer.producer.impl;

import kafka.producer.KeyedMessage;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * User: eduard.cojocaru
 * Date: 10/29/13
 */
public class TarDomeProducer extends AbstractDomeProducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(TarDomeProducer.class);

    public TarDomeProducer(ProducerContext producerContext) {
        super(producerContext);
        LOGGER.debug("Create tar producer");
    }

    protected long processStream(InputStream inputStream) throws IOException {

        long counter = 0;

        final List<BufferedReader> readersList = new ArrayList<>();

        try (TarArchiveInputStream tarArchiveInputStream = new TarArchiveInputStream(new BufferedInputStream(inputStream))) {

            TarArchiveEntry tarEntry = tarArchiveInputStream.getNextTarEntry();

            final String topicName = topic.getName();

            while (tarEntry != null) {

                LOGGER.info("processing {}", tarEntry.getName());
                if (tarEntry.isFile()) {

                    final String name = tarEntry.getName();

                    if (name.endsWith(".tgz") || name.endsWith(".tar.gz")) {
                        try (TarArchiveInputStream innerTarArchive = new TarArchiveInputStream(new BufferedInputStream(new GzipCompressorInputStream(tarArchiveInputStream)))) {

                            TarArchiveEntry innerTarEntry = innerTarArchive.getNextTarEntry();

                            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(tarArchiveInputStream));
                            readersList.add(bufferedReader);

                            while (innerTarEntry != null) {

                                LOGGER.info("processing: {} -> {}", name, innerTarEntry.getName());
                                if (innerTarEntry.isFile()) {
                                    String line;
                                    while ((line = bufferedReader.readLine()) != null) {
                                        producer.send(new KeyedMessage<String, String>(topicName, line));
                                        counter++;
                                    }
                                }
                                innerTarEntry = innerTarArchive.getNextTarEntry();
                            }
                        }
                    } else if (name.endsWith("gz")) {
                        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new GzipCompressorInputStream(tarArchiveInputStream)));
                        sendLines(readersList, topicName, bufferedReader);
                    } else if (name.endsWith(".csv")) {
                        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(tarArchiveInputStream));
                        sendLines(readersList, topicName, bufferedReader);
                    }
                }

                tarEntry = tarArchiveInputStream.getNextTarEntry();
            }
        } finally {
            producer.close();
            for (BufferedReader reader : readersList) {
                reader.close();
            }
        }

        return counter;
    }

    private void sendLines(List<BufferedReader> readersList, String topicName, BufferedReader bufferedReader) throws IOException {
        readersList.add(bufferedReader);

        String line;
        while ((line = bufferedReader.readLine()) != null) {
            producer.send(new KeyedMessage<String, String>(topicName, line));
        }
    }
}
