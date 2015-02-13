package com.edw.kafka.producer.producer.impl;

import com.edw.kafka.producer.util.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * User: Eduard.Cojocaru
 * Date: 11/11/13
 */
public class ProducerFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProducerFactory.class);

    private ProducerFactory() {
    }

    public static DomeProducer getProducer(ProducerContext context) {
        final String fileType = context.getFileType();
        final String fileName = context.getFileName();

        if (Constants.GZIP_FILE.equals(fileType) && (fileName.endsWith(".tgz") || fileName.endsWith(".tar.gz"))) {
            return new TgzDomeProducer(context);
        } else if (Constants.GZIP_FILE.equals(fileType) && fileName.endsWith(".gz")) {
            return new GzipDomeProducer(context);
        } else if (Constants.TAR_FILE.equals(fileType)) {
            return new TarDomeProducer(context);
        } else if (Constants.PLAIN_TEXT_FILE.equals(fileType)) {
            return new StreamDomeProducer(context);
        } else {
            LOGGER.info("Unsupported file type: {}", fileType);

            throw new IllegalArgumentException("Unsupported file type");
        }
    }
}
