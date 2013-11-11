package com.francetelecom.dome.producer.impl;

import com.francetelecom.dome.util.Constants;

/**
 * User: Eduard.Cojocaru
 * Date: 11/11/13
 */
public class ProducerFactory {

    private ProducerFactory() {
    }

    public static DomeProducer getProducer(ProducerContext context) {
        final String fileType = context.getFileType();
        final String fileName = context.getFileName();

        if (Constants.GZIP_FILE.equals(fileType) && fileName.endsWith(".gz")) {
            return new GzipDomeProducer(context);
        } else if (Constants.GZIP_FILE.equals(fileType) && fileName.endsWith(".tgz")) {
            return new TgzDomeProducer(context);
        } else {
            return new StreamDomeProducer(context);
        }
    }
}
