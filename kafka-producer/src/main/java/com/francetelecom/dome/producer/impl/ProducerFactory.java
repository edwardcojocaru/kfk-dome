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

        if (Constants.GZIP_FILE.equals(fileType) && (fileName.endsWith(".tgz") || fileName.endsWith(".tar.gz"))) {
            return new TgzDomeProducer(context);
        } else if (Constants.GZIP_FILE.equals(fileType) && fileName.endsWith(".tgz")) {
            return new GzipDomeProducer(context);
        } else if (Constants.TAR_FILE.equals(fileType)) {
            return new TarDomeProducer(context);
        } else if (Constants.PLAIN_TEXT_FILE.equals(fileType)){
            return new StreamDomeProducer(context);
        } else {
            throw new IllegalArgumentException("Unsupported file type");
        }
    }
}
