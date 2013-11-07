package com.francetelecom.dome.producer.watcher;

import org.apache.tika.Tika;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.spi.FileTypeDetector;

/**
 * User: eduard.cojocaru
 * Date: 11/07/13
 */
public class CustomFileTypeDetector extends FileTypeDetector {

    private final Tika tika = new Tika();

    @Override
    public String probeContentType(Path path) throws IOException {
        return tika.detect(path.toFile());
    }
}
