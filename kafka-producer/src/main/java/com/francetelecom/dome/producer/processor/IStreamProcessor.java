package com.francetelecom.dome.producer.processor;

import java.nio.file.Path;

/**
 * User: Eduard.Cojocaru
 * Date: 11/8/13
 */
public interface IStreamProcessor {

    void process(Path filePath);
}
