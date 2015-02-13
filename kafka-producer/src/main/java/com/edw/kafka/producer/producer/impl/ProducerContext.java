package com.edw.kafka.producer.producer.impl;

import com.edw.kafka.producer.beans.Topic;
import com.edw.kafka.producer.util.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * User: Eduard.Cojocaru
 * Date: 11/11/13
 */
public class ProducerContext {
    private static final Logger LOGGER = LoggerFactory.getLogger(ProducerContext.class);

    private Path filePath;

    private String fileName;

    private String fileType;

    private Topic topic;

    private Map<String, Object> producerConfig;
    private InputStream inputStream;

    private static final Set<String> SUPPORTED_EXTENSIONS = new HashSet<>();
    private static final Set<String> SUPPORTED_FILE_TYPES = new HashSet<>();

    static {
        SUPPORTED_EXTENSIONS.add(".gz");
        SUPPORTED_EXTENSIONS.add(".tgz");
        SUPPORTED_EXTENSIONS.add(".tar.gz");

        SUPPORTED_FILE_TYPES.add(Constants.GZIP_FILE);
        SUPPORTED_FILE_TYPES.add(Constants.TAR_FILE);
        SUPPORTED_FILE_TYPES.add(Constants.PLAIN_TEXT_FILE);
    }

    public ProducerContext(String fileName, String fileType, Topic topic, InputStream inputStream, Map<String, Object> producerConfig) {
        this.fileName = fileName;
        this.fileType = fileType;
        this.topic = topic;
        this.inputStream = inputStream;
        this.producerConfig = producerConfig;
    }

    public ProducerContext(Topic topic, Map<String, Object> producerConfig, InputStream inputStream) {
        this.topic = topic;
        this.producerConfig = producerConfig;
        this.inputStream = inputStream;
    }

    public ProducerContext(Path child, Map<String, Object> producerConfig) throws IOException {

        this.filePath = child;
        this.producerConfig = producerConfig;
        this.fileType = Files.probeContentType(child);

        this.fileName = child.getFileName().toString();
        LOGGER.info("File '{}' is of type '{}'", this.fileName, this.fileType);
        LOGGER.debug("Additional config: ", this.producerConfig);
    }

    public Path getFilePath() {
        return filePath;
    }

    public void setFilePath(Path filePath) {
        this.filePath = filePath;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public String getFileType() {
        return fileType;
    }

    public void setFileType(String fileType) {
        this.fileType = fileType;
    }

    public Topic getTopic() {
        return topic;
    }

    public void setTopic(Topic topic) {
        this.topic = topic;
    }

    public Map<String, Object> getProducerConfig() {
        return producerConfig;
    }

    public void setProducerConfig(Map<String, Object> producerConfig) {
        this.producerConfig = producerConfig;
    }

    public InputStream getInputStream() {
        return inputStream;
    }

    public void setInputStream(InputStream inputStream) {
        this.inputStream = inputStream;
    }

    public boolean isUnsupportedFileType() {
        if (!SUPPORTED_FILE_TYPES.contains(fileType)) {
            return true;
        }

        if (Constants.GZIP_FILE.equals(fileType)) {
            for (String extension : SUPPORTED_EXTENSIONS) {
                if (fileName.endsWith(extension)) {
                    return false;
                }
            }
            return true;
        } else if (Constants.PLAIN_TEXT_FILE.equals(fileType) && !fileName.endsWith(".csv")) {
            return true;
        }

        return false;
    }
}
