package com.francetelecom.dome.consumer.storm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import com.francetelecom.dome.consumer.utils.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

/**
 * User: Eduard.Cojocaru
 * Date: 11/4/13
 */
public class SimpleBolt implements IRichBolt {
    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleBolt.class);

    final static Charset ENCODING = StandardCharsets.UTF_8;

    private Fields fields = new Fields("word");

    private BufferedWriter writer;

    @Override
    public void prepare(Map configurationMap, TopologyContext topologyContext, OutputCollector outputCollector) {
        String fileName = "boltOutput" + hashCode() + ".log";

        LOGGER.info("Configuration: {}", configurationMap);

        try {
            Path path = getFilePath(fileName, this.<String, Object>cast(configurationMap));

            LOGGER.info("Create output file {}", path);
            writer = Files.newBufferedWriter(path, ENCODING);
        } catch (IOException e) {
            LOGGER.error("File creation ex.", e);
        }
    }

    private Path getFilePath(String fileName, Map<String, Object> configProperties) throws IOException {
        LOGGER.debug("create directories if not existent.");
        String directoryPathName = (String)configProperties.get("bolt.output.directory");

        final boolean useDefault = Boolean.parseBoolean((String) configProperties.get("use.default.bolt.output.directory"));
        if (useDefault) {
            if (hasLength(directoryPathName)) {
                LOGGER.warn("Bolt output directory not provided. Using default: '/tmp/bolt/'");
                directoryPathName = "/tmp/bolt/";
            }
        } else {
            if (hasLength(directoryPathName)) {
                LOGGER.error("'bolt.output.directory' not provided");
                throw new IllegalArgumentException("'bolt.output.directory' not provided");
            }
        }

        final Path directoryPath = Paths.get(directoryPathName);
        if (Files.notExists(directoryPath)) {
            Files.createDirectories(directoryPath);
            LOGGER.debug("Created directories for {}" + directoryPath);
        }

        directoryPathName = directoryPath.toString();
        final String separator = FileSystems.getDefault().getSeparator();
        directoryPathName = directoryPath + ((directoryPathName.endsWith(separator))?  separator : "");

        return Paths.get(directoryPathName + fileName);
    }

    private boolean hasLength(String directoryPathName) {
        return directoryPathName == null || directoryPathName.isEmpty();
    }

    @SuppressWarnings("unchecked")
    private <K,V> Map<K, V> cast(Map configurationMap) {
        return (Map<K, V>) configurationMap.get(Constants.KAFKA_CONFIG_KEY);
    }

    @Override
    public void execute(Tuple tuple) {
        final List<String> values = (List<String>) tuple.getValue(0);
        if (values != null && !values.isEmpty()) {
            try {
                for (String line : values) {
                    writer.write(line);
                    writer.newLine();
                    LOGGER.info(line);
                }
                writer.flush();
                LOGGER.info("\n");
            } catch (IOException ex) {
                LOGGER.error("Error writing file.", ex);
            }
        }
    }

    @Override
    public void cleanup() {
        try {
            writer.close();
        } catch (IOException e) {
            LOGGER.error("Error closing file.", e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(fields);
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }
}
