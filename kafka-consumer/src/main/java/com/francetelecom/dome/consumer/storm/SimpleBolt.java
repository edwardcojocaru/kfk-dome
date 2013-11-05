package com.francetelecom.dome.consumer.storm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
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
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        String fileName = "boltOutput" + hashCode() + ".log";

        try {
            Path path = Paths.get("/tmp/bolt/" + fileName);
            writer = Files.newBufferedWriter(path, ENCODING);
        } catch (IOException e) {
            LOGGER.error("File creation ex.", e);
        }
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
