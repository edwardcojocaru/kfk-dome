package com.francetelecom.dome.consumer.standalone;

import com.francetelecom.dome.consumer.ConsumerConnectorManager;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * User: eduard.cojocaru
 * Date: 10/31/13
 */
public class ConsumerManager implements Callable<String> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerManager.class);

    private final ConsumerConnector consumer;
    private final ExecutorService executorService;
    private final String topic;
    private final int count;

    public ConsumerManager(String topic, int count) {
        this(topic, count, "MasterGroupXYZ");
    }

    private ConsumerManager(String topic, int count, String group) {
        this.consumer = ConsumerConnectorManager.getInstance().getConnector(group);

        this.executorService = Executors.newFixedThreadPool(count);
        this.topic = topic;
        this.count = count;
    }

    @Override
    public String call() throws Exception {

        Map<String, Integer> topicCountMap = new HashMap<>();
        topicCountMap.put(topic, count);

        LOGGER.info("Create message stream. Count: " + count);
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        for (KafkaStream<byte[], byte[]> stream : consumerMap.get(topic)) {
            LOGGER.info("new ConsumeWorker added.");
            executorService.submit(new ConsumeWorker(stream));
        }

        return "done";
    }
}
