package com.edw.kafka.consumer.sync;

import com.edw.kafka.consumer.storm.KafkaConsumer;
import com.edw.kafka.consumer.storm.TopicEnum;
import kafka.consumer.KafkaStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * User: Eduard.Cojocaru
 * Date: 1/15/14
 */
public class ConsumerMock extends KafkaConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerMock.class);
    private Iterator consumerIterator;
    private String topic;
    private Map<TopicEnum, String> topicsMap;

    public ConsumerMock(BlockingQueue<String> queue, KafkaStream<byte[], byte[]> kafkaStream, AtomicBoolean working) {
        super(queue, kafkaStream, working);
    }

    public ConsumerMock(BlockingQueue<String> queue, KafkaStream<byte[], byte[]> kafkaStream, AtomicBoolean working, Iterator consumerIterator) {
        super(queue, kafkaStream, working);
        this.consumerIterator = consumerIterator;
    }

    public ConsumerMock(KafkaStream<byte[], byte[]> kafkaStream, Iterator consumerIterator) {
        super(kafkaStream);
        this.consumerIterator = consumerIterator;
    }

    public ConsumerMock(BlockingQueue<String> queue, KafkaStream<byte[], byte[]> kafkaStream, Iterator consumerIterator) {
        super(queue, kafkaStream);
        this.consumerIterator = consumerIterator;
    }

    public ConsumerMock(String topic, BlockingQueue<String> queue, Map<TopicEnum, String> topicsMap, KafkaStream<byte[], byte[]> kafkaStream) {
        super(queue, kafkaStream);
        this.topic = topic;
        this.topicsMap = topicsMap;
    }

    public ConsumerMock(String topic, Map<TopicEnum, String> topicsMap, KafkaStream<byte[], byte[]> kafkaStream) {
        super(kafkaStream);    //To change body of overridden methods use File | Settings | File Templates.
        this.topic = topic;
        this.topicsMap = topicsMap;
    }


    @Override
    public void run() {
        LOGGER.debug("Start receiving messages from kafka.");
        try {
            String fileName = topicsMap.get(TopicEnum.valueOf(topic));
            ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(contextClassLoader.getResourceAsStream(fileName)))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    try {
                        getQueue().put(line);
                    } catch (InterruptedException e) {
                        LOGGER.error("Entry lost due to thread interruption. Line: " + line, e);
                        getWorking().set(false);
                        break;
                    }
                }
            }
        } catch (Exception ex) {
            getWorking().set(false);
        }
    }


}
