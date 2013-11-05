package com.francetelecom.dome.consumer.storm;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.BlockingQueue;

public class KafkaConsumer extends Thread {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConsumer.class);
    private static final Charset ENCODING = StandardCharsets.UTF_8;

    private BlockingQueue<String> queue;
    private ConsumerIterator<byte[], byte[]> consumerIterator;

    public KafkaConsumer(BlockingQueue<String> queue, KafkaStream<byte[], byte[]> kafkaStream) {
        this.queue = queue;
        this.consumerIterator = kafkaStream.iterator();
    }

    @Override
    public void run() {
        LOGGER.info("Start receiving messages from kafka.");
        while (consumerIterator.hasNext()) {

            final String line = new String(consumerIterator.next().message(), ENCODING);
            try {
                queue.put(line);
            } catch (InterruptedException e) {
                LOGGER.error("Entry lost due to thread interruption. Line: " + line, e);
                break;
            }
        }
    }
}