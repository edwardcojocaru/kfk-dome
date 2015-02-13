package com.edw.kafka.consumer.storm;

import com.edw.kafka.consumer.utils.Constants;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class KafkaConsumer extends Thread {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConsumer.class);
    private static final Charset ENCODING = StandardCharsets.UTF_8;

    private BlockingQueue<String> queue;
    private AtomicBoolean working;
    private ConsumerIterator<byte[], byte[]> consumerIterator;

    public KafkaConsumer(BlockingQueue<String> queue, KafkaStream<byte[], byte[]> kafkaStream, AtomicBoolean working) {

        if (queue == null) {
            queue = new ArrayBlockingQueue<>(Constants.BATCH_SIZE);
        }
        this.queue = queue;

        if (working == null) {
            working = new AtomicBoolean(true);
        }
        this.working = working;

        this.consumerIterator = kafkaStream.iterator();
        setDaemon(true);
    }

    public KafkaConsumer(KafkaStream<byte[], byte[]> kafkaStream) {
        this(null, kafkaStream, null);
    }

    public KafkaConsumer(BlockingQueue<String> queue, KafkaStream<byte[], byte[]> kafkaStream) {
        this(queue, kafkaStream, null);
    }

    @Override
    public void run() {
        LOGGER.debug("Start receiving messages from kafka.");
        try {
            while (consumerIterator.hasNext()) {

                final String line = new String(consumerIterator.next().message(), ENCODING);
                try {
                    queue.put(line);
                } catch (InterruptedException e) {
                    LOGGER.error("Entry lost due to thread interruption. Line: " + line, e);
                    working.set(false);
                    break;
                }
            }
        } catch (RuntimeException ex) {
            working.set(false);
        }
    }

    public BlockingQueue<String> getQueue() {
        return queue;
    }

    public AtomicBoolean getWorking() {
        return working;
    }
}