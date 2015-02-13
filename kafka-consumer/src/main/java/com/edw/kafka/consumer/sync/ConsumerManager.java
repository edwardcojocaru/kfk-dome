package com.edw.kafka.consumer.sync;

import com.edw.kafka.consumer.exception.NotCreatedException;
import com.edw.kafka.consumer.storm.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * User: eduard.cojocaru
 * Date: 12/17/13
 */
public class ConsumerManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerManager.class);

    private ExecutorService executor;

    private Collection<String> topics;

    private final ConsumerFactory consumerFactory;

    private final Map<String, AtomicBoolean> workingMap = new HashMap<>();

    private final Map<String, BlockingQueue<String>> workingQueues = new HashMap<>();

    public ConsumerManager(Collection<String> topics, ConsumerFactory consumerFactory) {
        this.topics = topics;
        this.consumerFactory = consumerFactory;

        final int poolCapacity = topics.size();
        LOGGER.debug("Creating thread pool with {} elements", poolCapacity);
        executor = Executors.newFixedThreadPool(poolCapacity);

        init();

        new ManagerWorker().start();

        boolean allWorking = false;
        while (!allWorking) {
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                // do nothing or may be count not to exeed a limit
            }
            allWorking = true;
            for (AtomicBoolean working : workingMap.values()) {
                allWorking = allWorking && working.get();
            }
        }
    }

    private void init() {
        LOGGER.debug("Init workers");
        for (String topic : topics) {
            workingMap.put(topic, new AtomicBoolean(false));
        }
    }

    private KafkaConsumer getConsumer(String topic) throws NotCreatedException {
        KafkaConsumer consumer = getKafkaConsumer(topic);

        this.workingMap.put(topic, consumer.getWorking());
        workingQueues.put(topic, consumer.getQueue());
        return consumer;
    }

    private KafkaConsumer getKafkaConsumer(String topic) throws NotCreatedException {
        KafkaConsumer consumer;
        if (workingQueues.containsKey(topic)) {
            consumer = consumerFactory.createConsumer(topic, workingQueues.get(topic));
        } else {
            consumer = consumerFactory.createConsumer(topic);
        }

        return consumer;
    }

    private void keepConsumersAlive() {
        for (Map.Entry<String, AtomicBoolean> entry : workingMap.entrySet()) {
            if (!entry.getValue().get()) {
                final String topic = entry.getKey();
                LOGGER.info("Creating consumer for topic {}", topic);
                try {
                    executor.submit(getConsumer(topic));
                } catch (NotCreatedException e) {
                    LOGGER.error("Could not create consumer.", e);
                    try {
                        TimeUnit.SECONDS.sleep(3);
                    } catch (InterruptedException e1) {
                        break;
                    }
                }
            }
        }
    }

    private class ManagerWorker extends Thread {

        private ManagerWorker() {
            setDaemon(true);
        }

        @Override
        public void run() {
            LOGGER.info("ManagerWorker started.");
            while (true) {
                keepConsumersAlive();
            }
        }
    }

    public Map<String, BlockingQueue<String>> getWorkingQueues() {
        return workingQueues;
    }
}
