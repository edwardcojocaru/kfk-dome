package com.edw.kafka.producer.producer;


import com.edw.kafka.producer.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;

/**
 * User: Eduard.Cojocaru
 * Date: 10/25/13
 */
public class ProducerRunner {

    public static final Logger LOGGER = LoggerFactory.getLogger(ProducerRunner.class);

    private ExecutorService executor;

    public ProducerRunner(int numberOfThreads) {
        executor = Executors.newFixedThreadPool(numberOfThreads);
        LOGGER.info("Executor created. Capacity: {}", numberOfThreads);
    }

    public <T> Future<T> submitProducer(Callable<T> producer) {
        return executor.submit(producer);
    }

    public void initializeProducerTermination() {
        LOGGER.debug("Initialize producer executor termination.");
        Utils.waitToStopExecutorWorker(executor);
    }

}
