package com.francetelecom.dome.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * User: Eduard.Cojocaru
 * Date: 11/6/13
 */
public class Utils {

    private static final Logger LOGGER = LoggerFactory.getLogger(Utils.class);

    public static boolean waitToStopExecutorManager(ExecutorService executorService) {
        return Utils.waitToStopExecutor(Constants.MANAGERS_TIMEOUT, Constants.MANAGERS_TIMEOUT_UNITS, executorService);
    }

    public static boolean waitToStopExecutorWorker(ExecutorService executorService) {
        return Utils.waitToStopExecutor(10, TimeUnit.MINUTES, executorService);
    }

    public static boolean waitToStopExecutor(int timeout, TimeUnit minutes, ExecutorService workerExecutor1) {

        boolean isExecutorTerminated = false;

        workerExecutor1.shutdown();
        try {
            isExecutorTerminated = workerExecutor1.awaitTermination(timeout, minutes);
        } catch (InterruptedException e) {
            LOGGER.info("Executor interrupted.");
        }

        return isExecutorTerminated;
    }
}
