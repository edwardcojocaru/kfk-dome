package com.francetelecom.dome.producer.watcher;

import com.francetelecom.dome.beans.Configuration;
import com.francetelecom.dome.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * User: eduard.cojocaru
 * Date: 10/30/13
 */
public class DirectoryWatcherManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(DirectoryWatcherManager.class);

    public static final int WATCHED_DIRECTORIES_COUNT = 10;
    private final ExecutorService executor;
    private final ExecutorService workerExecutor;

    private Configuration configuration;

    public DirectoryWatcherManager(Configuration configuration) throws IOException {
        this.configuration = configuration;
        this.executor = Executors.newFixedThreadPool(WATCHED_DIRECTORIES_COUNT);
        this.workerExecutor = Executors.newFixedThreadPool(WATCHED_DIRECTORIES_COUNT);
    }

    public void watch() throws IOException {
        LOGGER.debug("Watch directories...");
        if (configuration.hasDirectoryToWatch()) {
            executor.submit(new DirectoryWatcher(configuration.getWatchedDirectory(), workerExecutor, configuration));
        }
    }

    public void stopWatching() {
        Utils.waitToStopExecutorWorker(workerExecutor);
        Utils.waitToStopExecutorManager(executor);
    }

}
