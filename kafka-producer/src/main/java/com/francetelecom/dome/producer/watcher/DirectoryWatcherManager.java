package com.francetelecom.dome.producer.watcher;

import com.francetelecom.dome.beans.Configuration;
import com.francetelecom.dome.producer.ProducerRunner;
import com.francetelecom.dome.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
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
    private final ProducerRunner producerRunner;
    private final List<DirectoryWatcher> watchers;

    private Configuration configuration;

    public DirectoryWatcherManager(Configuration configuration, ProducerRunner producerRunner) throws IOException {
        this.configuration = configuration;
        this.executor = Executors.newFixedThreadPool(WATCHED_DIRECTORIES_COUNT);
        this.producerRunner = producerRunner;
        this.watchers = new ArrayList<>();
    }

    public void watch() throws IOException {
        LOGGER.debug("Watch directories...");
        if (configuration.hasDirectoryToWatch()) {
            final DirectoryWatcher task = new DirectoryWatcher(configuration.getWatchedDirectory(), producerRunner, configuration);
            watchers.add(task);
            executor.submit(task);
        }
    }

    public void stopWatching() {
        for (DirectoryWatcher watcher : watchers) {
            watcher.stopWatching();
        }
        Utils.waitToStopExecutorManager(executor);
    }

}
