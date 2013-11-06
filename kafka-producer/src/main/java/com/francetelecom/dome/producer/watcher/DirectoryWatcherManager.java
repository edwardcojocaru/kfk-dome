package com.francetelecom.dome.producer.watcher;

import com.francetelecom.dome.beans.Configuration;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created with IntelliJ IDEA.
 * User: ecojocaru
 * Date: 11/5/13
 * Time: 11:01 PM
 * To change this template use File | Settings | File Templates.
 */
public class DirectoryWatcherManager {


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

        if (configuration.hasDirectoryToWatch()) {
            executor.submit(new DirectoryWatcher(configuration.getWatchedDirectory(), workerExecutor, configuration));
        }
    }

    // TODO add method for a list of directories
}
