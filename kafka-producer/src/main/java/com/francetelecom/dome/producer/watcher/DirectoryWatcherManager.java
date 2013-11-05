package com.francetelecom.dome.producer.watcher;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.WatchService;
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

    private final WatchService watcher;


    public DirectoryWatcherManager() throws IOException {
        this.executor = Executors.newFixedThreadPool(WATCHED_DIRECTORIES_COUNT);
        this.workerExecutor = Executors.newFixedThreadPool(WATCHED_DIRECTORIES_COUNT);
        this.watcher = FileSystems.getDefault().newWatchService();

    }

    public void watch(String watchedDirectoryPath) throws IOException {


        executor.submit(new DirectoryWatcher(watchedDirectoryPath, workerExecutor));
    }

    // TODO add method for a list of directories
}
