package com.francetelecom.dome.producer.watcher;

import com.francetelecom.dome.beans.Configuration;
import com.francetelecom.dome.beans.Topic;
import com.francetelecom.dome.producer.DomeProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.zip.GZIPInputStream;

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.OVERFLOW;

public class DirectoryWatcher implements Callable<String> {

    private static final Logger LOGGER = LoggerFactory.getLogger(DirectoryWatcher.class);
    public static final String GZIP_FILE = "application/x-gzip";

    private final ExecutorService executor;
    private Configuration configuration;

    private final WatchService watcher;
    private final Path watchedDirectory;

    private boolean watching = true;

    public DirectoryWatcher(String path, ExecutorService executor, Configuration configuration) throws IOException {
        this.executor = executor;
        this.configuration = configuration;
        this.watcher = FileSystems.getDefault().newWatchService();


        watchedDirectory = Paths.get(path);

        watchedDirectory.register(watcher, ENTRY_CREATE);
        LOGGER.info("Directory registered to watch: " + watchedDirectory);
    }


    @Override
    public String call() throws Exception {

        while (watching) {

            WatchKey key;
            try {
                key = watcher.take();
            } catch (InterruptedException x) {
                return "Interrupted";
            }

            for (WatchEvent<?> event: key.pollEvents()) {
                WatchEvent.Kind<?> kind = event.kind();

                if (kind == OVERFLOW) {
                    continue;
                }

                WatchEvent<Path> ev = cast(event);
                Path filename = ev.context();

                Path child;
                try {
                    child = watchedDirectory.resolve(filename);
                    LOGGER.info("Start processing file: {}.", child);
                    if (!Files.probeContentType(child).equals(GZIP_FILE)) {
                        LOGGER.warn(String.format("Unsupported file type. File name {}.", filename));
                        continue;
                    }
                } catch (IOException x) {
                    LOGGER.error("Error reading file.", x);
                    continue;
                }

                final Topic topic = configuration.getTopic(child.getFileName().toString());
                if (topic != null) {
                    LOGGER.info("Register job for {}", child);
                    final Future<String> submit = executor.submit(new DomeProducer(topic, new GZIPInputStream(Files.newInputStream(child))));
                    LOGGER.info(submit.get());
                } else  {
                    LOGGER.info("No topic file prefix match current file.");
                }
            }

            // Reset the key -- this step is critical if you want to
            // receive further watch events.  If the key is no longer valid,
            // the directory is inaccessible so exit the loop.
            boolean valid = key.reset();
            if (!valid) {
                LOGGER.info("Stopped watching directory.");
                break;
            }
        }

        return "done";
    }

    @SuppressWarnings("unchecked")
    private static <T> WatchEvent<T> cast(WatchEvent<?> event) {
        return (WatchEvent<T>)event;
    }

    // TODO add interruption method
}
