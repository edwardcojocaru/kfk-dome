package com.francetelecom.dome.producer.watcher;

import com.francetelecom.dome.beans.Configuration;
import com.francetelecom.dome.beans.Topic;
import com.francetelecom.dome.producer.DomeProducer;
import com.francetelecom.dome.producer.ProducerRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.*;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.GZIPInputStream;

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.OVERFLOW;

/**
 * User: eduard.cojocaru
 * Date: 10/30/13
 */
public class DirectoryWatcher implements Callable<String> {

    private static final Logger LOGGER = LoggerFactory.getLogger(DirectoryWatcher.class);
    public static final String GZIP_FILE = "application/x-gzip";

    private final ProducerRunner producerRunner;
    private Configuration configuration;

    private final WatchService watcher;
    private final Path watchedDirectory;

    private AtomicBoolean watching = new AtomicBoolean(Boolean.TRUE);

    public DirectoryWatcher(String path, ProducerRunner producerRunner, Configuration configuration) throws IOException {
        this.producerRunner = producerRunner;
        this.configuration = configuration;
        this.watcher = FileSystems.getDefault().newWatchService();


        watchedDirectory = Paths.get(path);
        if (Files.notExists(watchedDirectory)) {
            Files.createDirectories(watchedDirectory);
        }

        watchedDirectory.register(watcher, ENTRY_CREATE);
        LOGGER.info("Directory registered to watch: " + watchedDirectory);
    }


    @Override
    public String call() throws Exception {

        while (watching.get()) {

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
                    final String fileType = Files.probeContentType(child);
                    if (!(fileType.equals(GZIP_FILE))) {
                        LOGGER.warn("Unsupported file type. File name {}. File type: {}", child, fileType);
                        continue;
                    }
                } catch (IOException x) {
                    LOGGER.error("Error reading file.", x);
                    continue;
                }

                final Topic topic = configuration.getTopic(child.getFileName().toString());
                if (topic != null) {
                    LOGGER.info("Register job for {}", child);
                    final GZIPInputStream gzipInputStream;

                    try {
                        gzipInputStream = new GZIPInputStream(Files.newInputStream(child));
                    } catch (IOException e) {
                        LOGGER.error("Could not open file.", e);
                        continue;
                    }

                    producerRunner.submitProducer(new DomeProducer(topic, gzipInputStream));
                } else  {
                    LOGGER.info("No topic file prefix match current file.");
                }
            }

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

    public void stopWatching() {
        this.watching.set(Boolean.FALSE);
        try {
            watcher.close();
        } catch (IOException e) {
            LOGGER.warn("Watcher may be already closed.");
        }
    }
}
