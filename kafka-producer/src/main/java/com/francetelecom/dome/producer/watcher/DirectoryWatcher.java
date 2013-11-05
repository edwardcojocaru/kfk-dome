package com.francetelecom.dome.producer.watcher;

import com.francetelecom.dome.beans.Topic;
import com.francetelecom.dome.producer.DomeProducer;

import java.io.IOException;
import java.nio.file.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.zip.GZIPInputStream;

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.OVERFLOW;

/**
 * Created with IntelliJ IDEA.
 * User: ecojocaru
 * Date: 11/5/13
 * Time: 11:11 PM
 * To change this template use File | Settings | File Templates.
 */
public class DirectoryWatcher implements Callable<String> {

    private final ExecutorService executor;

    private final WatchService watcher;
    private final Path watchedDirectory;

    private boolean watching = true;

    public DirectoryWatcher(String path, ExecutorService executor) throws IOException {
        this.executor = executor;
        this.watcher = FileSystems.getDefault().newWatchService();


        watchedDirectory = Paths.get(path);

        watchedDirectory.register(watcher, ENTRY_CREATE);
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
                    // TODO check type gz
                    if (!Files.probeContentType(child).equals("text/plain")) {
                        System.err.format("Unsupported file type. File name %s.", filename);
                        continue;
                    }
                } catch (IOException x) {
                    System.err.println(x);
                    continue;
                }

                // TODO launch gzip producer
                //TODO add topic factory
                executor.submit(new DomeProducer(new Topic("a", "b"), new GZIPInputStream(Files.newInputStream(child))));

            }

            // Reset the key -- this step is critical if you want to
            // receive further watch events.  If the key is no longer valid,
            // the directory is inaccessible so exit the loop.
            boolean valid = key.reset();
            if (!valid) {
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
