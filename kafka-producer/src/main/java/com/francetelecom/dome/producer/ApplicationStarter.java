package com.francetelecom.dome.producer;

import com.francetelecom.dome.ConfigInitializer;
import com.francetelecom.dome.beans.Configuration;
import com.francetelecom.dome.beans.Profile;
import com.francetelecom.dome.configuration.ConfigurableFactory;
import com.francetelecom.dome.producer.remote.PortListener;
import com.francetelecom.dome.producer.watcher.DirectoryWatcherManager;
import com.francetelecom.dome.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * User: eduard.cojocaru
 * Date: 10/30/13
 */
public class ApplicationStarter {

    private static final Logger LOGGER = LoggerFactory.getLogger(ApplicationStarter.class);

    private ProducerRunner producerRunner;

    private ExecutorService topicRunner;

    private Map<Profile, PortListener> listeners = new ConcurrentHashMap<>();

    private List<Future<String>> topicFutures = new ArrayList<>();

    private Configuration configuration;
    private DirectoryWatcherManager directoryWatcherManager;

    public static void main(String[] args) throws Exception {

        final ApplicationStarter applicationStarter = new ApplicationStarter();
        String configurationPath = null;
        if (args != null && args.length == 1) {
            configurationPath = args[0];
            // TODO in other cases it may throw an exception
        }

        applicationStarter.start(configurationPath);
        LOGGER.info("Application started.");

        Thread.sleep(10 * 1000);
        applicationStarter.stopProducing();
    }

    private void start(String configurationPath) throws IOException {
        LOGGER.info("Getting configuration...");
        this.configuration = new ConfigInitializer(ConfigurableFactory.getConfigurable(configurationPath)).getConfiguration();
        this.producerRunner = new ProducerRunner(this.configuration.getLiveCapacity());

        this.topicRunner = Executors.newFixedThreadPool(this.configuration.getListenerCapacity());

        for (Profile profile : this.configuration.getProfiles()) {
            final PortListener listener = new PortListener(profile, this.producerRunner);
            this.topicFutures.add(this.topicRunner.submit(listener));
            this.listeners.put(profile, listener);
        }

        directoryWatcherManager = new DirectoryWatcherManager(this.configuration);
        directoryWatcherManager.watch();

        for (Future<String> future : this.topicFutures) {
            try {
                final String result = future.get(5, TimeUnit.SECONDS);
                LOGGER.debug("Result: {}", result);
            } catch (TimeoutException timeoutException) {
                LOGGER.debug("Connection must be opened.");
            } catch (ExecutionException ex) {
                LOGGER.error("Exception occur for one port", ex);
            } catch (InterruptedException iex) {
                LOGGER.error("Thread interrupted.", iex);
            }
        }
    }

    public void stopProducing() {
        LOGGER.info("Stopping producer application...");

        boolean isExecutorTerminated = Utils.waitToStopExecutorManager(this.topicRunner);

        if (!isExecutorTerminated) {
            try {
                Thread.sleep(10 * 1000);
            } catch (InterruptedException e) {
                LOGGER.info("Thread interrupted.");
            }
        }

        producerRunner.initializeProducerTermination();
        directoryWatcherManager.stopWatching();
    }
}
