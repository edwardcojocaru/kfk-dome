package com.francetelecom.dome.producer;

import com.francetelecom.dome.ConfigInitializer;
import com.francetelecom.dome.beans.Configuration;
import com.francetelecom.dome.beans.Profile;
import com.francetelecom.dome.configuration.BundleConfiguration;
import com.francetelecom.dome.configuration.Configurable;
import com.francetelecom.dome.configuration.StreamConfiguration;
import com.francetelecom.dome.producer.remote.PortListener;
import com.francetelecom.dome.producer.watcher.DirectoryWatcherManager;
import com.francetelecom.dome.util.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.ResourceBundle;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * User: eduard.cojocaru
 * Date: 10/30/13
 */
public class ApplicationStarter {

    private static final Logger LOGGER = LoggerFactory.getLogger(ApplicationStarter.class);

    private ProducerRunner producerRunner;

    private ExecutorService topicRunner;

    private List<PortListener> listeners = new ArrayList<>();

    private Configuration configuration;

    public static void main(String[] args) throws Exception {

        final ApplicationStarter applicationStarter = new ApplicationStarter();
        String configurationPath = null;
        if (args != null && args.length == 1) {
            configurationPath = args[0];
            // TODO in other cases it may throw an exception
        }

        applicationStarter.start(configurationPath);
        LOGGER.info("Application started.");

        DirectoryWatcherManager watcher = new DirectoryWatcherManager();
        watcher.watch(applicationStarter.configuration.getWatchedDirectory());
    }

    private void start(String configurationPath) throws IOException {
        LOGGER.info("Getting configuration...");
        this.configuration = new ConfigInitializer(getConfigurable(configurationPath)).getConfiguration();
        this.producerRunner = new ProducerRunner(this.configuration.getLiveCapacity());

        this.topicRunner = Executors.newFixedThreadPool(this.configuration.getListenerCapacity());

        for (Profile profile : this.configuration.getProfiles()) {
            final PortListener listener = new PortListener(profile, this.producerRunner);
            this.topicRunner.submit(listener);
            this.listeners.add(listener);
        }
    }

    private Configurable getConfigurable(String configurationPath) throws IOException {
        if (configurationPath != null) {
            return new StreamConfiguration(new FileInputStream(configurationPath));
        } else {
            return new BundleConfiguration(ResourceBundle.getBundle(Constants.CONFIGURATION_FILE_BASE_NAME));
        }
    }
}
