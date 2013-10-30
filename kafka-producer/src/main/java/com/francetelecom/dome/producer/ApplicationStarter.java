package com.francetelecom.dome.producer;

import com.francetelecom.dome.ConfigInitializer;
import com.francetelecom.dome.beans.Configuration;
import com.francetelecom.dome.beans.Profile;
import com.francetelecom.dome.producer.remote.PortListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
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

    public static void main(String[] args) {

        new ApplicationStarter().start();
    }

    private void start() {
        LOGGER.info("Getting configuration...");
        this.configuration = new ConfigInitializer().getConfiguration();
        this.producerRunner = new ProducerRunner(this.configuration.getLiveCapacity());

        this.topicRunner = Executors.newFixedThreadPool(this.configuration.getListenerCapacity());

        for (Profile profile : this.configuration.getProfiles()) {
            final PortListener listener = new PortListener(profile, this.producerRunner);
            this.topicRunner.submit(listener);
            this.listeners.add(listener);
        }
    }
}
