package com.francetelecom.dome.consumer.standalone;

import com.francetelecom.dome.utils.configuration.Configurable;
import com.francetelecom.dome.utils.configuration.ConfigurableFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * User: eduard.cojocaru
 * Date: 10/31/13
 */
public class ConsumerRunner {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerRunner.class);

    public static void main(String[] args) throws Exception {

        String configurationPath = null;
        if (args != null && args.length == 1) {
            configurationPath = args[0];
            // TODO in other cases it may throw an exception
        }
        Configurable configurable = ConfigurableFactory.getConfigurable(configurationPath);

        final ExecutorService executorService = Executors.newFixedThreadPool(4);

        executorService.submit(new ConsumerManager("real-topic-2p1r", 1, configurable));
    }
}
