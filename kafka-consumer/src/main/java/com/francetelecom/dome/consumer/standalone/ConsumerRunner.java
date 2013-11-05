package com.francetelecom.dome.consumer.standalone;

import com.francetelecom.dome.consumer.KafkaConfigManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * User: eduard.cojocaru
 * Date: 10/31/13
 */
public class ConsumerRunner {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerRunner.class);

    public static void main(String[] args) throws Exception {

        InputStream inputStream = ConsumerRunner.class.getClassLoader().getResourceAsStream("consumer.properties");
        KafkaConfigManager.INSTANCE.init(inputStream);

        final ExecutorService executorService = Executors.newFixedThreadPool(4);

        executorService.submit(new ConsumerManager("real-topic-2p1r", 1));

//        LOGGER.info("Waiting...1");
//        Thread.sleep(20 * 1000);
//        LOGGER.info("Done waiting...1");
//
//        executorService.submit(new ConsumerManager("real-topic-2p1r", 2));
//
//        LOGGER.info("Waiting...2");
//        Thread.sleep(20 * 1000);
//        LOGGER.info("Done waiting...2");
//
//        executorService.submit(new ConsumerManager("real-topic-2p1r", 3));
//
//        LOGGER.info("Waiting...3");
//        Thread.sleep(20 * 1000);
//        LOGGER.info("Done waiting...3");
//
//        executorService.submit(new ConsumerManager("real-topic-2p1r", 4));

//        executorService.submit(new ConsumerManager("real-topic-1p2r", 4));

       /* LOGGER.info("Waiting...1");
        Thread.sleep(20 * 1000);
        LOGGER.info("Done waiting...1");

        executorService.submit(new ConsumerManager("real-topic-1p2r", 1));

        LOGGER.info("Waiting...2");
        Thread.sleep(10 * 1000);
        LOGGER.info("Done waiting...2");

        executorService.submit(new ConsumerManager("real-topic-1p2r", 1));*/


    }
}
