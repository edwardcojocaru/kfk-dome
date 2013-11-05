package com.francetelecom.dome.consumer.standalone;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;

/**
 * User: Eduard.Cojocaru
 * Date: 11/4/13
 */
public class ConsumeWorker implements Callable<String> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumeWorker.class);

    private final KafkaStream<byte[], byte[]> kafkaStream;

    public ConsumeWorker(KafkaStream<byte[], byte[]> kafkaStream) {
        this.kafkaStream = kafkaStream;
    }

    @Override
    public String call() throws Exception {

        if (kafkaStream != null) {
//            final Iterable<MessageAndMetadata<byte[],byte[]>> take = kafkaStream.take(200);
            ConsumerIterator<byte[], byte[]> it = kafkaStream.iterator();

            LOGGER.info("Waiting messages...");
            while (it.hasNext()) {
                LOGGER.info(new String(it.next().message()));
            }
            Thread.sleep(1000);
        }
        return "Done";
    }
}
