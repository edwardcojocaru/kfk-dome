package com.francetelecom.dome.producer;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Properties;
import java.util.zip.GZIPInputStream;

/**
 * User: Eduard.Cojocaru
 * Date: 10/24/13
 */
public class SimpleProducer extends Thread {

    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleProducer.class);

    private final Producer<String, String> producer;

    private final Properties properties = new Properties();

    private final String topic;

    private final String filePath;

    private static final String NUMBER_OF_MESSAGES_IN_BATCH = "500";

    public SimpleProducer(String topic, String filePath) {

        properties.put("serializer.class", "kafka.serializer.StringEncoder");
        properties.put("zk.connect", "172.16.198.148:2181");
        properties.put("metadata.broker.list", "172.16.198.148:9092,172.16.198.148:9093");
        properties.put("producer.type", "async");
        properties.put("request.required.acks", "0");   // leader replica has received the data
        properties.put("batch.num.messages", NUMBER_OF_MESSAGES_IN_BATCH);

        this.producer = new Producer<String, String>(new ProducerConfig(properties));
        this.topic = topic;
        this.filePath = filePath;
    }

    @Override
    public void run() {

        final long initialTime = System.nanoTime();
        GZIPInputStream gzipInputStream = null;
        FileInputStream archiveStream = null;

        try {

            archiveStream = new FileInputStream(new File(filePath));

            gzipInputStream = new GZIPInputStream(archiveStream);
            final BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(gzipInputStream));

            String line;
            while ((line = bufferedReader.readLine()) != null) {
                final KeyedMessage<String, String> something = new KeyedMessage<String, String>(topic, line);

                producer.send(something);
            }

        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } finally {
            producer.close();
            try {
                if (gzipInputStream != null) {
                    gzipInputStream.close();
                }
                if (archiveStream != null) {
                    archiveStream.close();
                }
            } catch (IOException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
        }
        final long endTime = System.nanoTime();

        NumberFormat formatter = new DecimalFormat("#0.0000");
        LOGGER.info("Execution time is " + formatter.format((endTime - initialTime) / (1000d * 1000d * 1000d)) + " seconds");
    }
}
