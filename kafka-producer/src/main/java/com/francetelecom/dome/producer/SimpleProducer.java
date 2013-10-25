package com.francetelecom.dome.producer;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
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


    public SimpleProducer(String topic) {

        properties.put("serializer.class", "kafka.serializer.StringEncoder");
        properties.put("zk.connect", "172.16.198.148:2181");
        properties.put("metadata.broker.list", "172.16.198.148:9092,172.16.198.148:9093");
        properties.put("producer.type", "async");
        properties.put("request.required.acks", "1");   // leader replica has received the data
        properties.put("batch.num.messages", "1000");

        this.producer = new Producer<String, String>(new ProducerConfig(properties));
        this.topic = topic;
    }

    @Override
    public void run() {

        final long initialTime = System.currentTimeMillis();
        GZIPInputStream gzipInputStream = null;
        FileInputStream archiveStream = null;

        try {

            archiveStream = new FileInputStream(new File("c:/temp/myfileforkafka.csv.gz"));

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
        double time = (System.currentTimeMillis() - initialTime) / 1000;
        LOGGER.info("Time to process: " + time);
    }
}
