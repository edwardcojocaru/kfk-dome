package com.edw.kafka.consumer.sync;

import com.edw.kafka.consumer.exception.NotCreatedException;
import com.edw.kafka.consumer.storm.KafkaConsumer;
import com.edw.kafka.consumer.storm.TopicEnum;
import kafka.consumer.KafkaStream;

import java.util.Map;
import java.util.concurrent.BlockingQueue;

/**
 * User: Eduard.Cojocaru
 * Date: 1/15/14
 */
public class ConsumerFactoryMock extends ConsumerFactory {

    private Map<TopicEnum, String> topicsMap;
    private KafkaStream<byte[], byte[]> kafkaStream;

    public ConsumerFactoryMock(Map<TopicEnum, String> topicsMap, KafkaStream<byte[], byte[]> kafkaStream) {
        super(null);

        this.topicsMap = topicsMap;
        this.kafkaStream = kafkaStream;
    }

    @Override
    public KafkaConsumer createConsumer(String topic, BlockingQueue<String> queue) throws NotCreatedException {
        return new ConsumerMock(topic, queue, topicsMap, kafkaStream);
    }

    @Override
    public KafkaConsumer createConsumer(String topic) throws NotCreatedException {
        return new ConsumerMock(topic, topicsMap, kafkaStream);
    }
}
