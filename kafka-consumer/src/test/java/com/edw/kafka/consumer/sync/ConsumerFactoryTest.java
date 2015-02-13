package com.edw.kafka.consumer.sync;

import com.edw.kafka.consumer.exception.ConfigurationException;
import com.edw.kafka.consumer.exception.NotCreatedException;
import com.edw.kafka.consumer.storm.KafkaConsumer;
import com.edw.kafka.consumer.utils.Utils;
import kafka.consumer.KafkaStream;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.*;

/**
 * User: eduard.cojocaru
 * Date: 12/17/13
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ConsumerFactory.class, Utils.class, KafkaStream.class})
public class ConsumerFactoryTest {

    @Test(expected = ConfigurationException.class)
    public void testCreateConsumerNullTopic() throws Exception {
        new ConsumerFactory(new HashMap<String, Object>()).createConsumer(null);
    }

    @Test(expected = ConfigurationException.class)
    public void testCreateConsumerEmptyTopic() throws Exception {
        new ConsumerFactory(new HashMap<String, Object>()).createConsumer("");
    }

    @Test
    public void testCreateConsumerNotCreated() throws Exception {
        final HashMap<String, Object> kafkaConfig = new HashMap<>();
        final ConsumerFactory consumerFactory = new ConsumerFactory(kafkaConfig);

        mockStatic(Utils.class);
        when(Utils.getKafkaStream(kafkaConfig, "topic")).thenReturn(Collections.<KafkaStream<byte[], byte[]>>emptyList());
        when(Utils.getKafkaStream(kafkaConfig, "topicEx")).thenThrow(new RuntimeException("something"));

        try {
            consumerFactory.createConsumer("topic");
            fail("Expected NotCreatedException.");
        } catch (NotCreatedException e) {
        }

        try {
            consumerFactory.createConsumer("topicEx");
            fail("Expected NotCreatedException.");
        } catch (NotCreatedException e) {
        }

        verifyStatic();
        Utils.getKafkaStream(kafkaConfig, "topic");

        verifyStatic();
        Utils.getKafkaStream(kafkaConfig, "topicEx");
    }

    @Test
    public void testCreateConsumer() throws Exception {
        final HashMap<String, Object> kafkaConfig = new HashMap<>();
        final ConsumerFactory consumerFactory = new ConsumerFactory(kafkaConfig);


        final List<KafkaStream<byte[], byte[]>> value = new ArrayList<>();
        final KafkaStream kafkaStream = mock(KafkaStream.class);
        value.add(kafkaStream);

        mockStatic(Utils.class);
        when(Utils.getKafkaStream(kafkaConfig, "topic")).thenReturn(value);

        final KafkaConsumer kafkaConsumer = mock(KafkaConsumer.class);
        whenNew(constructor(KafkaConsumer.class, KafkaStream.class)).withArguments(kafkaStream).thenReturn(kafkaConsumer);

        KafkaConsumer actualConsumer = consumerFactory.createConsumer("topic");

        assertEquals(kafkaConsumer, actualConsumer);

        verifyNew(KafkaConsumer.class).withArguments(kafkaStream);
    }

    @Test
    public void testCreateConsumerNotCreatedWithQueue() throws Exception {
        final HashMap<String, Object> kafkaConfig = new HashMap<>();
        final ConsumerFactory consumerFactory = new ConsumerFactory(kafkaConfig);

        mockStatic(Utils.class);
        when(Utils.getKafkaStream(kafkaConfig, "topic")).thenReturn(Collections.<KafkaStream<byte[], byte[]>>emptyList());
        when(Utils.getKafkaStream(kafkaConfig, "topicEx")).thenThrow(new RuntimeException("something"));

        try {
            consumerFactory.createConsumer("topic", null);
            fail("Expected NotCreatedException.");
        } catch (NotCreatedException e) {
        }

        try {
            consumerFactory.createConsumer("topicEx", null);
            fail("Expected NotCreatedException.");
        } catch (NotCreatedException e) {
        }

        verifyStatic();
        Utils.getKafkaStream(kafkaConfig, "topic");

        verifyStatic();
        Utils.getKafkaStream(kafkaConfig, "topicEx");
    }

    @Test(expected = ConfigurationException.class)
    public void testCreateConsumerNullTopicNoQueue() throws Exception {
        new ConsumerFactory(new HashMap<String, Object>()).createConsumer(null, null);
    }

    @Test(expected = ConfigurationException.class)
    public void testCreateConsumerEmptyTopicNoQueue() throws Exception {
        new ConsumerFactory(new HashMap<String, Object>()).createConsumer("", null);
    }

    @Test
    public void testCreateConsumerWithQueue() throws Exception {
        final HashMap<String, Object> kafkaConfig = new HashMap<>();
        final ConsumerFactory consumerFactory = new ConsumerFactory(kafkaConfig);


        final List<KafkaStream<byte[], byte[]>> value = new ArrayList<>();
        final KafkaStream kafkaStream = mock(KafkaStream.class);
        value.add(kafkaStream);

        mockStatic(Utils.class);
        when(Utils.getKafkaStream(kafkaConfig, "topic")).thenReturn(value);

        final ArrayBlockingQueue<String> queue = new ArrayBlockingQueue<>(3);
        final KafkaConsumer kafkaConsumer = mock(KafkaConsumer.class);
        whenNew(constructor(KafkaConsumer.class, BlockingQueue.class, KafkaStream.class))
                .withArguments(queue, kafkaStream).thenReturn(kafkaConsumer);

        KafkaConsumer actualConsumer = consumerFactory.createConsumer("topic", queue);

        assertEquals(kafkaConsumer, actualConsumer);

        verifyNew(KafkaConsumer.class).withArguments(queue, kafkaStream);

    }
}
