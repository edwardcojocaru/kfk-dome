package com.edw.kafka.consumer.sync;

import com.edw.kafka.consumer.storm.KafkaConsumer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;
import static org.powermock.api.mockito.PowerMockito.mock;

/**
 * User: eduard.cojocaru
 * Date: 12/17/13
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ConsumerManager.class, ConsumerFactory.class, KafkaConsumer.class})
public class ConsumerManagerTest {
    @Before
    public void setUp() throws Exception {

    }

    @Test
    public void testCreateManager() throws Exception {

        List<String> topics = new ArrayList<>();
        topics.add("a");
        topics.add("b");

        final ConsumerFactory consumerFactory = mock(ConsumerFactory.class);

        final KafkaConsumer kafkaConsumer = mock(KafkaConsumer.class);
        when(consumerFactory.createConsumer("a")).thenReturn(kafkaConsumer);
        when(consumerFactory.createConsumer("b")).thenReturn(kafkaConsumer);

        final ArrayBlockingQueue<String> queue = new ArrayBlockingQueue<>(1);
        when(kafkaConsumer.getQueue()).thenReturn(queue);
        when(kafkaConsumer.getWorking()).thenReturn(new AtomicBoolean(false), new AtomicBoolean(false), new AtomicBoolean(true));
        when(consumerFactory.createConsumer(eq("a"), any(BlockingQueue.class))).thenReturn(kafkaConsumer);
        when(consumerFactory.createConsumer(eq("b"), any(BlockingQueue.class))).thenReturn(kafkaConsumer);

        final ConsumerManager consumerManager = new ConsumerManager(topics, consumerFactory);

        TimeUnit.SECONDS.sleep(3);
        final Map<String, BlockingQueue<String>> workingQueues = consumerManager.getWorkingQueues();

        assertEquals(2, workingQueues.size());
        assertTrue(workingQueues.keySet().contains("a"));
        assertTrue(workingQueues.keySet().contains("b"));

        verify(consumerFactory).createConsumer("a");
        verify(consumerFactory).createConsumer("b");
        verify(consumerFactory).createConsumer(eq("a"), any(BlockingQueue.class));
        verify(consumerFactory).createConsumer(eq("b"), any(BlockingQueue.class));
        verify(kafkaConsumer, times(4)).getQueue();
        verify(kafkaConsumer, times(4)).getWorking();
    }


}
