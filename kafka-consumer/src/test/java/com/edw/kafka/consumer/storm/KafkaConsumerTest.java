package com.edw.kafka.consumer.storm;

import com.edw.kafka.consumer.utils.Constants;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static junit.framework.Assert.assertNotNull;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import static org.powermock.api.mockito.PowerMockito.when;

/**
 * User: eduard.cojocaru
 * Date: 12/18/13
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({KafkaConsumer.class, KafkaStream.class, ConsumerIterator.class, MessageAndMetadata.class, BlockingQueue.class})
public class KafkaConsumerTest {

    private KafkaStream<byte[], byte[]> kafkaStream;

    @Before
    public void setUp() throws Exception {

        kafkaStream = mock(KafkaStream.class);
    }

    @After
    public void tearDown() throws Exception {
        verify(kafkaStream).iterator();
    }

    @Test
    public void testConstructorWithStream() throws Exception {

        final KafkaConsumer kafkaConsumer = new KafkaConsumer(kafkaStream);
        assertTrue(kafkaConsumer.getWorking().get());
        assertNotNull(kafkaConsumer.getQueue());
        assertEquals(Constants.BATCH_SIZE, kafkaConsumer.getQueue().remainingCapacity());
    }

    @Test
    public void testConstructorWithStreamAndQueue() throws Exception {

        final BlockingQueue<String> queue = new ArrayBlockingQueue<>(3);

        final KafkaConsumer kafkaConsumer = new KafkaConsumer(queue, kafkaStream);
        assertTrue(kafkaConsumer.getWorking().get());
        assertNotNull(kafkaConsumer.getQueue());
        assertTrue(queue == kafkaConsumer.getQueue());
    }

    @Test
    public void testGetQueue() throws Exception {
        final AtomicBoolean working = new AtomicBoolean(false);
        final ArrayBlockingQueue<String> queue = new ArrayBlockingQueue<>(3);
        new KafkaConsumer(queue, kafkaStream, working);
    }

    @Test
    public void testGetWorking() throws Exception {
        final AtomicBoolean working = new AtomicBoolean(false);
        final KafkaConsumer kafkaConsumer = new KafkaConsumer(null, kafkaStream, working);
        assertTrue(working == kafkaConsumer.getWorking());
    }

    @Test
    public void testRun() throws Exception {

        final MessageAndMetadata messageAndMetadata = mock(MessageAndMetadata.class);
        when(messageAndMetadata.message()).thenReturn(new String("this is a message").getBytes());

        final ConsumerIterator iterator = mock(ConsumerIterator.class);
        when(iterator.hasNext()).thenReturn(true, false);
        when(iterator.next()).thenReturn(messageAndMetadata);

        when(kafkaStream.iterator()).thenReturn(iterator);

        final KafkaConsumer kafkaConsumer = new KafkaConsumer(kafkaStream);

        kafkaConsumer.run();

        final BlockingQueue<String> queue = kafkaConsumer.getQueue();

        assertEquals("this is a message", queue.take());
        assertTrue(queue.isEmpty());

        verify(iterator, times(2)).hasNext();
        verify(iterator).next();
        verify(messageAndMetadata).message();
    }

    @Test
    public void testRunThread() throws Exception {

        final MessageAndMetadata messageAndMetadata = mock(MessageAndMetadata.class);
        when(messageAndMetadata.message()).thenReturn(new String("this is a message").getBytes());

        final ConsumerIterator iterator = mock(ConsumerIterator.class);
        when(iterator.hasNext()).thenReturn(true, false);
        when(iterator.next()).thenReturn(messageAndMetadata);

        when(kafkaStream.iterator()).thenReturn(iterator);

        final KafkaConsumer kafkaConsumer = new KafkaConsumer(kafkaStream);

        kafkaConsumer.start();
        kafkaConsumer.join();
        TimeUnit.SECONDS.sleep(1);

        final BlockingQueue<String> queue = kafkaConsumer.getQueue();

        assertEquals("this is a message", queue.take());
        assertTrue(queue.isEmpty());

        verify(iterator, times(2)).hasNext();
        verify(iterator).next();
        verify(messageAndMetadata).message();
    }

    @Test
    public void testRunWithRuntimeException() throws Exception {


        final ConsumerIterator iterator = mock(ConsumerIterator.class);
        when(iterator.hasNext()).thenReturn(true, false);
        when(iterator.next()).thenThrow(new IllegalArgumentException());

        when(kafkaStream.iterator()).thenReturn(iterator);

        final KafkaConsumer kafkaConsumer = new KafkaConsumer(kafkaStream);

        kafkaConsumer.run();

        final BlockingQueue<String> queue = kafkaConsumer.getQueue();

        assertTrue(queue.isEmpty());
        assertFalse(kafkaConsumer.getWorking().get());

        verify(iterator).hasNext();
        verify(iterator).next();
    }

    @Test
    public void testRunWithInterruptedException() throws Exception {

        final MessageAndMetadata messageAndMetadata = mock(MessageAndMetadata.class);
        when(messageAndMetadata.message()).thenReturn(new String("this is a message").getBytes());

        final ConsumerIterator iterator = mock(ConsumerIterator.class);
        when(iterator.hasNext()).thenReturn(true, false);
        when(iterator.next()).thenReturn(messageAndMetadata);

        when(kafkaStream.iterator()).thenReturn(iterator);

        final BlockingQueue<String> queue = mock(BlockingQueue.class);
        when(queue, "put", eq("this is a message")).thenThrow(new InterruptedException());

        final KafkaConsumer kafkaConsumer = new KafkaConsumer(queue, kafkaStream);

        kafkaConsumer.run();

        assertFalse(kafkaConsumer.getWorking().get());

        verify(iterator).hasNext();
        verify(iterator).next();
        verify(messageAndMetadata).message();
        verify(queue).put(eq("this is a message"));
    }

}
