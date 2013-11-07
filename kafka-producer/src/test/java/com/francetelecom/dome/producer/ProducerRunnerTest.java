package com.francetelecom.dome.producer;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.*;

/**
 * User: eduard.cojocaru
 * Date: 10/29/13
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ProducerRunner.class, Executors.class, ExecutorService.class})
public class ProducerRunnerTest {

    private ExecutorService executor;
    private int numberOfThreads;

    @Before
    public void init() {
        numberOfThreads = 10;

        executor = mock(ExecutorService.class);

        mockStatic(Executors.class);
        Mockito.when(Executors.newFixedThreadPool(numberOfThreads)).thenReturn(executor);
    }

    @Test
    public void testConstructor() throws Exception {

        new ProducerRunner(numberOfThreads);

        verifyStatic();
        Executors.newFixedThreadPool(numberOfThreads);
    }

    @Test
    public void testSubmitProducer() throws Exception {

        final ProducerRunner producerRunner = getProducerRunner();

        final Callable callable = mock(Callable.class);
        producerRunner.submitProducer(callable);

        verify(executor).submit(callable);

    }

    @Test
    public void testAwaitProducerTermination() throws Exception {

        final ProducerRunner producerRunner = getProducerRunner();

        producerRunner.initializeProducerTermination();

        verify(executor).awaitTermination(10, TimeUnit.MINUTES);
    }

    private ProducerRunner getProducerRunner() {
        final ProducerRunner producerRunner = new ProducerRunner(numberOfThreads);

        verifyStatic();
        Executors.newFixedThreadPool(numberOfThreads);
        return producerRunner;
    }

    //    public void initializeProducerTermination() {
//        try {
//            executor.awaitTermination(1, TimeUnit.HOURS);
//        } catch (InterruptedException e) {
//            // TODO handle it
//            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
//        }
//    }
}
