package com.francetelecom.dome.producer;

import com.francetelecom.dome.util.Constants;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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

        verify(executor).awaitTermination(Constants.MANAGERS_TIMEOUT, Constants.MANAGERS_TIMEOUT_UNITS);
    }

    private ProducerRunner getProducerRunner() {
        final ProducerRunner producerRunner = new ProducerRunner(numberOfThreads);

        verifyStatic();
        Executors.newFixedThreadPool(numberOfThreads);
        return producerRunner;
    }
}
