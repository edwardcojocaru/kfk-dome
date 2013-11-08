package com.francetelecom.dome.util;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.*;

/**
 * User: Eduard.Cojocaru
 * Date: 11/8/13
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ExecutorService.class, Utils.class})
public class UtilsTest {

    private ExecutorService executor;

    @Before
    public void setUp() {
        executor = mock(ExecutorService.class);
    }

    @Test
    public void testWaitToStopExecutorManager() throws Exception {

        mockStatic(Utils.class);
        Utils.waitToStopExecutor(Constants.MANAGERS_TIMEOUT, Constants.MANAGERS_TIMEOUT_UNITS, executor);

        Utils.waitToStopExecutorManager(executor);

        verifyStatic();
        Utils.waitToStopExecutor(Constants.MANAGERS_TIMEOUT, Constants.MANAGERS_TIMEOUT_UNITS, executor);
    }

    @Test
    public void testWaitToStopExecutorWorker() throws Exception {
        ExecutorService executor = mock(ExecutorService.class);
        mockStatic(Utils.class);
        Utils.waitToStopExecutor(Constants.WORKER_EXECUTOR_TIMEOUT, Constants.WORKER_EXECUTOR_TIMEOUT_UNITS, executor);

        Utils.waitToStopExecutorManager(executor);

        verifyStatic();
        Utils.waitToStopExecutor(Constants.WORKER_EXECUTOR_TIMEOUT, Constants.WORKER_EXECUTOR_TIMEOUT_UNITS, executor);
    }

    @Test
    public void testWaitToStopExecutor() throws Exception {

        int timeout = 13;
        final TimeUnit hours = TimeUnit.HOURS;

        when(executor.awaitTermination(timeout, hours)).thenReturn(true).thenReturn(false).thenThrow(new InterruptedException());

        assertTrue(Utils.waitToStopExecutor(timeout, hours, executor));
        assertFalse(Utils.waitToStopExecutor(timeout, hours, executor));
        assertFalse(Utils.waitToStopExecutor(timeout, hours, executor));

        verify(executor, times(3)).shutdown();
        verify(executor, times(3)).awaitTermination(timeout, hours);
    }
}
