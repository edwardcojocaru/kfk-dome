package com.edw.kafka.consumer.sync;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;

import static com.edw.kafka.utils.Constants.ONE_HOUR_IN_SECONDS;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

/**
 * User: eduard.cojocaru
 * Date: 12/17/13
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ConsumerSynchronizer.class, QueueHolder.class})
public class ConsumerSynchronizerTest {

    @Test
    public void testGetNextBatch() throws Exception {
        final ConsumerSynchronizer consumerSynchronizer = new ConsumerSynchronizer();

        final QueueHolder queueHolderSelected = mock(QueueHolder.class);
        when(queueHolderSelected.getAverage()).thenReturn(1d, 2d, 3d, 1d, 2d, 3d);
        when(queueHolderSelected.getStreamName()).thenReturn("good");
        final QueueHolder queueHolderNotSelected = mock(QueueHolder.class);
        when(queueHolderNotSelected.getAverage()).thenReturn(4d, 5d, 4d, 5d);
        consumerSynchronizer.addStreamQueue(queueHolderSelected);
        consumerSynchronizer.addStreamQueue(queueHolderSelected);
        consumerSynchronizer.addStreamQueue(queueHolderSelected);
        consumerSynchronizer.addStreamQueue(queueHolderNotSelected);
        consumerSynchronizer.addStreamQueue(queueHolderNotSelected);

        final List<StreamBatch> nextBatch = consumerSynchronizer.getNextBatch();
        assertEquals(3, nextBatch.size());

        for (StreamBatch batch : nextBatch) {
            assertEquals("good", batch.getStreamName());
        }

        verify(queueHolderSelected, times(6)).getAverage();
        verify(queueHolderNotSelected, times(4)).getAverage();
        verify(queueHolderSelected, times(3)).getStreamName();
        verify(queueHolderSelected, times(3)).getNextBatch();

    }

    @Test
    public void testAddStreamQueue() throws Exception {
        final ConsumerSynchronizer consumerSynchronizer = new ConsumerSynchronizer();

        final Field queueHoldersField = ConsumerSynchronizer.class.getDeclaredField("queueHolders");
        queueHoldersField.setAccessible(true);

        final QueueHolder queueHolder = mock(QueueHolder.class);
        consumerSynchronizer.addStreamQueue(queueHolder);

        final List list = (List) queueHoldersField.get(consumerSynchronizer);

        assertEquals(queueHolder, list.get(0));

    }

    @Test
    public void testHoldersAverage() throws Exception {

        final ConsumerSynchronizer consumerSynchronizer = new ConsumerSynchronizer();

        final QueueHolder queueHolder = mock(QueueHolder.class);
        when(queueHolder.getAverage()).thenReturn(1d, 2d, 3d, 4d, 5d);
        consumerSynchronizer.addStreamQueue(queueHolder);
        consumerSynchronizer.addStreamQueue(queueHolder);
        consumerSynchronizer.addStreamQueue(queueHolder);
        consumerSynchronizer.addStreamQueue(queueHolder);
        consumerSynchronizer.addStreamQueue(queueHolder);

        final Method getHoldersAverageMethod = ConsumerSynchronizer.class.getDeclaredMethod("getHoldersAverage");
        getHoldersAverageMethod.setAccessible(true);

        final double invoke = (double) getHoldersAverageMethod.invoke(consumerSynchronizer);

        assertEquals(new Double(3), new Double(invoke));
        verify(queueHolder, times(5)).getAverage();
    }

    @Test
    public void testConstructorWithError() throws Exception {

        final ConsumerSynchronizer consumerSynchronizer = new ConsumerSynchronizer(ONE_HOUR_IN_SECONDS);

        final Field errorField = ConsumerSynchronizer.class.getDeclaredField("error");
        errorField.setAccessible(true);

        assertEquals(new Double(ONE_HOUR_IN_SECONDS), errorField.get(consumerSynchronizer));
    }

    @Test
    public void testGetRelativeDate() throws Exception {

        final Method getRelativeDateMethod = ConsumerSynchronizer.class.getDeclaredMethod("getRelativeDate", double.class);
        getRelativeDateMethod.setAccessible(true);

        final String value = (String) getRelativeDateMethod.invoke(new ConsumerSynchronizer(ONE_HOUR_IN_SECONDS), (double) 1381761870);

        assertEquals("2013-10-14 17:44:30", value);
    }
}
