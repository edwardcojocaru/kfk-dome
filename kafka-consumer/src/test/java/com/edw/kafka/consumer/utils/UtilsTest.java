package com.edw.kafka.consumer.utils;

import com.edw.kafka.consumer.sync.ImportanceSelector;
import org.junit.Test;
import scala.actors.threadpool.Arrays;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import static org.junit.Assert.assertEquals;

/**
 * User: eduard.cojocaru
 * Date: 12/17/13
 */
public class UtilsTest {
    @Test
    public void testGetBatch() throws Exception {
        final BlockingQueue<String> queue = getQueue();
        assertEquals(5, Utils.getBatch(queue, 5).size());
        assertEquals(4, Utils.getBatch(queue, 5).size());
    }

    @Test
    public void testGetAverage() throws Exception {
        Double average = Utils.getAverage(getSelector(), Arrays.asList(new String[]{"1", "2", "3", "4", "5"}));
        assertEquals(new Double(3), average);

        average = Utils.getAverage(getSelector(), Arrays.asList(new String[]{"1", "2", "3", "4", "5", "6", "7", "8", "9"}));
        assertEquals(new Double(5), average);
    }

    private BlockingQueue<String> getQueue() throws InterruptedException {
        BlockingQueue<String> queue = new ArrayBlockingQueue<>(15);
        queue.put("1");
        queue.put("2");
        queue.put("3");
        queue.put("4");
        queue.put("5");
        queue.put("6");
        queue.put("7");
        queue.put("8");
        queue.put("9");
        return queue;
    }

    private ImportanceSelector getSelector() {
        return new ImportanceSelector() {
            @Override
            public Long getImportance(String line) {
                return Long.parseLong(line);
            }
        };
    }
}
