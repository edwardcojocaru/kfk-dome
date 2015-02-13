package com.edw.kafka.consumer.sync;

import org.junit.Test;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * User: eduard.cojocaru
 * Date: 12/17/13
 */
public class QueueHolderTest {

    @Test
    public void testGetNextBatch() throws Exception {

        final QueueHolder queueHolder = new QueueHolder(getSelector(), getQueue(), 5, null);

        List<String> nextBatch = queueHolder.getNextBatch();
        for (int i = 1; i <= 5; i++) {
            assertTrue(nextBatch.contains(Integer.toString(i)));
        }

        nextBatch = queueHolder.getNextBatch();
        assertEquals(4, nextBatch.size());
        for (int i = 6; i <= 9; i++) {
            assertTrue(nextBatch.contains(Integer.toString(i)));
        }
    }

    @Test
    public void testGetAverageForEmptyQueue() throws Exception {
        assertEquals(new Double(0), new Double(new QueueHolder(null, null, 0, null).getAverage()));
    }

    @Test
    public void testGetAverage() throws Exception {

        final QueueHolder queueHolder = new QueueHolder(getSelector(), getQueue(), 5, null);

        queueHolder.getNextBatch();
        assertEquals(new Double(3), new Double(queueHolder.getAverage()));

        queueHolder.getNextBatch();
        assertEquals(new Double(7.5), new Double(queueHolder.getAverage()));
    }

    private ImportanceSelector getSelector() {
        return new ImportanceSelector() {
            @Override
            public Long getImportance(String line) {
                return Long.parseLong(line);
            }
        };
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

    private BlockingQueue<String> getGnQueue() throws InterruptedException {
        BlockingQueue<String> queue = new ArrayBlockingQueue<>(15);
        queue.put("1381761869,1381761869,16,,35,2,24112,231014400436488,3547790357893665,421917782188,TSS153.CORP,5,2,CHARGING_ID,0A0A99C4,,31,255,,,,,255,,,31,255,,,,,255,,,D597DC19,D597D915,231,01,19,119,0");
        queue.put("1381761870,1381761869,16,,35,2,24112,231014400436488,3547790357893665,421917782188,TSS153.CORP,5,2,CHARGING_ID,0A0A99C4,,31,255,,,,,255,,,31,255,,,,,255,,,D597DC19,D597D915,231,01,19,119,0");
        queue.put("1381761871,1381761869,16,,35,2,24112,231014400436488,3547790357893665,421917782188,TSS153.CORP,5,2,CHARGING_ID,0A0A99C4,,31,255,,,,,255,,,31,255,,,,,255,,,D597DC19,D597D915,231,01,19,119,0");
        queue.put("1381761872,1381761869,16,,35,2,24112,231014400436488,3547790357893665,421917782188,TSS153.CORP,5,2,CHARGING_ID,0A0A99C4,,31,255,,,,,255,,,31,255,,,,,255,,,D597DC19,D597D915,231,01,19,119,0");
        queue.put("1381761873,1381761869,16,,35,2,24112,231014400436488,3547790357893665,421917782188,TSS153.CORP,5,2,CHARGING_ID,0A0A99C4,,31,255,,,,,255,,,31,255,,,,,255,,,D597DC19,D597D915,231,01,19,119,0");
        queue.put("1381761874,1381761869,16,,35,2,24112,231014400436488,3547790357893665,421917782188,TSS153.CORP,5,2,CHARGING_ID,0A0A99C4,,31,255,,,,,255,,,31,255,,,,,255,,,D597DC19,D597D915,231,01,19,119,0");
        return queue;
    }

    @Test
    public void testGetGnAverage() throws Exception {

        final QueueHolder queueHolder = new QueueHolder(new GnImportanceSelector(), getGnQueue(), 3, null);

        queueHolder.getNextBatch();
        assertEquals(new Double(1381761870), new Double(queueHolder.getAverage()));

        queueHolder.getNextBatch();
        assertEquals(new Double(1381761873), new Double(queueHolder.getAverage()));
    }

    @Test
    public void testGetGnAverageDelayed() throws Exception {

        final int fiveMinutesDelay = 5 * 60;
        final QueueHolder queueHolder = new QueueHolder(new GnImportanceSelector(), getGnQueue(), 3, null, fiveMinutesDelay);

        queueHolder.getNextBatch();
        assertEquals(new Double(1381761870 + fiveMinutesDelay), new Double(queueHolder.getAverage()));
        assertEquals(new Double(1381761870), new Double(queueHolder.getAverage() - queueHolder.getDelay()));

        queueHolder.getNextBatch();
        assertEquals(new Double(1381761873 + fiveMinutesDelay), new Double(queueHolder.getAverage()));
    }

    private BlockingQueue<String> getGiQueue() throws InterruptedException {
        BlockingQueue<String> queue = new ArrayBlockingQueue<>(15);
        queue.put("1381761599984,4042323000,10.19.7.27,10.144.251.122@inet,1,17,358,1,520966497,443,,,177273722,48886,0,1381760560,23620,0,12134,40652,1,10,1,6,16777216,0,0,0,0,0,,");
        queue.put("1381761599984,4042323000,10.19.7.27,10.144.251.122@inet,1,17,358,1,520966497,443,,,177273722,48886,0,1381760561,23620,0,12134,40652,1,10,1,6,16777216,0,0,0,0,0,,");
        queue.put("1381761599984,4042323000,10.19.7.27,10.144.251.122@inet,1,17,358,1,520966497,443,,,177273722,48886,0,1381760562,23620,0,12134,40652,1,10,1,6,16777216,0,0,0,0,0,,");
        queue.put("1381761599984,4042323000,10.19.7.27,10.144.251.122@inet,1,17,358,1,520966497,443,,,177273722,48886,0,1381760563,23620,0,12134,40652,1,10,1,6,16777216,0,0,0,0,0,,");
        queue.put("1381761599984,4042323000,10.19.7.27,10.144.251.122@inet,1,17,358,1,520966497,443,,,177273722,48886,0,1381760564,23620,0,12134,40652,1,10,1,6,16777216,0,0,0,0,0,,");
        queue.put("1381761599984,4042323000,10.19.7.27,10.144.251.122@inet,1,17,358,1,520966497,443,,,177273722,48886,0,1381760565,23620,0,12134,40652,1,10,1,6,16777216,0,0,0,0,0,,");
        return queue;
    }

    @Test
    public void testGetGiAverage() throws Exception {

        final QueueHolder queueHolder = new QueueHolder(new GiImportanceSelector(), getGiQueue(), 3, null);

        queueHolder.getNextBatch();
        assertEquals(new Double(1381760561), new Double(queueHolder.getAverage()));

        queueHolder.getNextBatch();
        assertEquals(new Double(1381760564), new Double(queueHolder.getAverage()));
    }

    @Test
    public void testGetStreamName() throws Exception {
        assertEquals("streamName", new QueueHolder(null, null, 0, "streamName").getStreamName());
    }
}
