package com.edw.kafka.consumer.storm;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.edw.kafka.consumer.exception.ConfigurationException;
import com.edw.kafka.consumer.exception.ConsumerStoppedException;
import com.edw.kafka.consumer.utils.Constants;
import com.edw.kafka.consumer.utils.Utils;
import kafka.consumer.KafkaStream;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatcher;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.*;

/**
 * User: eduard.cojocaru
 * Date: 12/17/13
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({KafkaSpout.class, SpoutOutputCollector.class, TopologyContext.class, OutputFieldsDeclarer.class, Utils.class, KafkaStream.class, Executors.class, ExecutorService.class})
public class KafkaSpoutTest {

    private KafkaSpout kafkaSpout;

    @Before
    public void setUp() throws Exception {
        kafkaSpout = new KafkaSpout("topic", 1);
    }

    @Test
    public void testDeclareOutputFields() throws Exception {
        final OutputFieldsDeclarer declarer = mock(OutputFieldsDeclarer.class);
        kafkaSpout.declareOutputFields(declarer);
        verify(declarer).declare(any(Fields.class));
    }

    @Test
    public void testNextTuple() throws Exception {

        mockStatic(Utils.class);
        when(Utils.getBatch(any(BlockingQueue.class), anyInt())).thenReturn(new ArrayList<String>());

        setField(kafkaSpout, "working", new AtomicBoolean(true));

        kafkaSpout.nextTuple();

        verifyStatic();
        Utils.getBatch(any(BlockingQueue.class), anyInt());
    }

    public static void setField(Object targetBean, String fieldName, Object fieldValue) throws Exception {
        final Field declaredField = targetBean.getClass().getDeclaredField(fieldName);
        declaredField.setAccessible(true);

        declaredField.set(targetBean, fieldValue);
    }

    @Test
    public void testNextTupleEmit() throws Exception {

        mockStatic(Utils.class);
        final List<String> strings = new ArrayList<>();
        strings.add("element");
        when(Utils.getBatch(any(BlockingQueue.class), anyInt())).thenReturn(strings);

        setField(kafkaSpout, "working", new AtomicBoolean(true));
        final SpoutOutputCollector collector = mock(SpoutOutputCollector.class);
        setField(kafkaSpout, "collector", collector);

        kafkaSpout.nextTuple();

        verify(collector).emit(argThat(new ArgumentMatcher<Values>() {
            @Override
            public boolean matches(Object argument) {
                Values values = (Values) argument;
                return ((List) values.get(0)).get(0).equals("element");
            }
        }), anyLong());

        verifyStatic();
        Utils.getBatch(any(BlockingQueue.class), anyInt());
    }

    @Test(expected = ConsumerStoppedException.class)
    public void testNextTupleException() throws Exception {

        mockStatic(Utils.class);
        when(Utils.getBatch(any(BlockingQueue.class), anyInt())).thenReturn(new ArrayList<String>());

        setField(kafkaSpout, "working", new AtomicBoolean(false));
        setField(kafkaSpout, "queue", new ArrayBlockingQueue<String>(3));

        kafkaSpout.nextTuple();

        verifyStatic();
        Utils.getBatch(any(BlockingQueue.class), anyInt());
    }

    @Test
    public void testNextTupleNotEmptyQueue() throws Exception {

        mockStatic(Utils.class);
        when(Utils.getBatch(any(BlockingQueue.class), anyInt())).thenReturn(new ArrayList<String>());

        setField(kafkaSpout, "working", new AtomicBoolean(false));
        final ArrayBlockingQueue<String> fieldValue = new ArrayBlockingQueue<>(3);
        fieldValue.put("abc");
        setField(kafkaSpout, "queue", fieldValue);

        kafkaSpout.nextTuple();

        verifyStatic();
        Utils.getBatch(any(BlockingQueue.class), anyInt());
    }

    @Test
    public void testOpen() throws Exception {

        final HashMap<String, Object> value = new HashMap<>();
        value.put("a", "b");
        Map configurationMap = new HashMap();
        configurationMap.put(Constants.KAFKA_CONFIG_KEY, value);

        final List<KafkaStream<byte[], byte[]>> kafkaStreams = new ArrayList<>();
        final KafkaStream kafkaStream = mock(KafkaStream.class);
        kafkaStreams.add(kafkaStream);

        final ExecutorService executorService = mock(ExecutorService.class);
        mockStatic(Executors.class);
        when(Executors.newFixedThreadPool(anyInt())).thenReturn(executorService);

        mockStatic(Utils.class);
        when(Utils.getKafkaStream(value, "topic")).thenReturn(kafkaStreams);
        when(Utils.getConfigMap(configurationMap)).thenReturn(value);

        final KafkaConsumer kafkaConsumer = mock(KafkaConsumer.class);
        whenNew(constructor(KafkaConsumer.class, BlockingQueue.class, KafkaStream.class, AtomicBoolean.class))
                .withArguments(any(BlockingQueue.class), eq(kafkaStream), any(AtomicBoolean.class))
                .thenReturn(kafkaConsumer);

        SpoutOutputCollector collector = mock(SpoutOutputCollector.class);
        TopologyContext topologyContext = mock(TopologyContext.class);

        kafkaSpout.open(configurationMap, topologyContext, collector);

        verifyStatic();
        Utils.getConfigMap(configurationMap);
        verifyStatic();
        Utils.getKafkaStream(value, "topic");
        verifyStatic();
        Executors.newFixedThreadPool(anyInt());

        verify(executorService).submit(kafkaConsumer);
        verifyNew(KafkaConsumer.class);
    }

    @Test(expected = ConfigurationException.class)
    public void testConstructorForException() throws Exception {
        new KafkaSpout("", 0);
    }
}
