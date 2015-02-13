package com.edw.kafka.consumer.storm;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.edw.kafka.consumer.exception.ConfigurationException;
import com.edw.kafka.consumer.sync.*;
import com.edw.kafka.consumer.utils.Constants;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatcher;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import scala.actors.threadpool.Arrays;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.*;
import static org.powermock.api.support.membermodification.MemberMatcher.constructor;

/**
 * User: eduard.cojocaru
 * Date: 12/17/13
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({KafkaSyncSpout.class, ConsumerSynchronizer.class, StreamBatch.class, SpoutOutputCollector.class, TopologyContext.class, ConsumerManager.class, ImportanceSelectorFactory.class})
public class KafkaSyncSpoutTest {

    private KafkaSyncSpout kafkaSyncSpout;

    @Before
    public void setUp() {
        Map<TopicEnum, String> map = new HashMap<>();
        map.put(TopicEnum.GI_TOPIC, "topic");

        kafkaSyncSpout = new KafkaSyncSpout(map, 5);
    }

    @Test
    public void testDeclareOutputFields() throws Exception {

        OutputFieldsDeclarer declarer = mock(OutputFieldsDeclarer.class);
        kafkaSyncSpout.declareOutputFields(declarer);

        verify(declarer).declareStream(eq(TopicEnum.GI_TOPIC.getStreamName()), any(Fields.class));
    }

    @Test
    public void testOpen() throws Exception {

        final ConsumerSynchronizer consumerSynchronizer = mock(ConsumerSynchronizer.class);

        whenNew(constructor(ConsumerSynchronizer.class, double.class)).withArguments(anyDouble()).thenReturn(consumerSynchronizer);

        final ConsumerManager consumerManager = mock(ConsumerManager.class);
        whenNew(constructor(ConsumerManager.class, Collection.class, ConsumerFactory.class))
                .withArguments(any(Collection.class), any(ConsumerFactory.class))
                .thenReturn(consumerManager);

        final ImportanceSelector importanceSelector = mock(ImportanceSelector.class);

        mockStatic(ImportanceSelectorFactory.class);
        PowerMockito.when(ImportanceSelectorFactory.getImportanceSelector(any(TopicEnum.class))).thenReturn(importanceSelector);

        SpoutOutputCollector collector = mock(SpoutOutputCollector.class);
        Map configurationMap = new HashMap();
        final HashMap<String, Object> value = new HashMap<>();
        value.put("a", "b");
        configurationMap.put(Constants.KAFKA_CONFIG_KEY, value);

        TopologyContext topologyContext = mock(TopologyContext.class);

        kafkaSyncSpout.open(configurationMap, topologyContext, collector);

        verifyNew(ConsumerSynchronizer.class).withArguments((double) com.francetelecom.dome.utils.Constants.ONE_HOUR_IN_SECONDS);
        verifyNew(ConsumerManager.class).withArguments(any(Collection.class), any(ConsumerFactory.class));

        verify(consumerSynchronizer).addStreamQueue(argThat(new ArgumentMatcher<QueueHolder>() {
            @Override
            public boolean matches(Object argument) {
                QueueHolder holder = (QueueHolder) argument;
                Object selectorObject;
                try {
                    final Field selector = QueueHolder.class.getDeclaredField("selector");
                    selector.setAccessible(true);
                    selectorObject = selector.get(holder);
                } catch (NoSuchFieldException | IllegalAccessException e) {
                    return false;
                }

                return TopicEnum.GI_TOPIC.getStreamName().equals(holder.getStreamName()) || selectorObject instanceof GiImportanceSelector;
            }
        }));

        verifyStatic();
        ImportanceSelectorFactory.getImportanceSelector(any(TopicEnum.class));
    }

    @Test
    public void testNextTuple() throws Exception {

        final Field consumerSynchronizerField = KafkaSyncSpout.class.getDeclaredField("consumerSynchronizer");
        consumerSynchronizerField.setAccessible(true);

        final ConsumerSynchronizer consumerSynchronizer = mock(ConsumerSynchronizer.class);
        final StreamBatch streamBatch = mock(StreamBatch.class);
        when(streamBatch.getStreamName()).thenReturn("streamName");
        when(consumerSynchronizer.getNextBatch()).thenReturn(new ArrayList<StreamBatch>(), Arrays.asList(new StreamBatch[]{streamBatch}));

        consumerSynchronizerField.set(kafkaSyncSpout, consumerSynchronizer);

        final Field collectorField = KafkaSyncSpout.class.getDeclaredField("collector");
        collectorField.setAccessible(true);

        SpoutOutputCollector collector = mock(SpoutOutputCollector.class);
        collectorField.set(kafkaSyncSpout, collector);
        kafkaSyncSpout.nextTuple();
        kafkaSyncSpout.nextTuple();

        verify(consumerSynchronizer, times(2)).getNextBatch();
        verify(streamBatch).getStreamName();
        verify(streamBatch).getBatch();

        verify(collector).emit(eq("streamName"), any(Values.class));
    }

    @Test(expected = ConfigurationException.class)
    public void testConstructorExceptionNullMap() throws Exception {
        new KafkaSyncSpout(null, 0);
    }

    @Test(expected = ConfigurationException.class)
    public void testConstructorExceptionEmptyMap() throws Exception {
        new KafkaSyncSpout(new HashMap<TopicEnum, String>(), 0);
    }
}
