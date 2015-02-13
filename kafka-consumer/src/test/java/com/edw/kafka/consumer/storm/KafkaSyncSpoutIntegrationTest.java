package com.edw.kafka.consumer.storm;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import com.edw.kafka.consumer.sync.*;
import com.edw.kafka.consumer.utils.Constants;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.anyDouble;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.*;
import static org.powermock.api.support.membermodification.MemberMatcher.constructor;

/**
 * User: eduard.cojocaru
 * Date: 12/17/13
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ConsumerManager.class, ConsumerSynchronizer.class, KafkaSyncSpout.class, ImportanceSelectorFactory.class, ImportanceSelector.class, QueueHolder.class})
public class KafkaSyncSpoutIntegrationTest {

    private KafkaSyncSpout kafkaSyncSpout;

    @Before
    public void setUp() {
        Map<TopicEnum, String> map = new HashMap<>();
        map.put(TopicEnum.GI_TOPIC, "topic");

        kafkaSyncSpout = spy(new KafkaSyncSpout(map, 5));
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
        when(ImportanceSelectorFactory.getImportanceSelector(any(TopicEnum.class))).thenReturn(importanceSelector);

        QueueHolder queueHolder = mock(QueueHolder.class);
        whenNew(constructor(QueueHolder.class, ImportanceSelector.class, BlockingQueue.class, int.class, String.class, int.class))
                .withArguments(eq(importanceSelector), any(BlockingQueue.class), anyInt(), anyString(), anyInt())
                .thenReturn(queueHolder);

        SpoutOutputCollector collector = mock(SpoutOutputCollector.class);
        Map configurationMap = new HashMap();
        final HashMap<String, Object> value = new HashMap<>();
        value.put("a", "b");
        configurationMap.put(Constants.KAFKA_CONFIG_KEY, value);

        TopologyContext topologyContext = mock(TopologyContext.class);

        kafkaSyncSpout.open(configurationMap, topologyContext, collector);

        verifyNew(ConsumerManager.class).withArguments(any(Collection.class), any(ConsumerFactory.class));
        verify(consumerManager).getWorkingQueues();

        verify(consumerSynchronizer).addStreamQueue(eq(queueHolder));

        verifyNew(QueueHolder.class).withArguments(eq(importanceSelector), any(BlockingQueue.class), anyInt(), anyString(), anyInt());

        verifyStatic();
        ImportanceSelectorFactory.getImportanceSelector(any(TopicEnum.class));
        // final Map<String, Object> kafkaConfig = Utils.getConfigMap(configurationMap);
//        final ConsumerManager consumerManager = new ConsumerManager(topicsMap.values(), new ConsumerFactory(kafkaConfig));
//        final Map<String, BlockingQueue<String>> workingQueues = consumerManager.getWorkingQueues();
//
//        for (Map.Entry<TopicEnum, String> topic : topicsMap.entrySet()) {
//            final TopicEnum topicEnum = topic.getKey();
//            final ImportanceSelector importanceSelector = ImportanceSelectorFactory.getImportanceSelector(topicEnum);
//            final BlockingQueue<String> queue = workingQueues.get(topic.getValue());
//            final String streamName = topicEnum.getStreamName();
//            consumerSynchronizer.addStreamQueue(new QueueHolder(importanceSelector, queue, batchSize, streamName, getDelay(topicEnum)));
//        }
    }


}
