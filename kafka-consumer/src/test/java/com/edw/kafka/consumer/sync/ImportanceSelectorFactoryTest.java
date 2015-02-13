package com.edw.kafka.consumer.sync;

import com.edw.kafka.consumer.storm.TopicEnum;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * User: eduard.cojocaru
 * Date: 12/17/13
 */
public class ImportanceSelectorFactoryTest {

    @Test
    public void testGetImportanceSelector() throws Exception {
        ImportanceSelector importanceSelector = ImportanceSelectorFactory.getImportanceSelector(TopicEnum.GI_TOPIC);
        assertTrue(importanceSelector instanceof GiImportanceSelector);

        importanceSelector = ImportanceSelectorFactory.getImportanceSelector(TopicEnum.GN_TOPIC);
        assertTrue(importanceSelector instanceof GnImportanceSelector);
    }
}
