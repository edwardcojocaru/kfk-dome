package com.edw.kafka.consumer.sync;

import com.edw.kafka.consumer.exception.ConfigurationException;
import com.edw.kafka.consumer.storm.TopicEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * User: eduard.cojocaru
 * Date: 12/16/13
 */
public final class ImportanceSelectorFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(ImportanceSelectorFactory.class);

    public static ImportanceSelector getImportanceSelector(TopicEnum topicEnum) {

        LOGGER.debug("Getting importance selector for {} topic", topicEnum);
        switch (topicEnum) {
            case GI_TOPIC:
                return new GiImportanceSelector();
            case GN_TOPIC:
                return new GnImportanceSelector();
            default:
                LOGGER.error("Topic enum not in supported list.");
                throw new ConfigurationException("topic enum not defined.");
        }

    }
}
