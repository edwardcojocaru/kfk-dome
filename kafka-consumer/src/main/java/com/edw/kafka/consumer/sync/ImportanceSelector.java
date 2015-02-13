package com.edw.kafka.consumer.sync;

/**
 * User: eduard.cojocaru
 * Date: 12/16/13
 */
public interface ImportanceSelector {

    Long getImportance(String line);
}
