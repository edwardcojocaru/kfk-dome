package com.edw.kafka.consumer.sync;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * User: eduard.cojocaru
 * Date: 12/17/13
 */
public class GnImportanceSelectorTest {

    public static final String LINE = "1385925258,1385925258,18,128,3094,1,50204,231014400129859,3555990504282601,421907520029,INTERNET,5,1,1940977497,0A862B9A,,31,4,1500,2097152,8847360,1000,2,0,0,31,4,1500,2097152,8847360,1000,0,0,0,000000000000000000000000D597FC1D,000000000000000000000000D597D91D,231,01,1940977497,123,0";
    public static final String BAD_LINE = "abc,4042323000,10.13.7.36,10.134.213.202@inet,1,16,2,1,3562662062,80,www.webnoviny.sk,/templates/webnoviny/i/mbg.gif,176608714,43261,0,abc,920,0,1140,341,1,9,1,6,50397184,0,0,0,0,0,,";

    @Test
    public void testGetImportance() throws Exception {
        final GnImportanceSelector giImportanceSelector = new GnImportanceSelector();
        assertEquals(Long.valueOf(1385925258), giImportanceSelector.getImportance(LINE));
        assertEquals(Long.valueOf(0), giImportanceSelector.getImportance(BAD_LINE));
        assertEquals(Long.valueOf(0), giImportanceSelector.getImportance(""));
    }
}
