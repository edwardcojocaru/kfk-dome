package com.edw.kafka.consumer.sync;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * User: eduard.cojocaru
 * Date: 12/17/13
 */
public class GiImportanceSelectorTest {

    public static final String LINE = "1385925478190,4042323000,10.13.7.36,10.134.213.202@inet,1,16,2,1,3562662062,80,www.webnoviny.sk,/templates/webnoviny/i/mbg.gif,176608714,43261,0,1385925477,920,0,1140,341,1,9,1,6,50397184,0,0,0,0,0,,";
    public static final String BAD_LINE = "1385925478190,4042323000,10.13.7.36,10.134.213.202@inet,1,16,2,1,3562662062,80,www.webnoviny.sk,/templates/webnoviny/i/mbg.gif,176608714,43261,0,abc,920,0,1140,341,1,9,1,6,50397184,0,0,0,0,0,,";

    @Test
    public void testGetImportance() throws Exception {
        final GiImportanceSelector giImportanceSelector = new GiImportanceSelector();
        assertEquals(Long.valueOf(1385925477), giImportanceSelector.getImportance(LINE));
        assertEquals(Long.valueOf(0), giImportanceSelector.getImportance(BAD_LINE));
        assertEquals(Long.valueOf(0), giImportanceSelector.getImportance(""));
    }
}
