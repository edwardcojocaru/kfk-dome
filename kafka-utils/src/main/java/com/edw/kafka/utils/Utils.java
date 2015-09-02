package com.edw.kafka.utils;

/**
 * User: Eduard.Cojocaru
 * Date: 1/15/14
 */
public final class Utils {

    
    private static final long CHECK_INTERVAL_IN_MINUTES = 10;
    private static final long SECOND_MILLIS = 1000;
    private static final long MINUTE_MILLIS = SECOND_MILLIS * 60;
    private static final long MINUTES_MILLIS = CHECK_INTERVAL_IN_MINUTES * MINUTE_MILLIS;

    private Utils() {
    }

    public static boolean isLogTime(long lastPrinted1) {
        final long waitingTimeMillis = MINUTES_MILLIS;
        final long currentTime = System.currentTimeMillis();
        final boolean notPrintedInLastMinute = lastPrinted1 + waitingTimeMillis - 100 < currentTime;

        return currentTime % waitingTimeMillis < SECOND_MILLIS && notPrintedInLastMinute;
    }

    public static boolean isLogTime(long lastPrinted, long minutes) {
        final long waitingTimeMillis = MINUTE_MILLIS * minutes;
        final long currentTime = System.currentTimeMillis();
        final boolean notPrintedInLastMinute = lastPrinted + waitingTimeMillis - 100 < currentTime;

        return currentTime % waitingTimeMillis < SECOND_MILLIS && notPrintedInLastMinute;
    }
}
