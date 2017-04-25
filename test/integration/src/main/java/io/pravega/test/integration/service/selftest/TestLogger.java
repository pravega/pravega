/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.test.integration.service.selftest;

import io.pravega.common.Timer;

import java.time.Duration;

/**
 * Logging for Self tester
 */
class TestLogger {
    private static final Timer TIME = new Timer();

    static void log(String component, String messageFormat, Object... args) {
        String header = String.format("%s [%s]: ", formatTime(TIME.getElapsed()), component);
        String message = String.format(messageFormat, args);
        System.out.println(header + message);
    }

    private static String formatTime(Duration time) {
        long currentTime = time.toMillis();
        int totalSeconds = (int) (currentTime / 1000);
        int totalMinutes = totalSeconds / 60;
        int totalHours = totalMinutes / 60;
        return String.format(
                "%d:%02d:%02d.%03d",
                totalHours,
                totalMinutes % 60,
                totalSeconds % 60,
                currentTime % 1000);
    }
}
