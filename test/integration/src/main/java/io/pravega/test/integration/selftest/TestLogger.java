/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.integration.selftest;

import io.pravega.common.Timer;

import java.time.Duration;

/**
 * Logging for Self tester.
 */
public final class TestLogger {
    private static final Timer TIME = new Timer();

    public static void log(String component, String messageFormat, Object... args) {
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
