/**
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.emc.pravega.service.selftest;

import com.emc.pravega.common.AutoStopwatch;

import java.time.Duration;

/**
 * Logging for Self tester
 */
class TestLogger {
    private static final AutoStopwatch TIME = new AutoStopwatch();

    static void log(String component, String messageFormat, Object... args) {
        String header = String.format("%s [%s]: ", formatTime(TIME.elapsed()), component);
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
