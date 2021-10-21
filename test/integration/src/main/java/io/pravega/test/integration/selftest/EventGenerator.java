/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.test.integration.selftest;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Generates sequential Appends using arbitrary routing keys.
 */
@ThreadSafe
class EventGenerator {
    //region Members

    private final int ownerId;
    private final boolean recordStartTime;
    private final AtomicInteger sequence;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the EventGenerator class.
     *
     * @param ownerId         The Id to attach to all appends generated with this instance.
     * @param recordStartTime Whether to record start times in the appends.
     */
    EventGenerator(int ownerId, boolean recordStartTime) {
        this.ownerId = ownerId;
        this.recordStartTime = recordStartTime;
        this.sequence = new AtomicInteger(0);
    }

    //endregion

    //region New Event

    /**
     * Generates a new Event.
     *
     * @param length The total length of the Event (including header).
     * @param routingKey The routing key of the Event.
     * @return The generated Event.
     */
    Event newEvent(int length, int routingKey) {
        int sequence = this.sequence.getAndIncrement();
        long startTime = this.recordStartTime ? getCurrentTimeNanos() : Long.MIN_VALUE;
        return new Event(this.ownerId, routingKey, sequence, startTime, length);
    }

    private static long getCurrentTimeNanos() {
        return System.nanoTime();
    }

    //endregion

    //region Validation

    /**
     * Validates that the given Event is valid.
     *
     * @param event The event to inspect.
     * @return A ValidationResult representing the validation.
     */
    static ValidationResult validate(Event event) {
        ValidationResult result;
        try {
            // Deserialize and validate the append.
            event.validateContents();
            result = ValidationResult.success(event.getRoutingKey(), event.getTotalLength());
            if (event.getStartTime() >= 0) {
                // Set the elapsed time, but only for successful operations that did encode the start time.
                result.setElapsed(Duration.ofNanos(getCurrentTimeNanos() - event.getStartTime()));
            }
        } catch (Exception ex) {
            // Any validation exceptions are thrown either from the Event Constructor or from validateContents().
            result = ValidationResult.failed(ex.getMessage());
        }

        return result;
    }

    //endregion
}
