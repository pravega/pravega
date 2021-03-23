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
package io.pravega.segmentstore.server;

import io.pravega.common.AbstractTimer;
import lombok.Getter;

/**
 * AbstractTimer Implementation that allows full control over the return values, for use in Unit testing.
 */
public class ManualTimer extends AbstractTimer {
    @Getter
    private volatile long elapsedNanos;

    /**
     * Sets the value of the timer.
     *
     * @param value The value to set, in milliseconds.
     */
    public void setElapsedMillis(long value) {
        this.elapsedNanos = value * NANOS_TO_MILLIS;
    }
}
