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
package io.pravega.segmentstore.storage;

import com.google.common.base.Preconditions;
import java.time.Duration;
import lombok.Getter;

/**
 * Provides information about a {@link DurableDataLog}'s write settings and/or limitations.
 */
@Getter
public class WriteSettings {
    /**
     * The maximum number of bytes allowed for a single write.
     */
    private final int maxWriteLength;
    /**
     * The maximum amount of time that a write may be outstanding for before it is timed out.
     */
    private final Duration maxWriteTimeout;
    /**
     * The maximum number of bytes (across all in-flight appends) that can be outstanding for the {@link DurableDataLog}
     * at any given time.
     * This is a suggested value and is not enforced inside the {@link DurableDataLog}.
     */
    private final int maxOutstandingBytes;

    /**
     * Creates a new instance of the {@link WriteSettings} class.
     *
     * @param maxWriteLength      The maximum number of bytes allowed for a single write.
     * @param maxWriteTimeout     The maximum amount of time that a write may be outstanding for before it is timed out.
     * @param maxOutstandingBytes The maximum number of bytes (across all in-flight appends) that can be outstanding for
     *                            the {@link DurableDataLog} at any given time.
     */
    public WriteSettings(int maxWriteLength, Duration maxWriteTimeout, int maxOutstandingBytes) {
        Preconditions.checkArgument(maxWriteLength > 0, "maxWriteLength must be a positive integer");
        Preconditions.checkArgument(!maxWriteTimeout.isNegative(), "maxWriteTimeout must be a non-negative duration.");
        Preconditions.checkArgument(maxOutstandingBytes > 0, "maxOutstandingBytes must be a positive integer");
        this.maxWriteLength = maxWriteLength;
        this.maxWriteTimeout = maxWriteTimeout;
        this.maxOutstandingBytes = maxOutstandingBytes;
    }

    @Override
    public String toString() {
        return String.format("MaxWriteLength = %s, MaxOutstandingBytes = %s, MaxWriteTimeoutMillis = %s",
                this.maxWriteLength, this.maxOutstandingBytes, this.maxWriteTimeout.toMillis());
    }
}
