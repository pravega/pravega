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
package io.pravega.segmentstore.server.logs.operations;

import java.util.Arrays;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * Defines various priority levels for {@link Operation}s.
 */
@Getter
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public enum OperationPriority {
    /**
     * Highest possible priority level. This is reserved for Segment Store internal operations that are ESSENTIAL to
     * its functioning. Examples include, but are not limited to, {@link Operation}s that have a direct result of freeing
     * resources (such as those required by the Storage adapter(s)).
     *
     * Consider using {@link #High} before assigning this priority to operations.
     *
     * All {@link Operation}s with this priority level are EXEMPT from throttling in the OperationProcessor.
     *
     * WARNING: NO EXTERNAL OPERATIONS SHOULD BE ASSIGNED THIS PRIORITY LEVEL. Consider using {@link #Critical} instead.
     */
    SystemCritical((byte) 0, true),

    /**
     * Highest priority level that can be assigned to external {@link Operation}s. Only {@link Operation}s that can clear
     * an immediate problem should be assigned this level (i.e., Deleting or Truncating a Segment - which results in
     * resources potentially being freed). This priority level should NEVER be assigned to {@link Operation}s that may
     * result in more resources being used by the system (including adding more data to the Cache).
     *
     * All {@link Operation}s with this priority level are EXEMPT from throttling in the OperationProcessor.
     */
    Critical((byte) 1, true),

    /**
     * Elevated priority level recommended Segment Store internal operations that are needed for its function, but are
     * not as crucial as those requiring {@link #SystemCritical}. Examples include, but are not limited to, maintenance
     * {@link Operation}s that are performed in order to keep the system healthy (Segment evictions, etc.).
     *
     * All {@link Operation}s with this priority level are subject to throttling in the OperationProcessor.
     */
    High((byte) 2, false),

    /**
     * Normal priority level. All external {@link Operation}s that do not qualify for {@link #Critical} have this.
     *
     * All {@link Operation}s with this priority level are subject to throttling in the OperationProcessor.
     */
    Normal((byte) 3, false);

    /**
     * Numeric value for Priority. Lower value is more important.
     */
    final byte value;

    /**
     * Whether {@link Operation}s with this priority level are exempt ({@code true} or not {@code false} from throttling.
     */
    final boolean throttlingExempt;

    /**
     * Returns the maximum numeric value for {@link #getValue()} for all enum values defined in {@link OperationPriority}.
     *
     * NOTE: This should not be misinterpreted as returning the highest priority - refer to the Javadoc in this class
     * to determine that.
     *
     * @return The max value for all {@link #getValue()} in this enum.
     */
    public static byte getMaxPriorityValue() {
        return (byte) Arrays.stream(OperationPriority.values()).mapToInt(OperationPriority::getValue).max().getAsInt();
    }
}
