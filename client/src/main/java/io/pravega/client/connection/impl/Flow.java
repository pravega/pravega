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
package io.pravega.client.connection.impl;

import com.google.common.annotations.VisibleForTesting;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.concurrent.GuardedBy;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Synchronized;
import lombok.ToString;

/**
 * The class represents a Flow between a Segment client and the SegmentStore. It consists of a
 * <i>flowId</i> and a <i>requestSequenceNumber</i>. The <i>flowId</i> is used to represent the
 * communication between Segment clients and the SegmentStore which uses an underlying
 * connection pool. This <i>flowId</i> is unique per connection pool and a Flow is always tied to
 * a specific network connection.
 * The <i>requestSequenceNumber</i> is used to represent the requestSequence for a given Flow.
 */
@ToString
@EqualsAndHashCode
public class Flow {
    private static final AtomicInteger ID_GENERATOR = new AtomicInteger(0);
    @Getter
    private final int flowId;
    @GuardedBy("$lock")
    private int requestSequenceNumber = 0;

    @VisibleForTesting
    public Flow(int flowId, int requestSequenceNumber) {
        this.flowId = flowId;
        this.requestSequenceNumber = requestSequenceNumber;
    }

    /**
     * Create a new Flow.
     *
     * @return Flow.
     */
    public static Flow create() {
        return new Flow(ID_GENERATOR.updateAndGet(i -> i == Integer.MAX_VALUE ? 1 : i + 1), 0);
    }

    /**
     * Obtain a Flow from a {@code long} representation.
     *
     * @param flowAsLong a {@code long} representation of {@link Flow}.
     * @return Flow.
     */
    public static Flow from(long flowAsLong) {
        return new Flow((int) (flowAsLong >> 32), (int) flowAsLong);
    }

    /**
     * Obtain a FlowID from a {@code long} representation.
     *
     * @param requestID request identifier.
     * @return Flow ID.
     */
    public static int toFlowID(long requestID) {
        return (int) (requestID >> 32);
    }

    /**
     * Return a {@code long} representation of {@link Flow}.
     *
     * @return long representation.
     */
    @Synchronized
    public long asLong() {
        return ((long) flowId << 32) | (requestSequenceNumber & 0xFFFFFFFL);
    }

    /**
     * Obtain a {@code long} representation of {@link Flow} with the next sequence number.
     *
     * @return {@code long} representation of {@link Flow} with the next sequence number.
     */
    @Synchronized
    public long getNextSequenceNumber() {
        if (requestSequenceNumber == Integer.MAX_VALUE) {
            requestSequenceNumber = 0;
        } else {
            requestSequenceNumber += 1;
        }
        return asLong();
    }
}
