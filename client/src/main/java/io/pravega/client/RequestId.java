/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client;

import com.google.common.annotations.VisibleForTesting;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.concurrent.GuardedBy;
import lombok.Getter;
import lombok.Synchronized;
import lombok.ToString;

/**
 * The class represents a request id that is sent as part of every WireCommand. It consists of a
 * <i>sessionId</i> and a <i>requestSequenceNumber</i>. The <i>sessionId</i> is used to represent the
 * communication between Segment clients and the SegmentStore which uses an underlying
 * connection pool. This <i>sessionId</i> is unique per connection pool.
 * The <i>requestSequenceNumber</i> is used to represent the requestSequence for a given session.
 */
@ToString
public class RequestId {
    private static final AtomicInteger ID_GENERATOR = new AtomicInteger(0);
    @Getter
    private final int sessionId;
    @GuardedBy("$LOCK")
    private int requestSequenceNumber = 0;

    public RequestId() {
        this(ID_GENERATOR.updateAndGet(i -> i == Integer.MAX_VALUE ? 0 : i + 1), 0);
    }

    @VisibleForTesting
    RequestId(int sessionId, int requestSequenceNumber) {
        this.sessionId = sessionId;
        this.requestSequenceNumber = requestSequenceNumber;
    }

    /**
     * Return a {@code long} representation of {@link RequestId}.
     * @return long representation.
     */
    @Synchronized
    public long asLong() {
        return ((long) sessionId << 32) | ((long) requestSequenceNumber & 0xFFFFFFFL);
    }

    /**
     * Obtain the {@link RequestId} corresponding to the next sequence number as {@code long}.
     * @return RequestId corresponding to next sequence number.
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
