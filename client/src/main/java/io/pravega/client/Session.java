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
 * The class represents a Session between a Segment client and the SegmentStore. It consists of a
 * <i>sessionId</i> and a <i>requestSequenceNumber</i>. The <i>sessionId</i> is used to represent the
 * communication between Segment clients and the SegmentStore which uses an underlying
 * connection pool. This <i>sessionId</i> is unique per connection pool.
 * The <i>requestSequenceNumber</i> is used to represent the requestSequence for a given session.
 */
@ToString
public class Session {
    private static final AtomicInteger ID_GENERATOR = new AtomicInteger(0);
    @Getter
    private final int sessionId;
    @GuardedBy("$LOCK")
    private int requestSequenceNumber = 0;

    @VisibleForTesting
    public Session(int sessionId, int requestSequenceNumber) {
        this.sessionId = sessionId;
        this.requestSequenceNumber = requestSequenceNumber;
    }

    /**
     * Create a new Session.
     * @return Session.
     */
    public static Session create() {
        return new Session(ID_GENERATOR.updateAndGet(i -> i == Integer.MAX_VALUE ? 0 : i + 1), 0);
    }

    /**
     * Obtain a Session from a {@code long} representation.
     * @param sessionAsLong a {@code long} representation of {@link Session}.
     * @return Session.
     */
    public static Session from(long sessionAsLong) {
        return new Session((int) (sessionAsLong >> 32), (int) sessionAsLong);
    }

    /**
     * Return a {@code long} representation of {@link Session}.
     * @return long representation.
     */
    @Synchronized
    public long asLong() {
        return ((long) sessionId << 32) | ((long) requestSequenceNumber & 0xFFFFFFFL);
    }

    /**
     * Obtain a {@code long} representation of {@link Session} with the next sequence number.
     * @return {@code long} representation of {@link Session} with the next sequence number.
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
