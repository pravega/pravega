/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.shared.protocol.netty;

import java.util.UUID;

import io.netty.buffer.ByteBuf;
import lombok.Data;

@Data
public class Append implements Request, Comparable<Append> {
    final String segment;
    final UUID connectionId;
    final long eventNumber;
    final int eventCount;
    final ByteBuf data;
    final Long expectedLength;

    public Append(String segment, UUID connectionId, long eventNumber, ByteBuf data, Long expectedLength) {
        this(segment, connectionId, eventNumber, 1, data, expectedLength);
    }

    public Append(String segment, UUID connectionId, long eventNumber, int eventCount, ByteBuf data, Long expectedLength) {
        this.segment = segment;
        this.connectionId = connectionId;
        this.eventNumber = eventNumber;
        this.eventCount = eventCount;
        this.data = data;
        this.expectedLength = expectedLength;
    }

    public boolean isConditional() {
        return expectedLength != null;
    }

    @Override
    public void process(RequestProcessor cp) {
        cp.append(this);
    }

    @Override
    public int compareTo(Append other) {
        return Long.compare(eventNumber, other.eventNumber);
    }
}
