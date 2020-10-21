/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.shared.protocol;

import io.pravega.common.util.ArrayView;
import io.pravega.shared.protocol.WireCommands.Event;
import java.util.UUID;
import lombok.Data;
import lombok.RequiredArgsConstructor;

@Data
@RequiredArgsConstructor
public class Append implements Request, Comparable<Append> {
    final String segment;
    final UUID writerId;
    final long eventNumber;
    final int eventCount;
    final ArrayView data;
    final Long expectedLength;
    final long flowId;

    public Append(String segment, UUID writerId, long eventNumber, Event event, long flowId) {
        this(segment, writerId, eventNumber, 1, event.asArrayView(), null, flowId);
    }
    
    public Append(String segment, UUID writerId, long eventNumber, Event event, long expectedLength, long flowId) {
        this(segment, writerId, eventNumber, 1, event.asArrayView(), expectedLength, flowId);
    }

    public int getDataLength() {
        return data.getLength();
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

    @Override
    public long getRequestId() {
        return flowId;
    }
}
