/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
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
    final ByteBuf data;
    final Long expectedLength;
    
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
