/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.shared.protocol.netty;

import io.netty.buffer.ByteBuf;
import java.util.UUID;
import lombok.Data;

@Data
public class Append implements Request, Comparable<Append> {
    final String segment;
    final UUID writerId;
    final AppendSequence eventNumber;
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
        return eventNumber.compareTo(other.getEventNumber());
    }
}
