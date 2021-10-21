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
package io.pravega.shared.protocol.netty;

import io.netty.buffer.ByteBuf;
import io.pravega.shared.protocol.netty.WireCommands.Event;
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
    final ByteBuf data;
    final Long expectedLength;
    final long flowId;

    public Append(String segment, UUID writerId, long eventNumber, Event event, long flowId) {
        this(segment, writerId, eventNumber, 1, event.getAsByteBuf(), null, flowId);
    }
    
    public Append(String segment, UUID writerId, long eventNumber, Event event, long expectedLength, long flowId) {
        this(segment, writerId, eventNumber, 1, event.getAsByteBuf(), expectedLength, flowId);
    }

    public int getDataLength() {
        return data.readableBytes();
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
