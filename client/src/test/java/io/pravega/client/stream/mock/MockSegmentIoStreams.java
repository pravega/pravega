/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
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
package io.pravega.client.stream.mock;

import io.pravega.client.segment.impl.EndOfSegmentException;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.segment.impl.SegmentInputStream;
import io.pravega.client.segment.impl.SegmentOutputStream;
import io.pravega.client.segment.impl.SegmentSealedException;
import io.pravega.client.stream.impl.PendingEvent;
import io.pravega.shared.protocol.netty.WireCommands;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import javax.annotation.concurrent.GuardedBy;
import lombok.RequiredArgsConstructor;
import lombok.Synchronized;

@RequiredArgsConstructor
public class MockSegmentIoStreams implements SegmentOutputStream, SegmentInputStream {

    private final Segment segment;
    @GuardedBy("$lock")
    private int readIndex; 
    @GuardedBy("$lock")
    private int eventsWritten = 0;
    @GuardedBy("$lock")
    private long writeOffset = 0;
    @GuardedBy("$lock")
    private final ArrayList<ByteBuffer> dataWritten = new ArrayList<>();
    @GuardedBy("$lock")
    private final ArrayList<Long> offsetList = new ArrayList<>(); 
    
    @Override
    @Synchronized
    public void setOffset(long offset) {
        int index = offsetList.indexOf(offset);
        if (index < 0) {
            throw new IllegalArgumentException("There is not an entry at offset: " + offset);
        }
        readIndex = index;
    }

    @Override
    @Synchronized
    public long getOffset() {
        if (readIndex <= 0) {
            return 0;
        } else if (readIndex >= eventsWritten) {
            return writeOffset;
        }
        return offsetList.get(readIndex);
    }

    @Override
    @Synchronized
    public long fetchCurrentStreamLength() {
        return writeOffset;
    }

    
    @Override
    public ByteBuffer read() throws EndOfSegmentException {
        return read(Long.MAX_VALUE);
    }
    
    @Override
    @Synchronized
    public ByteBuffer read(long timeout) throws EndOfSegmentException {
        if (readIndex >= eventsWritten) {
            throw new EndOfSegmentException();
        }
        ByteBuffer buffer = dataWritten.get(readIndex);
        readIndex++;
        return buffer.slice();
    }

    @Override
    @Synchronized
    public void write(PendingEvent event) throws SegmentSealedException {
        if (event.getExpectedOffset() == null || event.getExpectedOffset() == writeOffset) {
            dataWritten.add(event.getData().slice());
            offsetList.add(writeOffset);
            eventsWritten++;
            writeOffset += event.getData().remaining() + WireCommands.TYPE_PLUS_LENGTH_SIZE;
            event.getAckFuture().complete(true);
        } else {
            event.getAckFuture().complete(false);
        }
    }
    
    @Override
    public void close() {
        //Noting to do.
    }

    @Override
    public void flush() throws SegmentSealedException {
        //Noting to do.
    }

    @Override
    public boolean canReadWithoutBlocking() {
        return true;
    }

    @Override
    public Segment getSegmentId() {
        return segment;
    }

    @Override
    public void fillBuffer() {
        //Noting to do.
    }

    @Override
    public Collection<PendingEvent> getUnackedEvents() {
        return Collections.emptyList();
    }

    @Override
    public String getSegmentName() {
        return segment.getScopedName();
    }

}
