/**
 *  Copyright (c) 2016 Dell Inc. or its subsidiaries. All Rights Reserved
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.emc.pravega.stream.mock;

import com.emc.pravega.common.netty.WireCommands;
import com.emc.pravega.stream.impl.segment.EndOfSegmentException;
import com.emc.pravega.stream.impl.segment.SegmentInputStream;
import com.emc.pravega.stream.impl.segment.SegmentOutputStream;
import com.emc.pravega.stream.impl.segment.SegmentSealedException;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;

import javax.annotation.concurrent.GuardedBy;

import lombok.Synchronized;

public class MockSegmentIoStreams implements SegmentOutputStream, SegmentInputStream {

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
    @Synchronized
    public ByteBuffer read() throws EndOfSegmentException {
        if (readIndex >= eventsWritten) {
            throw new EndOfSegmentException();
        }
        ByteBuffer buffer = dataWritten.get(readIndex);
        readIndex++;
        return buffer.slice();
    }

    @Override
    @Synchronized
    public void write(ByteBuffer buff, CompletableFuture<Boolean> onComplete) throws SegmentSealedException {
        dataWritten.add(buff.slice());
        offsetList.add(writeOffset);
        eventsWritten++;
        writeOffset += buff.remaining() + WireCommands.TYPE_PLUS_LENGTH_SIZE;
        onComplete.complete(true);
    }

    @Override
    @Synchronized
    public void conditionalWrite(long expectedLength, ByteBuffer buff, CompletableFuture<Boolean> onComplete)
            throws SegmentSealedException {
        if (writeOffset == expectedLength) {
            write(buff, onComplete);
        } else {
            onComplete.complete(false);
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

}
