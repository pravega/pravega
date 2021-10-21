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
package io.pravega.client.byteStream.impl;

import io.pravega.client.byteStream.ByteStreamReader;
import io.pravega.client.segment.impl.EndOfSegmentException;
import io.pravega.client.segment.impl.SegmentInputStream;
import io.pravega.client.segment.impl.SegmentMetadataClient;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

@RequiredArgsConstructor
@ToString
public class ByteStreamReaderImpl extends ByteStreamReader {

    /**
     * Input is threadsafe but calls that update offset are guarded by a lock on input itself
     * because {@link #skip(long)} needs to return the number of bytes actually skipped which
     * requires two operations on input to be atomic.
     */
    @NonNull
    private final SegmentInputStream input;
    @NonNull
    private final SegmentMetadataClient meta;
    private final AtomicBoolean closed = new AtomicBoolean(false); 

    @Override
    public boolean isOpen() {
        return !closed.get();
    }

    @Override
    public long getOffset() {
        return input.getOffset();
    }

    @Override
    public void seekToOffset(long offset) {
        Exceptions.checkNotClosed(closed.get(), this);
        synchronized (input) {            
            input.setOffset(offset);
        }
    }

    @Override
    public int available() {
        Exceptions.checkNotClosed(closed.get(), this);
        return input.bytesInBuffer();
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            input.close();
            meta.close();
        }
    }

    @Override
    public long fetchHeadOffset() {
        return Futures.getThrowingException(meta.fetchCurrentSegmentHeadOffset());
    }


    @Override
    public long fetchTailOffset() {
        return Futures.getThrowingException(meta.fetchCurrentSegmentLength());
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        Exceptions.checkNotClosed(closed.get(), this);
        try {
            synchronized (input) {                
                return input.read(dst, Long.MAX_VALUE);
            }
        } catch (EndOfSegmentException e) {
            return -1;
        }
    }

    @Override
    public int read() throws IOException {
        Exceptions.checkNotClosed(closed.get(), this);
        ByteBuffer buffer = ByteBuffer.allocate(1);
        try {
            int read;
            synchronized (input) {                
                read = input.read(buffer, Long.MAX_VALUE);
            }
            if (read > 0) {
                buffer.flip();
                return buffer.get() & 0xFF;
            } else {
                return read;
            }
        } catch (EndOfSegmentException e) {
            return -1;
        }
    }

    @Override
    public int read(byte[] b) throws IOException {
        return read(ByteBuffer.wrap(b));
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        return read(ByteBuffer.wrap(b, off, len));
    }

    @Override
    public long skip(long toSkip) {
        Exceptions.checkNotClosed(closed.get(), this);
        long endOffset = fetchTailOffset();
        synchronized (input) {
            long offset = input.getOffset();
            long newOffset = Math.min(offset + toSkip, endOffset);
            input.setOffset(newOffset);
            return newOffset - offset;
        }
    }

    @Override
    public CompletableFuture<Integer> onDataAvailable() {
        Exceptions.checkNotClosed(closed.get(), this);
        return input.fillBuffer().thenApply(v -> available());
    }

}
