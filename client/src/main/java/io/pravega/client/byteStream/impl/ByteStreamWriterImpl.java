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

import com.google.common.annotations.VisibleForTesting;
import io.pravega.client.byteStream.ByteStreamWriter;
import io.pravega.client.segment.impl.SegmentMetadataClient;
import io.pravega.client.segment.impl.SegmentOutputStream;
import io.pravega.client.stream.impl.PendingEvent;
import io.pravega.common.concurrent.Futures;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Synchronized;

import javax.annotation.concurrent.GuardedBy;

@RequiredArgsConstructor
public class ByteStreamWriterImpl extends ByteStreamWriter {
    
    @NonNull
    private final SegmentOutputStream out;
    @NonNull
    private final SegmentMetadataClient meta;
    @GuardedBy("$lock")
    private CompletableFuture<Void> latestEventFuture;

    @Override
    public void write(int b) throws IOException {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public void write(ByteBuffer src) throws IOException {
        out.write(PendingEvent.withoutHeader(null, src, updateLastEventFuture()));
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        ByteBuffer data = ByteBuffer.wrap(b, off, len);
        out.write(PendingEvent.withoutHeader(null, data, updateLastEventFuture()));
    }

    @VisibleForTesting
    @Synchronized
    CompletableFuture<Void> updateLastEventFuture() {
        this.latestEventFuture = new CompletableFuture<>();
        return latestEventFuture;
    }

    @Override
    public void close() throws IOException {
        out.close();
        meta.close();
    }

    @Override
    @Synchronized
    public void flush() throws IOException {
        out.flush();
    }

    @Override
    @Synchronized
    public CompletableFuture<Void> flushAsync() {
        out.flushAsync();
        return this.latestEventFuture;
    }

    @Override
    public void closeAndSeal() throws IOException {
        out.close();
        Futures.getThrowingException(meta.sealSegment());
        meta.close();
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
    public void truncateDataBefore(long offset) {
        Futures.getThrowingException(meta.truncateSegment(offset));
    }

}
