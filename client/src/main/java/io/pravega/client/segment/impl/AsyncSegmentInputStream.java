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
package io.pravega.client.segment.impl;

import io.pravega.shared.protocol.netty.WireCommands.SegmentRead;
import java.util.concurrent.CompletableFuture;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

/**
 * Allows for reading from a Segment asynchronously.
 */
@RequiredArgsConstructor
@ToString
abstract class AsyncSegmentInputStream implements AutoCloseable {
    @Getter
    protected final Segment segmentId;

    /**
     * Reads from the Segment at the specified offset asynchronously.
     * 
     * 
     * @param offset The offset in the segment to read from
     * @param length The suggested number of bytes to read. (Note the result may contain either more or less than this
     *            value.)
     * @return A future for the result of the read call. If an exception occurred, it will be completed with the causing
     * exception. Notable exceptions:
     * * {@link SegmentTruncatedException} If the segment is truncated or it does not exist.
     * * {@link io.pravega.client.stream.impl.ConnectionClosedException} If the connection is closed due an exception while performing the read.
     * * {@link io.pravega.common.util.RetriesExhaustedException} If the configured number of retry attempts to read failed.
     */
    public abstract CompletableFuture<SegmentRead> read(long offset, int length);

    @Override
    public abstract void close();
    
    public abstract boolean isClosed();
}
