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

package io.pravega.client.stream;

import io.pravega.client.segment.impl.EndOfSegmentException;

import java.util.concurrent.CompletableFuture;

/**
 * Allows to read data from a specific segment.
 */
public interface SegmentReader<T> extends AutoCloseable {

    /**
     * Gets the next event in the segment. If there are no events currently available this will block up for
     * timeoutMillis waiting for them to arrive.
     *
     * @param timeoutMillis An upper bound on how long the call may block before returning null.
     * @return The next event in the segment. In the case if timeoutMillis is reached, it will return null.
     * @throws TruncatedDataException If the segment has been truncated beyond the current offset and the data cannot be read.
     * @throws EndOfSegmentException If segment is sealed and reader reached at the end of segment.
     */
    T read(long timeoutMillis) throws TruncatedDataException, EndOfSegmentException;

    /**
     * Returns a future that will be completed when there is data available to be read.
     * @return a future.
     */
    CompletableFuture<Void> isAvailable();

    /**
     * Gets the current snapshot of segment reader. It will provide information about segment reader's position,
     * segment itself and whether the reader has processed all events within the segment.
     *
     * @return The snapshot of segment reader.
     */
    SegmentReaderSnapshot getSnapshot();
}
