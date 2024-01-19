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

import java.util.concurrent.CompletableFuture;

/**
 * Allows to read data from a specific segment.
 */
public interface SegmentReader<T> extends AutoCloseable {

    /**
     * Gets the next event in the segment. If there are no events currently available this will block up for
     * timeoutMillis waiting for them to arrive. If none do, an EventReadWithStatus {@link EventReadWithStatus} will be returned with null for
     * {@link EventReadWithStatus#getEvent()}.
     *
     * @param timeoutMillis An upper bound on how long the call may block before returning null.
     * @return An instance of {@link EventReadWithStatus}, which contains the next event in the segment. In the case the timeoutMillis
     *         is reached, {@link EventReadWithStatus#getEvent()} returns null.
     * @throws TruncatedDataException If the segment has been truncated beyond the current offset and the data cannot be read.
     */
    EventReadWithStatus<T> read(long timeoutMillis) throws TruncatedDataException;

    /**
     * Returns a future that will be completed when there is data available to be read.
     * @return a future.
     */
    CompletableFuture<Void> isAvailable();



    /**
     * The status of this segment reader.
     */
    enum Status {
        /** The next event is available right now. */
        AVAILABLE_NOW,
        /** The segment is not sealed and reader has read all the events. */
        AVAILABLE_LATER,
        /** The segment is sealed and reader has read all the events. */
        FINISHED
    }
}
