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

/**
 * Allows to read data from a specific segment.
 */
public interface SegmentReader<T> extends AutoCloseable {

    /**
     * Gets the next event in the stream. If there are no events currently available this will block up for
     * timeoutMillis waiting for them to arrive. If none do, an EventRead will be returned with null for
     * {@link ReadEventWithStatus#getEvent()}.
     *
     * @param timeoutMillis An upper bound on how long the call may block before returning null.
     * @return An instance of {@link ReadEventWithStatus}, which contains the next event in the segment. In the case the timeoutMillis
     *         is reached, {@link ReadEventWithStatus#getEvent()} returns null.
     */
    ReadEventWithStatus<T> read(long timeoutMillis);

    /**
     * Check the status of segment.
     * If segment have events to read then status will be AVAILABLE_NOW {@link Status#AVAILABLE_NOW}
     * If all the events in the segment has been read and segment is not sealed then the status will be AVAILABLE_LATER {@link Status#AVAILABLE_LATER}
     * If all the events in the segment has been read and segment is sealed then the status will be FINISHED {@link Status#FINISHED}
     *
     * @return Status of the segment.
     */
    Status checkStatus();

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
