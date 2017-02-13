/**
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
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
package com.emc.pravega.stream;

import com.emc.pravega.stream.impl.EventReadImpl;

/**
 * A reader for a stream.
 * 
 * This class is safe to use across threads, but doing so will not increase performance.
 *
 * @param <T> The type of events being sent through this stream.
 */
public interface EventStreamReader<T> extends AutoCloseable {

    /**
     * Gets the next event in the stream. If there are no events currently available this will block
     * up for timeout waiting for them to arrive. If none do, an EventRead will be returned with
     * null for {@link EventRead#getEvent()}. (As well as for most other fields) However the
     * {@link EventRead#getEventSequence()} will be populated. (This is useful for applications that
     * want to be sure they have read all the events within a time range.)
     *
     * @param timeout An upper bound on how long the call may block before returning null.
     * @return The next event in the stream, or null if timeout is reached.
     */
    EventRead<T> readNextEvent(long timeout);

    /**
     * Gets the configuration that this reader was created with.
     */
    ReaderConfig getConfig();

    /**
     * Re-read an event that was previously read, by passing the segment returned from
     * {@link EventReadImpl#getSegment()} and the offset returned from
     * {@link EventReadImpl#getOffsetInSegment()} 
     * This does not affect the current position of the reader.
     * 
     * This is a blocking call. Passing invalid offsets has undefined behavior.
     * 
     * @param segment The segment to read the event from.
     * @param offset The byte offset within the segment to read from.
     * @return The event at the position specified by the provided pointer or null if the event has
     *         been deleted.
     */
    T read(Segment segment, long offset);

    /**
     * Close the reader. No further actions may be performed. If this reader is part of a
     * reader group, this will automatically invoke
     * {@link ReaderGroup#readerOffline(String, Position)}
     *
     * @see java.lang.AutoCloseable#close()
     */
    @Override
    void close();
}
