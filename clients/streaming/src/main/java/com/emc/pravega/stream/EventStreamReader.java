/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.emc.pravega.stream;

import com.emc.pravega.StreamManager;
import com.emc.pravega.stream.impl.EventReadImpl;
import com.emc.pravega.stream.impl.segment.NoSuchEventException;

/**
 * A reader for a stream.
 * 
 * This class is safe to use across threads, but doing so will not increase performance.
 *
 * @param <T> The type of events being sent through this stream.
 */
public interface EventStreamReader<T> extends AutoCloseable {

    /**
     * Gets the next event in the stream. If there are no events currently available this will block up for
     * timeout waiting for them to arrive. If none do, an EventRead will be returned with null for
     * {@link EventRead#getEvent()}. (As well as for most other fields) However the
     * {@link EventRead#getEventSequence()} will be populated. (This is useful for applications that want to
     * be sure they have read all the events within a time range.)
     *
     * @param timeout An upper bound on how long the call may block before returning null.
     * @return The next event in the stream, or null if timeout is reached.
     * @throws ReinitializationRequiredException Is throw in the event that
     *         {@link ReaderGroup#resetReadersToCheckpoint(Checkpoint)} or
     *         {@link StreamManager#alterReaderGroup(String, ReaderGroupConfig, java.util.List)} was called
     *         which requires readers to be reinitialized.
     */
    EventRead<T> readNextEvent(long timeout) throws ReinitializationRequiredException;

    /**
     * Gets the configuration that this reader was created with.
     */
    ReaderConfig getConfig();

    /**
     * Re-read an event that was previously read, by passing the pointer returned from
     * {@link EventReadImpl#getEventPointer()}.
     * This does not affect the current position of the reader.
     * 
     * This is a blocking call. Passing invalid offsets has undefined behavior.
     * 
     * @param pointer The pointer object to enable a random read of the event.
     * @return The event at the position specified by the provided pointer or null if the event has
     *         been deleted.
     * @throws NoSuchEventException Reader was not able to fetch the event.
     */
    T read(EventPointer pointer) throws NoSuchEventException;

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
