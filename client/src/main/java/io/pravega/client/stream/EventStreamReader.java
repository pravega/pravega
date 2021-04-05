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

import io.pravega.client.segment.impl.NoSuchEventException;

/**
 * A reader for a stream.
 * <p>
 * This class is safe to use across threads, but doing so will not increase performance.
 *
 * @param <T> The type of events being sent through this stream.
 */
public interface EventStreamReader<T> extends AutoCloseable {

    /**
     * Returns a window which represents the range of time that this reader is currently reading as
     * provided by writers via the {@link EventStreamWriter#noteTime(long)} API.
     * 
     * If no writers were providing timestamps at the current position in the stream `null` will be returned.
     *  
     * @param stream the stream to obtain a time window for.
     * @return A TimeWindow which bounds the current location in the stream, or null if one cannot be established.
     */
    TimeWindow getCurrentTimeWindow(Stream stream);
    
    /**
     * Gets the next event in the stream. If there are no events currently available this will block up for
     * timeoutMillis waiting for them to arrive. If none do, an EventRead will be returned with null for
     * {@link EventRead#getEvent()}. (As well as for most other fields)
     *
     * An EventRead with null for {@link EventRead#getEvent()} is returned when the Reader has read all events up to the
     * configured end {@link StreamCut} specified using {@link ReaderGroupConfig}.
     *<p>
     * Note: An EventRead with null for {@link EventRead#getEvent()} is returned when {@link EventRead#isCheckpoint()}
     * is true. A null can also be returned due to delays in the Pravega cluster.
     *
     * @param timeoutMillis An upper bound on how long the call may block before returning null.
     * @return An instance of {@link EventRead}, which contains the next event in the stream. In the case the timeoutMillis
     *         is reached, {@link EventRead#getEvent()} returns null.
     * @throws ReinitializationRequiredException Is thrown in the event that
     *             {@link ReaderGroup#resetReaderGroup(ReaderGroupConfig)} was called
     *             which requires readers to be reinitialized.
     * @throws TruncatedDataException if the data that would be read next has been truncated away
     *             and can no longer be read. (If following this readNextEvent is called again it
     *             will resume from the next available event.)
     */
    EventRead<T> readNextEvent(long timeoutMillis) throws ReinitializationRequiredException, TruncatedDataException;

    /**
     * Gets the configuration that this reader was created with.
     *
     * @return Reader configuration
     */
    ReaderConfig getConfig();

    /**
     * Re-read an event that was previously read, by passing the pointer returned from
     * {@link EventRead#getEventPointer()}.
     * This does not affect the current position of the reader.
     * <p>
     * This is a blocking call. Passing invalid offsets has undefined behavior.
     * 
     * @param pointer The pointer object to enable a random read of the event.
     * @return The event at the position specified by the provided pointer or null if the event has
     *         been deleted.
     * @throws NoSuchEventException Reader was not able to fetch the event.
     */
    T fetchEvent(EventPointer pointer) throws NoSuchEventException;

    /**
     * Close the reader. No further actions may be performed. If this reader is part of a
     * reader group, this will automatically invoke
     * {@link ReaderGroup#readerOffline(String, Position)}
     *
     * @see java.lang.AutoCloseable#close()
     */
    @Override
    void close();

    /**
     * Close the reader at a specific position. No further actions may be performed. If this reader is part of
     * a {@link ReaderGroup}, this will automatically invoke {@link ReaderGroup#readerOffline(String, Position)} with
     * the supplied position.
     *
     * @param position {@link Position} to use while reporting readerOffline on the {@link ReaderGroup}.
     */
    void closeAt(Position position);
}
