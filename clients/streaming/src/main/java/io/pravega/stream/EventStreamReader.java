/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.stream;

import io.pravega.stream.impl.segment.NoSuchEventException;

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
     * {@link EventRead#getEvent()}. (As well as for most other fields)
     *
     * @param timeout An upper bound on how long the call may block before returning null.
     * @return An instance of {@link EventRead}, which contains the next event in the stream. In the case the timeout
     *         is reached, {@link EventRead#getEvent()} returns null.
     * @throws ReinitializationRequiredException Is throw in the event that
     *         {@link ReaderGroup#resetReadersToCheckpoint(Checkpoint)} or
     *         {@link ReaderGroup#alterConfig(ReaderGroupConfig, java.util.Set)} was called
     *         which requires readers to be reinitialized.
     */
    EventRead<T> readNextEvent(long timeout) throws ReinitializationRequiredException;

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
