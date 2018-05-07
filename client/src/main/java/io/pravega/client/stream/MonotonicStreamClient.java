/**
  * Copyright (c) 2018 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.stream;

import java.util.Iterator;
import java.util.Map.Entry;

/**
 * Provides a stream that can be read and written to with strong consistency.
 * Each item written from the stream is accompanied by a long.
 * These are provided on write to guarantee that value is always increasing.
 * A specific location can also be marked, which can also be updated if it is newer. 
 * @param <T> The type of data written.
 */
public interface MonotonicStreamClient<T> extends AutoCloseable {
    
    public interface SequencedPosition {
        /**
         * Returns the sequence associated with this position.
         */
        long getSequence();
    }
    
    /**
     * Returns the oldest position that reads can start from. 
     *
     * @return The oldest readable position.
     */
    SequencedPosition fetchOldestPosition();
    
    /**
     * Returns the latest position.
     *
     * @return Latest position.
     */
    SequencedPosition fetchLatestPosition();
    
    /**
     * Read all data after a specified position to the end of the stream. The iterator returned will
     * stop once it reaches the end of the data that was in the stream at the time this method was
     * called.
     * 
     * @param start The position the iterator should start at.
     * @return An iterator over Sequence, value pairs.
     * @throws TruncatedDataException if the data at start is no longer available.
     */
    Iterator<Entry<SequencedPosition, T>> readFrom(SequencedPosition start) throws TruncatedDataException;

    /**
     * If the supplied sequence is larger than previously written sequences the value will be written and the position returned.
     * If the supplied sequence is less than or equal to previously written sequences, nothing will occur and null will be returned.
     * 
     * @param sequence The sequence to verify is greater than previously written sequences.
     * @param value The value to be written to the stream.
     * @return true if the data was written. False if the sequence was less than or equal to previously written values.
     */
    boolean writeValue(long sequence, T value);
    
    /**
     * Removes all data below the position provided. This will update {@link #fetchOldestPosition()}
     * to the provided position. After this call returns if {@link #readFrom(SequencedPosition)} is called
     * with an older position it will throw.
     * 
     * @param position The position that should be the new oldest position.
     */
    void truncateToPosition(SequencedPosition position);
    
    /**
     * Closes the client and frees any resources associated with it. (It may no longer be used).
     * @see java.lang.AutoCloseable#close()
     */
    @Override
    abstract void close();
}
