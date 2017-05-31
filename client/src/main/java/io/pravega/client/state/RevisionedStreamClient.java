/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.state;

import java.util.Iterator;
import java.util.Map.Entry;

/**
 * Provides a stream that can be read and written to with strong consistency.
 * Each item read from the stream is accompanied by a Revision.
 * These can be provided on write to guarantee that the writer is aware of all data in the stream.
 * A specific location can also be marked, which can also be updated with strong consistency. 
 * @param <T> The type of data written.
 */
public interface RevisionedStreamClient<T> {
    
    /**
     * Returns the oldest revision than can be read.
     *
     * @return The oldest readable revision.
     */
    Revision fetchOldestRevision();
    
    /**
     * Returns the latest revision.
     *
     * @return Latest revision.
     */
    Revision fetchLatestRevision();
    
    /**
     * Read from a specified revision to the end of the stream.
     * The iterator returned will stop once it reaches the end of what was in the stream at the time this method was called.
     * 
     * @param start The location the iterator should start at.
     * @return An iterator over Revision, value pairs.
     */
    Iterator<Entry<Revision, T>> readFrom(Revision start);

    /**
     * If the supplied revision is the latest revision in the stream write the provided value and return the new revision.
     * If the supplied revision is not the latest, nothing will occur and null will be returned.
     * 
     * @param latestRevision The version to verify is the most recent.
     * @param value The value to be written to the stream.
     * @return The new revision if the data was written successfully or null if it was not.
     */
    Revision writeConditionally(Revision latestRevision, T value);
    
    /**
     * Write a new value to the stream.
     * @param value The value to be written.
     */
    void writeUnconditionally(T value);
    
    /**
     * Returns a location previously set by {@link #compareAndSetMark(Revision, Revision)}.
     * @return The marked location. (null if setMark was never been called)
     */
    Revision getMark();
    
    /**
     * Records a provided location that can later be obtained by calling {@link #getMark()}.
     * Atomically set the mark to newLocation if it is the expected value.
     * @param expected The expected value (May be null to indicate the mark is expected to be null)
     * @param newLocation The new value 
     * @return true if it was successful. False if the mark was not the expected value.
     */
    boolean compareAndSetMark(Revision expected, Revision newLocation);

}