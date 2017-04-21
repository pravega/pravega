/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.state;

import java.util.Iterator;
import java.util.Map.Entry;

/**
 * Provides a stream that can be read and written to with strong consistency.
 * Each item read from the stream is accompanied by a Revision.
 * These can be provided on write to guarantee that the writer is aware of all data in the stream.
 */
public interface RevisionedStreamClient<T> {
    
    /**
     * Returns the latest revision.
     *
     * @return Latest revision.
     *
     */
    Revision fetchRevision();
    
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

}