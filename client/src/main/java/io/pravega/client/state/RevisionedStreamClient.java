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
package io.pravega.client.state;

import io.pravega.client.stream.TruncatedDataException;
import java.util.Iterator;
import java.util.Map.Entry;

/**
 * Provides a stream that can be read and written to with strong consistency.
 * Each item read from the stream is accompanied by a Revision.
 * These can be provided on write to guarantee that the writer is aware of all data in the stream.
 * A specific location can also be marked, which can also be updated with strong consistency. 
 * @param <T> The type of data written.
 */
public interface RevisionedStreamClient<T> extends AutoCloseable {
    
    /**
     * Returns the oldest revision that reads can start from. 
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
     * Read all data after a specified revision to the end of the stream. The iterator returned will
     * stop once it reaches the end of the data that was in the stream at the time this method was
     * called.
     * 
     * @param start The location the iterator should start at.
     * @return An iterator over Revision, value pairs.
     * @throws TruncatedDataException If the data at start no longer exists because it has been
     *             truncated. IE: It is below {@link #fetchOldestRevision()}
     */
    Iterator<Entry<Revision, T>> readFrom(Revision start) throws TruncatedDataException;

    /**
     * Read all data from a given start revision to a given end revision. The returned iterator will
     * stop once it reaches the given end of the data that was in the stream at the time this method was
     * called.
     *
     * @param startRevision The location the iterator should start at.
     * @param endRevision The location the iterator should end at.
     * @return An iterator over Revision, value pairs.
     * @throws TruncatedDataException If the data at start no longer exists because it has been
     *             truncated. IE: It is below {@link #fetchOldestRevision()}
     */
    Iterator<Entry<Revision, T>> readRange(Revision startRevision, Revision endRevision) throws TruncatedDataException;

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
    
    /**
     * Removes all data through the revision provided. This will update
     * {@link #fetchOldestRevision()} to the provided revision. After this call returns if
     * {@link #readFrom(Revision)} is called with an older revision it will throw.
     * 
     * @param revision The revision that should be the new oldest Revision.
     */
    void truncateToRevision(Revision revision);
    
    /**
     * Closes the client and frees any resources associated with it. (It may no longer be used)
     * @see java.lang.AutoCloseable#close()
     */
    @Override
    abstract void close();

}