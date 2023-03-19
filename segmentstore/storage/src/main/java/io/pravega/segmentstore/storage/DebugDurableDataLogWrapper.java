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
package io.pravega.segmentstore.storage;

public interface DebugDurableDataLogWrapper extends AutoCloseable {
    /**
     * Creates a special DurableDataLog wrapping the underlying log that only supports reading from the log. It does
     * not support initialization or otherwise modifications to the log. Accessing this log will not interfere with other
     * active writes to this log (i.e., it will not fence anyone out or close Ledgers that shouldn't be closed).
     *
     * @return A new DurableDataLog instance.
     * @throws DataLogInitializationException If an exception occurred fetching metadata from ZooKeeper.
     */
    DurableDataLog asReadOnly() throws DataLogInitializationException;

    /**
     * Loads a fresh copy BookKeeperLog Metadata from ZooKeeper, without doing any sort of fencing or otherwise modifying
     * it.
     *
     * @return A new instance of the LogMetadata class, or null if no such metadata exists (most likely due to this being
     * the first time accessing this log).
     * @throws DataLogInitializationException If an Exception occurred.
     */
    ReadOnlyLogMetadata fetchMetadata() throws DataLogInitializationException;

    /**
     * Allows to overwrite the metadata of a {@link DurableDataLog}. Warning: This should only be used for repair and/or
     * administration purposes.
     *
     * @param metadata Metadata to use to overwrite.
     * @throws DurableDataLogException if a problem manipulating the {@link DurableDataLog} metadata is found.
     */
    void forceMetadataOverWrite(ReadOnlyLogMetadata metadata) throws DurableDataLogException;

    /**
     * Allows to override the epoch of the current DurableDataLog Metadata. This method has been introduced
     * to help with cluster recoveries. (not to be used otherwise).
     *
     * @param epoch The epoch to override in the Log Metadata.
     * @throws DurableDataLogException any Exception while overriding the metadata.
     */
    void overrideEpochInMetadata(long epoch) throws DurableDataLogException;

    /**
     * Completely deletes the metadata of a {@link DurableDataLog}. Warning: This should only be used for repair and/or
     * administration purposes.
     *
     * @throws DurableDataLogException if a problem manipulating the {@link DurableDataLog} metadata is found.
     */
    void deleteDurableLogMetadata() throws DurableDataLogException;

}
