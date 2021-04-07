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
package io.pravega.controller.store.kvtable;

import io.pravega.client.tables.KeyValueTableConfiguration;
import io.pravega.controller.store.VersionedMetadata;
import io.pravega.controller.store.kvtable.records.KVTEpochRecord;
import io.pravega.controller.store.kvtable.records.KVTSegmentRecord;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * Properties of a KeyValueTable and operations that can be performed on it.
 * Identifier for a KeyValueTable is its name.
 */
public interface KeyValueTable {

    /**
     * Get Scope Name.
     *
     * @return Name of scope.
     */
    String getScopeName();

    /**
     * Get name of stream.
     *
     * @return Name of stream.
     */
    String getName();

    /**
     * Create the stream, by creating/modifying underlying data structures.
     *
     * @param configuration kvtable configuration.
     * @param createTimestamp kvtable configuration.
     * @param startingSegmentNumber kvtable starting segment number.
     * @return boolean indicating success.
     */
    CompletableFuture<CreateKVTableResponse> create(final KeyValueTableConfiguration configuration, final long createTimestamp, final int startingSegmentNumber);


    /**
     * Refresh the KeyValueTable object. Typically to be used to invalidate any caches.
     * This allows us reuse of KVTable object without having to recreate a new object for each new operation
     */
    void refresh();

    /**
     * Api to get creation time of the stream.
     * 
     * @return CompletableFuture which, upon completion, has the creation time of the stream. 
     */
    CompletableFuture<Long> getCreationTime();

    /**
     * Fetches the current stream configuration.
     *
     * @return current kvtable configuration.
     */
    CompletableFuture<KeyValueTableConfiguration> getConfiguration();

    /**
     * Api to get the current state with its current version.
     *
     * @return Future which when completed has the versioned state.
     */
    CompletableFuture<VersionedMetadata<KVTableState>> getVersionedState();

    /**
     * Update the state of the stream.
     * @param state desired state
     * @return void
     */
    CompletableFuture<Void> updateState(final KVTableState state);

    /**
     * Api to update versioned state as a CAS operation.
     *
     * @param state existing state
     * @param newState desired state
     * @return Future which when completed contains the updated state and version if successful or exception otherwise.
     */
    CompletableFuture<VersionedMetadata<KVTableState>> updateVersionedState(final VersionedMetadata<KVTableState> state, final KVTableState newState);
    
    /**
     * Get the state of the kvtable.
     *
     * @return state othe given kvtable.
     * @param ignoreCached ignore cached value and fetch from store
     */
    CompletableFuture<KVTableState> getState(boolean ignoreCached);

    /**
     * Get the UUID of the kvtable.
     * @return UUID of the given kvtable.
     */
    CompletableFuture<String> getId();

    /**
     * Method to get current active segments of the kvtable.
     *
     * @return Future which when completed will contain currently active segments
     */
    CompletableFuture<List<KVTSegmentRecord>> getActiveSegments();

    /**
     * Returns the epoch record corresponding to supplied epoch.
     *
     * @param epoch epoch to retrieve record for
     * @return CompletableFuture which on completion will have the epoch record corresponding to the given epoch
     */
    CompletableFuture<KVTEpochRecord> getEpochRecord(int epoch);

    /**
     * Returns the currently active KeyValueTable epoch.
     *
     * @param ignoreCached if ignore cache is set to true then fetch the value from the store.
     * @return currently active kvtable epoch.
     */
    CompletableFuture<KVTEpochRecord> getActiveEpochRecord(boolean ignoreCached);

    /**
     * Fetches all segment ids in the KeyValueTable.
     *
     * @return Future which when completed contains a list of all segments in the KeyValueTable.
     */
    CompletableFuture<Set<Long>> getAllSegmentIds();

    /**
     * Deletes this KeyValueTable.
     */
    CompletableFuture<Void> delete();

}
