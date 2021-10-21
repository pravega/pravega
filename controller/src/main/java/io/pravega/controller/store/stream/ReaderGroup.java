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
package io.pravega.controller.store.stream;

import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.controller.store.VersionedMetadata;
import io.pravega.controller.store.stream.records.ReaderGroupConfigRecord;

import java.util.concurrent.CompletableFuture;

/**
 * Reader Group Interface
 */
interface ReaderGroup {

    String getScope();

    /**
     * Get name of stream.
     *
     * @return Name of stream.
     */
    String getName();

    /**
     * Create the stream, by creating/modifying underlying data structures.
     *
     * @param configuration reader group configuration.
     * @param createTimestamp creation timestamp.
     * @return void.
     */
    CompletableFuture<Void> create(final ReaderGroupConfig configuration, final long createTimestamp, OperationContext context);

    /**
     * Deletes a Reader Group.
     *
     * @return void.
     */
    CompletableFuture<Void> delete(OperationContext context);
    
    /**
     * Starts updating the configuration of an existing stream.
     *
     * @param configuration new stream configuration.
     * @return future of new StreamConfigWithVersion.
     */
    CompletableFuture<Void> startUpdateConfiguration(final ReaderGroupConfig configuration, OperationContext context);

    /**
     * Completes an ongoing updates configuration of an existing Reader Group.
     * @param existing - existing ReaderGroupConfigRecord
     */
    CompletableFuture<Void> completeUpdateConfiguration(VersionedMetadata<ReaderGroupConfigRecord> existing, OperationContext context);

    /**
     * Fetches the current ReaderGroup configuration.
     *
     * @return current ReaderGroup configuration.
     */
    CompletableFuture<VersionedMetadata<ReaderGroupConfigRecord>> getVersionedConfigurationRecord(OperationContext context);

    /**
     * Api to get the current state with its current version.
     *
     * @return Future which when completed has the versioned state.
     */
    CompletableFuture<VersionedMetadata<ReaderGroupState>> getVersionedState(OperationContext context);

    /**
     * Api to update versioned state as a CAS operation.
     *
     * @param state desired state
     * @return Future which when completed contains the updated state and version if successful or exception otherwise.
     */
    CompletableFuture<VersionedMetadata<ReaderGroupState>> updateVersionedState(final VersionedMetadata<ReaderGroupState> state,
                                                                                final ReaderGroupState newState, OperationContext context);
    
    /**
     * Get the state of the reader group.
     *
     * @return state of the given reader group.
     * @param ignoreCached ignore cached value and fetch from store
     */
    CompletableFuture<ReaderGroupState> getState(boolean ignoreCached, OperationContext context);

    /**
     * Refresh the reader group object. Typically to be used to invalidate any caches.
     * This allows us reuse of reader group object without having to recreate a new object for each new operation
     */
    void refresh();
}
