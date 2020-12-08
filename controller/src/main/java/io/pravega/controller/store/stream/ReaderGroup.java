/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
     * Get Scope Name.
     *
     * @return Name of scope.
     */
    String getScopeName();

    /**
     * Create the stream, by creating/modifying underlying data structures.
     *
     * @param configuration reader group configuration.
     * @param createTimestamp creation timestamp.
     * @return boolean indicating success.
     */
    CompletableFuture<Void> create(final ReaderGroupConfig configuration, final long createTimestamp);

    /**
     * Deletes a Reader Group.
     *
     * @return boolean indicating success.
     */
    CompletableFuture<Void> delete();

    /**
     * Api to get creation time of the stream.
     * 
     * @return CompletableFuture which, upon completion, has the creation time of the stream. 
     */
    CompletableFuture<Long> getCreationTime();
    
    /**
     * Starts updating the configuration of an existing stream.
     *
     * @param configuration new stream configuration.
     * @return future of new StreamConfigWithVersion.
     */
    CompletableFuture<Void> startUpdateConfiguration(final ReaderGroupConfig configuration);

    /**
     * Completes an ongoing updates configuration of an existing stream.
     *
     * @return future of new StreamConfigWithVersion.
     * @param existing
     */
    CompletableFuture<Void> completeUpdateConfiguration(VersionedMetadata<ReaderGroupConfig> existing);

    /**
     * Fetches the current stream configuration.
     *
     * @return current stream configuration.
     */
    CompletableFuture<ReaderGroupConfig> getConfiguration();

    /**
     * Fetches the current stream configuration.
     *
     * @return current stream configuration.
     */
    CompletableFuture<VersionedMetadata<ReaderGroupConfigRecord>> getVersionedConfigurationRecord();



    /**
     * Api to get the current state with its current version.
     *
     * @return Future which when completed has the versioned state.
     */
    CompletableFuture<VersionedMetadata<ReaderGroupState>> getVersionedState();

    /**
     * Update the state of the stream.
     *
     * @return boolean indicating whether the state of stream is updated.
     */
    CompletableFuture<Void> updateState(final ReaderGroupState state);

    /**
     * Api to update versioned state as a CAS operation.
     *
     * @param state desired state
     * @return Future which when completed contains the updated state and version if successful or exception otherwise.
     */
    CompletableFuture<VersionedMetadata<ReaderGroupState>> updateVersionedState(final VersionedMetadata<ReaderGroupState> state,
                                                                                final ReaderGroupState newState);
    
    /**
     * Get the state of the reader group.
     *
     * @return state of the given reader group.
     * @param ignoreCached ignore cached value and fetch from store
     */
    CompletableFuture<ReaderGroupState> getState(boolean ignoreCached);

    /**
     * Refresh the reader group object. Typically to be used to invalidate any caches.
     * This allows us reuse of reader group object without having to recreate a new object for each new operation
     */
    void refresh();

  }
