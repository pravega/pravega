/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.store.kvtable;

import io.pravega.client.tables.KeyValueTableConfiguration;
import io.pravega.controller.store.Artifact;
import io.pravega.controller.store.kvtable.records.KVTConfigurationRecord;
import io.pravega.controller.store.VersionedMetadata;

import java.util.concurrent.CompletableFuture;

/**
 * Properties of a stream and operations that can be performed on it.
 * Identifier for a stream is its name.
 */
public interface KeyValueTable extends Artifact {

    /**
     * Create the stream, by creating/modifying underlying data structures.
     *
     * @param configuration stream configuration.
     * @return boolean indicating success.
     */
    CompletableFuture<CreateKVTableResponse> create(final KeyValueTableConfiguration configuration, final long createTimestamp, final int startingSegmentNumber);


    /**
     * Refresh the KeyValueTable object. Typically to be used to invalidate any caches.
     * This allows us reuse of KVTable object without having to recreate a new object for each new operation
     */
    void refresh();

    /**
     * Deletes an already SEALED stream.
     *
     * @return boolean indicating success.
     */
    //CompletableFuture<Void> delete();

    /**
     * Api to get creation time of the stream.
     * 
     * @return CompletableFuture which, upon completion, has the creation time of the stream. 
     */
    CompletableFuture<Long> getCreationTime();

    /**
     * Fetches the current stream configuration.
     *
     * @return current stream configuration.
     */
    CompletableFuture<KeyValueTableConfiguration> getConfiguration();

    /**
     * Fetches the current stream configuration.
     *
     * @return current stream configuration.
     */
    CompletableFuture<VersionedMetadata<KVTConfigurationRecord>> getVersionedConfigurationRecord();

    /**
     * Api to get the current state with its current version.
     *
     * @return Future which when completed has the versioned state.
     */
    CompletableFuture<VersionedMetadata<KVTableState>> getVersionedState();

    /**
     * Update the state of the stream.
     *
     * @return boolean indicating whether the state of stream is updated.
     */
    CompletableFuture<Void> updateState(final KVTableState state);

    /**
     * Api to update versioned state as a CAS operation.
     *
     * @param state desired state
     * @return Future which when completed contains the updated state and version if successful or exception otherwise.
     */
    CompletableFuture<VersionedMetadata<KVTableState>> updateVersionedState(final VersionedMetadata<KVTableState> state, final KVTableState newState);
    
    /**
     * Get the state of the stream.
     *
     * @return state othe given stream.
     * @param ignoreCached ignore cached value and fetch from store
     */
    CompletableFuture<KVTableState> getState(boolean ignoreCached);

    /**
     * Get the UUID of the kvtable.
     * @return UUID of the given ktable.
     */
    CompletableFuture<String> getId();

    /**
     * Fetches details of specified segment.
     *
     * @param segmentId segment number.
     * @return segment at given number.
     */
    //CompletableFuture<StreamSegmentRecord> getSegment(final long segmentId);

    /**
     * Fetches all segment ids in the stream between head of the stream and tail of the stream. 
     *
     * @return Future which when completed contains a list of all segments in the stream.
     */
    //CompletableFuture<Set<Long>> getAllSegmentIds();

    /**
     * Method to get segments at the current tail of the stream.
     * 
     * @return Future which when completed will contain currently active segments
     */
    //CompletableFuture<List<StreamSegmentRecord>> getActiveSegments();
    

    /**
     * Returns the active segments in the specified epoch.
     *
     * @param epoch epoch number.
     * @return list of numbers of segments active in the specified epoch.
     */
    //CompletableFuture<List<StreamSegmentRecord>> getSegmentsInEpoch(int epoch);


    /**
     * Returns the currently active stream epoch.
     *
     * @param ignoreCached if ignore cache is set to true then fetch the value from the store. 
     * @return currently active stream epoch.
     */
    //CompletableFuture<EpochRecord> getActiveEpoch(boolean ignoreCached);

    /**
     * Returns the epoch record corresponding to supplied epoch.
     *
     * @param epoch epoch to retrieve record for
     * @return CompletableFuture which on completion will have the epoch record corresponding to the given epoch
     */
    //CompletableFuture<EpochRecord> getEpochRecord(int epoch);
    

}
