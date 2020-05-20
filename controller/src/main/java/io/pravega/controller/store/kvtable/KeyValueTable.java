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
import io.pravega.client.tables.KeyValueTableConfiguration;
import io.pravega.controller.store.kvtable.records.KVTableConfigurationRecord;
import io.pravega.controller.store.stream.records.ActiveTxnRecord;
import io.pravega.controller.store.stream.records.CommittingTransactionsRecord;
import io.pravega.controller.store.stream.records.EpochRecord;
import io.pravega.controller.store.stream.records.EpochTransitionRecord;
import io.pravega.controller.store.stream.records.HistoryTimeSeries;
import io.pravega.controller.store.stream.records.RetentionSet;
import io.pravega.controller.store.stream.records.SealedSegmentsMapShard;
import io.pravega.controller.store.stream.records.StreamConfigurationRecord;
import io.pravega.controller.store.stream.records.StreamCutRecord;
import io.pravega.controller.store.stream.records.StreamCutReferenceRecord;
import io.pravega.controller.store.stream.records.StreamSegmentRecord;
import io.pravega.controller.store.stream.records.StreamTruncationRecord;
import io.pravega.controller.store.stream.records.WriterMark;
import io.pravega.controller.store.VersionedMetadata;

import java.util.AbstractMap.SimpleEntry;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Properties of a stream and operations that can be performed on it.
 * Identifier for a stream is its name.
 */
interface KeyValueTable {

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
    CompletableFuture<VersionedMetadata<KVTableConfigurationRecord>> getVersionedConfigurationRecord();

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
     * Method to get versioned Epoch Transition Record from store.
     * 
     * @return Future which when completed contains existing epoch transition record with version
     */
    //CompletableFuture<VersionedMetadata<EpochTransitionRecord>> getEpochTransition();

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
    
    /**
     * This method attempts to create a new Waiting Request node and set the processor's name in the node.
     * If a node already exists, this attempt is ignored.
     *
     * @param processorName name of the request processor that is waiting to get an opportunity for processing.
     * @return CompletableFuture which indicates that a node was either created successfully or records the failure.
     */
    //CompletableFuture<Void> createWaitingRequestIfAbsent(String processorName);

    /**
     * This method fetches existing waiting request processor's name if any. It returns null if no processor is waiting.
     *
     * @return CompletableFuture which has the name of the processor that had requested for a wait, or null if there was no
     * such request.
     */
    //CompletableFuture<String> getWaitingRequestProcessor();

    /**
     * Delete existing waiting request processor if the name of the existing matches suppied processor name.
     *
     * @param processorName processor whose record is to be deleted.
     * @return CompletableFuture which indicates completion of processing.
     */
    //CompletableFuture<Void> deleteWaitingRequestConditionally(String processorName);



    /**
     * Method to get the requested chunk of the HistoryTimeSeries.
     *
     * @param chunkNumber chunk number.
     * @return Completable future that, upon completion, holds the requested HistoryTimeSeries chunk.
     */
    //CompletableFuture<HistoryTimeSeries> getHistoryTimeSeriesChunk(int chunkNumber);
}
