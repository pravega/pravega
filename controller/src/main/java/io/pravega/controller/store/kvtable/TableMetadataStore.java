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
import io.pravega.controller.store.ArtifactStore;
import io.pravega.controller.store.VersionedMetadata;
import io.pravega.controller.stream.api.grpc.v1.Controller.CreateKeyValueTableStatus;
import io.pravega.shared.controller.event.ControllerEvent;
import io.pravega.controller.store.OperationContext;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Stream Metadata.
 */
public interface TableMetadataStore extends AutoCloseable, ArtifactStore {

    CompletableFuture<Boolean> checkScopeExists(String scope);
    /**
     * Creates a new stream with the given name and configuration.
     *
     * @param scopeName       scope name
     * @param kvtName         KeyValueTable name
     * @param executor        callers executor
     * @return boolean indicating whether the stream was created
     */
    CompletableFuture<UUID> createEntryForKVTable(final String scopeName,
                                                  final String kvtName,
                                                  final Executor executor);

    /**
     * Creates a new stream with the given name and configuration.
     *
     * @param scopeName       scope name
     * @param kvtName      stream name
     * @param configuration   stream configuration
     * @param createTimestamp stream creation timestamp
     * @param context         operation context
     * @param executor        callers executor
     * @return boolean indicating whether the stream was created
     */
    CompletableFuture<CreateKVTableResponse> createKeyValueTable(final String scopeName,
                                            final String kvtName,
                                            final KeyValueTableConfiguration configuration,
                                            final long createTimestamp,
                                            final OperationContext context,
                                            final Executor executor);

    /**
     * Api to check if a stream exists in the store or not.
     * @param scopeName scope name
     * @param kvtName KVTable name
     * @return true if stream exists, false otherwise
     */
    CompletableFuture<Boolean> checkKeyValueTableExists(final String scopeName,
                                                 final String kvtName);


    /**
     * Api to get creation time for the stream. 
     * 
     * @param scopeName       scope name
     * @param streamName      stream name
     * @param context         operation context
     * @param executor        callers executor
     * @return CompletableFuture, which when completed, will contain the creation time of the stream. 
     */
    CompletableFuture<Long> getCreationTime(final String scopeName,
                                            final String streamName,
                                            final OperationContext context,
                                            final Executor executor);
    
    /**
     * Api to Delete the stream related metadata.
     *
     * @param scopeName       scope name
     * @param streamName      stream name
     * @param context         operation context
     * @param executor        callers executor
     * @return future
     */
    /*
    CompletableFuture<Void> deleteKeyValueTable(final String scopeName,
                                         final String streamName,
                                         final KVTOperationContext context,
                                         final Executor executor);
*/
    /**
     * Api to set the state for stream in metadata.
     * @param scope scope name
     * @param name stream name
     * @param state stream state
     * @param context operation context
     * @param executor callers executor
     * @return Future of boolean if state update succeeded.
     */

    CompletableFuture<Void> setState(String scope, String name,
                                     KVTableState state, OperationContext context,
                                        Executor executor);


    /**
     * Api to get the state for stream from metadata.
     *
     * @param scope scope name
     * @param name stream name
     * @param ignoreCached ignore cached value and fetch from store.
     * @param context operation context
     * @param executor callers executor
     * @return Future of boolean if state update succeeded.
     */
    CompletableFuture<KVTableState> getState(final String scope, final String name, final boolean ignoreCached, final OperationContext context, final Executor executor);

    /**
     * Api to get the current state with its current version.
     *
     * @param scope scope
     * @param name stream
     * @param context operation context
     * @param executor executor
     * @return Future which when completed has the versioned state.
     */

    CompletableFuture<VersionedMetadata<KVTableState>> getVersionedState(final String scope, final String name,
                                                                         final OperationContext context, final Executor executor);


    /**
     * Api to update versioned state as a CAS operation.
     *
     * @param scope scope name
     * @param name kvTable name
     * @param state desired state
     * @param previous current state with version
     * @param context operation context
     * @param executor executor
     * @return Future which when completed contains the updated state and version if successful or exception otherwise.
     */

    CompletableFuture<VersionedMetadata<KVTableState>> updateVersionedState(final String scope, final String name,
                                                    final KVTableState state, final VersionedMetadata<KVTableState> previous,
                                                    final OperationContext context,
                                                    final Executor executor);


    /**
     * List existing streams in scopes.
     *
     * @param scopeName Name of the scope
     * @return A map of streams in scope to their configurations
     */
    //CompletableFuture<Map<String, StreamConfiguration>> listKVTablesInScope(final String scopeName);

    /**
     * List existing streams in scopes with pagination. This api continues listing streams from the supplied continuation token
     * and returns a count limited list of streams and a new continuation token.
     *
     * @param scopeName Name of the scope
     * @param continuationToken continuation token
     * @param limit limit on number of streams to return.
     * @param executor 
     * @return A pair of list of streams in scope with the continuation token. 
     */
    /*
    CompletableFuture<Pair<List<String>, String>> listKVTables(final String scopeName, final String continuationToken,
                                                             final int limit, final Executor executor);
*/
    /**
     * List Scopes in cluster.
     *
     * @return List of scopes
     */
    //CompletableFuture<List<String>> listScopes();


    /**
     * Fetches the current stream configuration.
     *
     * @param scope    stream scope
     * @param name     stream name.
     * @param context  operation context
     * @param executor callers executor
     * @return current stream configuration.
     */
    /*
    CompletableFuture<KeyValueTableConfiguration> getConfiguration(final String scope, final String name,
                                                            final KVTOperationContext context,
                                                            final Executor executor);
                                                            */

    /**
     * Fetches the current stream configuration.
     *
     * @param scope        stream scope
     * @param name         stream name.
     * @param context      operation context
     * @param executor     callers executor
     * @return current stream configuration.
     */
    /*
    CompletableFuture<VersionedMetadata<StreamConfigurationRecord>> getConfigurationRecord(final String scope, final String name,
                                                                                           final KVTOperationContext context,
                                                                                           final Executor executor);
                                                                                           */

    /**
     * Get Segment.
     *
     * @param scope    stream scope
     * @param name     stream name.
     * @param number   segment number.
     * @param context  operation context
     * @param executor callers executor
     * @return segment at given number.
     */
    //CompletableFuture<StreamSegmentRecord> getSegment(final String scope, final String name, final long number, final KVTOperationContext context, final Executor executor);

    /**
     * Api to get all segments in the stream. 
     *
     * @param scope    stream scope
     * @param name     stream name.
     * @param context  operation context
     * @param executor callers executor
     *                 
     * @return Future, which when complete will contain a list of all segments in the stream. 
     */
    /*
    CompletableFuture<Set<Long>> getAllSegmentIds(final String scope, final String name, final KVTOperationContext context,
                                                   final Executor executor);
*/
    /**
     * Get active segments.
     *
     * @param scope    stream scope
     * @param name     stream name.
     * @param executor callers executor
     * @param context  operation context
     * @return currently active segments
     */
    /*
    CompletableFuture<List<StreamSegmentRecord>> getActiveSegments(final String scope, final String name,
                                                                   final KVTOperationContext context, final Executor executor);
    

*/
    /**
     * Returns the segments in the specified epoch of the specified stream.
     *
     * @param scope    scope.
     * @param stream   stream.
     * @param epoch    epoch.
     * @param context  operation context
     * @param executor callers executor
     * @return         list of active segments in specified epoch.
     */
    /*
    CompletableFuture<List<StreamSegmentRecord>> getSegmentsInEpoch(final String scope,
                                                       final String stream,
                                                       final int epoch,
                                                       final KVTOperationContext context,
                                                       final Executor executor);
*/
    /**
     * Given a segment return a map containing the numbers of the segments immediately succeeding it
     * mapped to a list of the segments they succeed.
     *
     * @param scope         stream scope
     * @param streamName    stream name.
     * @param segmentId the segment number
     * @param context       operation context
     * @param executor      callers executor
     * @return segments that immediately follow the specified segment and the segments they follow.
     */
    /*
    CompletableFuture<Map<StreamSegmentRecord, List<Long>>> getSuccessors(final String scope,
                                                                                     final String streamName,
                                                                                     final long segmentId,
                                                                                     final KVTOperationContext context,
                                                                                     final Executor executor);

*/




    /**
     * Remove the specified host from the index.
     *
     * @param hostId Host identifier.
     * @return A future indicating completion of removal of the host from index.
     */
    //CompletableFuture<Void> removeHostFromIndex(String hostId);

    /**
     * Adds specified request in the host's task index. 
     * This is idempotent operation.
     *
     * @param hostId      Host identifier.
     * @param id          Unique id used while adding task to index.
     * @param request     Request to index.
     * @return            A future when completed will indicate that the task is indexed for the given host.
     */
    CompletableFuture<Void> addRequestToIndex(final String hostId, final String id, final ControllerEvent request);

    /**
     * Removes the index for task identified by `id` in host task index for host identified by `hostId`
     * This is idempotent operation.
     *
     * @param hostId Node whose child is to be removed.
     * @param id     Unique id used while adding task to index.
     * @return Future which when completed will indicate that the task has been removed from index.
     */
    CompletableFuture<Void> removeTaskFromIndex(final String hostId, final String id);

    /**
     * Returns a map of pending tasks that were created by the host but their corresponding event was probably not posted.
     *
     * @param hostId Host identifier.
     * @param limit number of tasks to retrieve from store
     * @return A CompletableFuture which when completed will have a map of tasks to events that should be posted.
     */
    //CompletableFuture<Map<String, ControllerEvent>> getPendingsTaskForHost(final String hostId, final int limit);

    /**
     * Remove the specified host from the index.
     *
     * @param hostId Host identifier.
     * @return A future indicating completion of removal of the host from index.
     */
    //CompletableFuture<Void> removeHostFromTaskIndex(String hostId);

    /**
     * Fetches set of hosts that own some tasks for which events have to be posted.
     *
     * @return set of hosts owning some pending tasks.
     */
    //CompletableFuture<Set<String>> listHostsWithPendingTask();

    /**
     * Returns the currently active epoch of the specified stream.
     *
     * @param scope    scope.
     * @param stream   stream.
     * @param context  operation context
     * @param ignoreCached  boolean indicating whether to use cached value or force fetch from underlying store.
     * @param executor callers executor
     * @return         Completable future that holds active epoch history record upon completion.
     */
    /*
    CompletableFuture<EpochRecord> getActiveEpoch(final String scope,
                                                  final String stream,
                                                  final KVTOperationContext context,
                                                  final boolean ignoreCached,
                                                  final Executor executor);
*/
    /**
     * Returns the record for the given epoch of the specified stream.
     *
     * @param scope    scope.
     * @param stream   stream.
     * @param epoch    epoch
     * @param context  operation context
     * @param executor callers executor
     * @return         Completable future that, upon completion, holds epoch history record corresponding to request epoch.
     */
    /*
    CompletableFuture<EpochRecord> getEpoch(final String scope,
                                              final String stream,
                                              final int epoch,
                                              final KVTOperationContext context,
                                              final Executor executor);
*/
}
