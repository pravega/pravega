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
import io.pravega.controller.store.Scope;
import io.pravega.controller.store.VersionedMetadata;
import io.pravega.controller.store.kvtable.records.KVTSegmentRecord;
import io.pravega.shared.controller.event.ControllerEvent;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * KeyValueTable Metadata Store.
 */
public interface KVTableMetadataStore extends AutoCloseable {

    /**
     * Method to create an operation context. A context ensures that multiple calls to store for the same data are avoided
     * within the same operation. All api signatures are changed to accept context. If context is supplied, the data will be
     * looked up within the context and, upon a cache miss, will be fetched from the external store and cached within the context.
     * Once an operation completes, the context is discarded.
     *
     * @param scope Stream scope.
     * @param name  Stream name.
     * @return Return a streamContext
     */
    KVTOperationContext createContext(final String scope, final String name);

    CompletableFuture<Boolean> checkScopeExists(String scope);

    /**
     * Creates a new stream with the given name and configuration.
     *
     * @param scopeName       scope name
     * @param kvtName         KeyValueTable name
     * @param id              Unique Identifier for KVTable
     * @param executor        callers executor
     * @return boolean indicating whether the stream was created
     */
    CompletableFuture<Void> createEntryForKVTable(final String scopeName,
                                                  final String kvtName,
                                                  final byte[] id,
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
                                            final KVTOperationContext context,
                                            final Executor executor);

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
                                            final KVTOperationContext context,
                                            final Executor executor);

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
                                     KVTableState state, KVTOperationContext context,
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
    CompletableFuture<KVTableState> getState(final String scope, final String name, final boolean ignoreCached, final KVTOperationContext context, final Executor executor);

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
                                                                         final KVTOperationContext context, final Executor executor);

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
                                                    final KVTOperationContext context,
                                                    final Executor executor);

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

    KeyValueTable getKVTable(String scope, final String name, KVTOperationContext context);

    /**
     * Get active segments.
     *
     * @param scope    kvtable scope
     * @param name     kvtable name.
     * @param executor callers executor
     * @param context  operation context
     * @return currently active segments
     */
    CompletableFuture<List<KVTSegmentRecord>> getActiveSegments(final String scope, final String name, final KVTOperationContext context, final Executor executor);

    /**
     * Fetches the current stream configuration.
     *
     * @param scope    stream scope
     * @param name     stream name.
     * @param context  operation context
     * @param executor callers executor
     * @return current stream configuration.
     */
    CompletableFuture<KeyValueTableConfiguration> getConfiguration(final String scope, final String name,
                                                            final KVTOperationContext context,
                                                            final Executor executor);

    /**
     * Returns a Scope object from scope identifier.
     *
     * @param scopeName scope identifier is scopeName.
     * @return Scope object.
     */
    Scope newScope(final String scopeName);
}
