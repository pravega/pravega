/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.store.stream;

import io.pravega.client.stream.RetentionPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.controller.server.retention.BucketChangeListener;
import io.pravega.controller.server.retention.BucketOwnershipListener;
import io.pravega.controller.store.stream.tables.ActiveTxnRecord;
import io.pravega.controller.store.stream.tables.CommittingTransactionsRecord;
import io.pravega.controller.store.stream.tables.EpochTransitionRecord;
import io.pravega.controller.store.stream.tables.HistoryRecord;
import io.pravega.controller.store.stream.tables.State;
import io.pravega.controller.store.stream.tables.StreamConfigurationRecord;
import io.pravega.controller.store.stream.tables.StreamCutRecord;
import io.pravega.controller.store.stream.tables.StreamTruncationRecord;
import io.pravega.controller.store.task.TxnResource;
import io.pravega.controller.stream.api.grpc.v1.Controller.CreateScopeStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.DeleteScopeStatus;

import java.util.AbstractMap.SimpleEntry;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Stream Metadata.
 */
public interface StreamMetadataStore {

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
    OperationContext createContext(final String scope, final String name);

    /**
     * Creates a new stream with the given name and configuration.
     *
     * @param scopeName       scope name
     * @param streamName      stream name
     * @param configuration   stream configuration
     * @param createTimestamp stream creation timestamp
     * @param context         operation context
     * @param executor        callers executor
     * @return boolean indicating whether the stream was created
     */
    CompletableFuture<CreateStreamResponse> createStream(final String scopeName,
                                            final String streamName,
                                            final StreamConfiguration configuration,
                                            final long createTimestamp,
                                            final OperationContext context,
                                            final Executor executor);

    /**
     * Api to check if a stream exists in the store or not.
     * @param scopeName scope name
     * @param streamName stream name
     * @return true if stream exists, false otherwise
     */
    CompletableFuture<Boolean> checkStreamExists(final String scopeName,
                                                 final String streamName);


    /**
     * Api to Delete the stream related metadata.
     *
     * @param scopeName       scope name
     * @param streamName      stream name
     * @param context         operation context
     * @param executor        callers executor
     * @return future
     */
    CompletableFuture<Void> deleteStream(final String scopeName,
                                         final String streamName,
                                         final OperationContext context,
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
    CompletableFuture<Boolean> setState(String scope, String name,
                                        State state, OperationContext context,
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
    CompletableFuture<State> getState(final String scope, final String name, final boolean ignoreCached, final OperationContext context, final Executor executor);

    /**
     * Creates a new scope with the given name.
     *
     * @param scopeName Scope name
     * @return null on success and exception on failure.
     */
    CompletableFuture<CreateScopeStatus> createScope(final String scopeName);

    /**
     * Deletes a Scope if contains no streams.
     *
     * @param scopeName Name of scope to be deleted
     * @return null on success and exception on failure.
     */
    CompletableFuture<DeleteScopeStatus> deleteScope(final String scopeName);

    /**
     * Retrieve configuration of scope.
     *
     * @param scopeName Name of scope.
     * @return Returns configuration of scope.
     */
    CompletableFuture<String> getScopeConfiguration(final String scopeName);

    /**
     * List existing streams in scopes.
     *
     * @param scopeName Name of the scope
     * @return List of streams in scope
     */
    CompletableFuture<List<StreamConfiguration>> listStreamsInScope(final String scopeName);

    /**
     * List Scopes in cluster.
     *
     * @return List of scopes
     */
    CompletableFuture<List<String>> listScopes();

    /**
     * Updates the configuration of an existing stream.
     *
     * @param scope         stream scope
     * @param name          stream name.
     * @param configuration new stream configuration.
     * @param context       operation context
     * @param executor      callers executor
     * @return Future of operation
     */
    CompletableFuture<Void> startUpdateConfiguration(final String scope,
                                                     final String name,
                                                     final StreamConfiguration configuration,
                                                     final OperationContext context,
                                                     final Executor executor);

    /**
     * Complete an ongoing update of stream configuration.
     *
     * @param scope         stream scope
     * @param name          stream name.
     * @param context       operation context
     * @param executor      callers executor
     * @return future of opration
     */
    CompletableFuture<Void> completeUpdateConfiguration(final String scope,
                                                        final String name,
                                                        final OperationContext context,
                                                        final Executor executor);

    /**
     * Fetches the current stream configuration.
     *
     * @param scope    stream scope
     * @param name     stream name.
     * @param context  operation context
     * @param executor callers executor
     * @return current stream configuration.
     */
    CompletableFuture<StreamConfiguration> getConfiguration(final String scope, final String name,
                                                            final OperationContext context,
                                                            final Executor executor);

    /**
     * Fetches the current stream configuration.
     *
     * @param scope        stream scope
     * @param name         stream name.
     * @param ignoreCached ignore cached value.
     * @param context      operation context
     * @param executor     callers executor
     * @return current stream configuration.
     */
    CompletableFuture<StreamConfigurationRecord> getConfigurationRecord(final String scope, final String name,
                                                                        final boolean ignoreCached,
                                                                        final OperationContext context,
                                                                        final Executor executor);

    /**
     * Start new stream truncation.
     *
     * @param scope         stream scope
     * @param name          stream name.
     * @param streamCut     new stream cut.
     * @param context       operation context
     * @param executor      callers executor
     * @return future of operation.
     */
    CompletableFuture<Void> startTruncation(final String scope,
                                            final String name,
                                            final Map<Long, Long> streamCut,
                                            final OperationContext context,
                                            final Executor executor);

    /**
     * Complete an ongoing stream truncation.
     *
     * @param scope               stream scope
     * @param name                stream name.
     * @param context             operation context
     * @param executor            callers executor
     * @return boolean indicating whether the stream was updated
     */
    CompletableFuture<Void> completeTruncation(final String scope,
                                               final String name,
                                               final OperationContext context,
                                               final Executor executor);

    /**
     * Fetches the current stream cut.
     *
     * @param scope        stream scope
     * @param name         stream name.
     * @param ignoreCached ignore cached value.
     * @param context      operation context
     * @param executor     callers executor
     * @return current truncation property.
     */
    CompletableFuture<StreamTruncationRecord> getTruncationRecord(final String scope, final String name,
                                                                  final boolean ignoreCached,
                                                                  final OperationContext context,
                                                                  final Executor executor);

    /**
     * Set the stream state to sealed.
     *
     * @param scope    stream scope
     * @param name     stream name.
     * @param context  operation context
     * @param executor callers executor
     * @return boolean indicating whether the stream was updated.
     */
    CompletableFuture<Boolean> setSealed(final String scope, final String name, final OperationContext context, final Executor executor);

    /**
     * Get the stream sealed status.
     *
     * @param scope    stream scope
     * @param name     stream name.
     * @param context  operation context
     * @param executor callers executor
     * @return boolean indicating whether the stream is sealed.
     */
    CompletableFuture<Boolean> isSealed(final String scope, final String name, final OperationContext context, final Executor executor);

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
    CompletableFuture<Segment> getSegment(final String scope, final String name, final long number, final OperationContext context, final Executor executor);

    /**
     * Get active segments.
     *
     * @param scope    stream scope
     * @param name     stream name.
     * @param executor callers executor
     * @param context  operation context
     * @return currently active segments
     */
    CompletableFuture<List<Segment>> getActiveSegments(final String scope, final String name, final OperationContext context, final Executor executor);

    /**
     * Get active segments at given timestamp.
     *
     * @param scope     stream scope
     * @param name      stream name.
     * @param timestamp point in time.
     * @param context   operation context
     * @param executor  callers executor
     * @return the list of segments numbers active at timestamp.
     */
    CompletableFuture<Map<Long, Long>> getActiveSegments(final String scope, final String name, final long timestamp, final OperationContext context, final Executor executor);

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
    CompletableFuture<List<Segment>> getActiveSegments(final String scope,
                                                       final String stream,
                                                       final int epoch,
                                                       final OperationContext context,
                                                       final Executor executor);

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
    CompletableFuture<List<Long>> getActiveSegmentIds(final String scope,
                                                                   final String stream,
                                                                   final int epoch,
                                                                   final OperationContext context,
                                                                   final Executor executor);

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
    CompletableFuture<Map<Long, List<Long>>> getSuccessors(final String scope,
                                                                                     final String streamName,
                                                                                     final long segmentId,
                                                                                     final OperationContext context,
                                                                                     final Executor executor);

    /**
     * Given two stream cuts, this method return a list of segments that lie between given stream cuts.
     *
     * @param scope      stream scope
     * @param streamName stream name.
     * @param from       from stream cut
     * @param to         to stream cut
     * @param context    operation context
     * @param executor   callers executor
     * @return Future which when completed contains list of segments between given stream cuts.
     */
    CompletableFuture<List<Segment>> getSegmentsBetweenStreamCuts(final String scope,
                                                           final String streamName,
                                                           final Map<Long, Long> from,
                                                           final Map<Long, Long> to,
                                                           final OperationContext context,
                                                           final Executor executor);

    /**
     * Method to validate stream cut based on its definition - disjoint sets that cover the entire range of keyspace.
     *
     * @param scope scope name
     * @param streamName stream name
     * @param streamCut stream cut to validate
     * @param context execution context
     * @param executor executor
     * @return Future which when completed has the result of validation check (true for valid and false for illegal streamCuts).
     */
    CompletableFuture<Boolean> isStreamCutValid(final String scope,
                                                final String streamName,
                                                final Map<Long, Long> streamCut,
                                                final OperationContext context,
                                                final Executor executor);

    /**
     * Scales in or out the currently set of active segments of a stream.
     *
     * @param scope          stream scope
     * @param name           stream name.
     * @param newRanges      new key ranges to be added to the stream which maps to a new segment per range in the stream
     * @param sealedSegments segments to be sealed
     * @param scaleTimestamp timestamp at which scale was requested
     * @param runOnlyIfStarted run only if the scale operation has already been started.
     * @param context        operation context
     * @param executor       callers executor
     * @return the list of newly created segments
     */
    CompletableFuture<EpochTransitionRecord> startScale(final String scope, final String name,
                                                            final List<Long> sealedSegments,
                                                            final List<SimpleEntry<Double, Double>> newRanges,
                                                            final long scaleTimestamp,
                                                            final boolean runOnlyIfStarted,
                                                            final OperationContext context,
                                                            final Executor executor);

    /**
     * Method to create new segments in stream metadata.
     *
     * @param scope          stream scope
     * @param name           stream name.
     * @param isManualScale  flag to indicate that the processing is being performed for manual scale
     * @param context        operation context
     * @param executor       callers executor
     * @return future
     */
    CompletableFuture<Void> scaleCreateNewSegments(final String scope,
                                                   final String name,
                                                   final boolean isManualScale,
                                                   final OperationContext context,
                                                   final Executor executor);

    /**
     * Called after new segments are created in SSS.
     *
     * @param scope          stream scope
     * @param name           stream name.
     * @param context        operation context
     * @param executor       callers executor
     * @return future
     */
    CompletableFuture<Void> scaleNewSegmentsCreated(final String scope,
                                                    final String name,
                                                    final OperationContext context,
                                                    final Executor executor);

    /**
     * Called after old segments are sealed in segment store.
     *
     * @param scope          stream scope
     * @param name           stream name.
     * @param sealedSegmentSizes sealed segments with size at the time of sealing
     * @param context        operation context
     * @param executor       callers executor
     * @return future
     */
    CompletableFuture<Void> scaleSegmentsSealed(final String scope, final String name,
                                                final Map<Long, Long> sealedSegmentSizes,
                                                final OperationContext context,
                                                final Executor executor);

    /**
     * This method is called from Rolling transaction workflow after new transactions that are duplicate of active transactions
     * have been created successfully in segment store.
     * This method will update metadata records for epoch to add two new epochs, one for duplicate txn epoch where transactions
     * are merged and the other for duplicate active epoch.
     *
     * @param scope          stream scope
     * @param name           stream name.
     * @param sealedTxnEpochSegments sealed segments from intermediate txn epoch with size at the time of sealing
     * @param txnEpoch       epoch for transactions that need to be rolled over
     * @param time           timestamp
     * @param context        operation context
     * @param executor       callers executor
     * @return CompletableFuture which upon completion will indicate that we have successfully created new epoch entries.
     */
    CompletableFuture<Void> rollingTxnNewSegmentsCreated(final String scope, final String name, Map<Long, Long> sealedTxnEpochSegments,
                                                         final int txnEpoch, final long time, final OperationContext context, final Executor executor);

    /**
     * This is final step of rolling transaction and is called after old segments are sealed in segment store.
     * This should complete the epoch transition in the metadata store.
     *
     * @param scope          stream scope
     * @param name           stream name.
     * @param sealedActiveEpochSegments sealed segments from active epoch with size at the time of sealing
     * @param activeEpoch    active epoch against which rolling txn was started
     * @param time           timestamp
     * @param context        operation context
     * @param executor       callers executor
     * @return CompletableFuture which upon successful completion will indicate that rolling transaction is complete.
     */
    CompletableFuture<Void> rollingTxnActiveEpochSealed(final String scope, final String name, final Map<Long, Long> sealedActiveEpochSegments,
                                                        final int activeEpoch, final long time, final OperationContext context, final Executor executor);


    /**
     * If the state of the stream in the store matches supplied state, reset.
     *
     * @param scope          stream scope
     * @param name           stream name.
     * @param state          state to match
     * @param context        operation context
     * @param executor       callers executor
     * @return future of completion of state update
     */
    CompletableFuture<Void> resetStateConditionally(final String scope, final String name,
                                                    final State state,
                                                    final OperationContext context,
                                                    final Executor executor);

    /**
     * Method to create a new unique transaction id on the stream.
     *
     * @param scopeName        Scope
     * @param streamName       Stream
     *                         the scaling operation is initiated on the txn stream.
     * @param context          operation context
     * @param executor         callers executor
     * @return Future when completed contains a new unique txn id.
     */
    CompletableFuture<UUID> generateTransactionId(final String scopeName, final String streamName,
                                                  final OperationContext context,
                                                  final Executor executor);

    /**
     * Method to create a new transaction on a stream.
     *
     * @param scopeName        Scope
     * @param streamName       Stream
     * @param txnId            Transaction identifier.
     * @param lease            Time for which transaction shall remain open with sending any heartbeat.
     * @param maxExecutionTime Maximum time for which client may extend txn lease.
     * @param context          operation context
     * @param executor         callers executor
     * @return Transaction data along with version information.
     */
    CompletableFuture<VersionedTransactionData> createTransaction(final String scopeName, final String streamName,
                                                                  final UUID txnId,
                                                                  final long lease, final long maxExecutionTime,
                                                                  final OperationContext context,
                                                                  final Executor executor);

    /**
     * Heartbeat to keep the transaction open for at least lease amount of time.
     *
     * @param scopeName  Scope
     * @param streamName Stream
     * @param txData     Transaction data
     * @param lease      Lease duration in ms
     * @param context    operation context
     * @param executor   callers executor
     * @return Transaction data along with version information.
     */
    CompletableFuture<VersionedTransactionData> pingTransaction(final String scopeName, final String streamName,
                                                                final VersionedTransactionData txData, final long lease,
                                                                final OperationContext context, final Executor executor);

    /**
     * Fetch transaction metadata along with its version.
     *
     * @param scopeName  scope
     * @param streamName stream
     * @param txId       transaction id
     * @param context    operation context
     * @param executor   callers executor
     * @return transaction metadata along with its version.
     */
    CompletableFuture<VersionedTransactionData> getTransactionData(String scopeName, String streamName, UUID txId,
                                                                   final OperationContext context,
                                                                   final Executor executor);

    /**
     * Get transaction status from the stream store.
     *
     * @param scope    stream scope
     * @param stream   stream
     * @param txId     transaction id
     * @param context  operation context
     * @param executor callers executor
     * @return transaction status.
     */
    CompletableFuture<TxnStatus> transactionStatus(final String scope, final String stream, final UUID txId, final OperationContext context, final Executor executor);

    /**
     * Update stream store to mark transaction as committed.
     *
     * @param scope    scope
     * @param stream   stream
     * @param txId     transaction id
     * @param context  operation context
     * @param executor callers executor
     * @return transaction status.
     */
    CompletableFuture<TxnStatus> commitTransaction(final String scope, final String stream,
                                                   final UUID txId, final OperationContext context,
                                                   final Executor executor);

    /**
     * Update stream store to mark transaction as sealed.
     *
     * @param scope    scope
     * @param stream   stream
     * @param txId     transaction id
     * @param commit   Boolean indicating whether to change txn state to committing or aborting.
     * @param version  Expected version of the transaction record in the store.
     * @param context  operation context
     * @param executor callers executor
     * @return         Pair containing the transaction status after sealing and transaction epoch.
     */
    CompletableFuture<SimpleEntry<TxnStatus, Integer>> sealTransaction(final String scope, final String stream,
                                                                       final UUID txId, final boolean commit,
                                                                       final Optional<Integer> version,
                                                                       final OperationContext context,
                                                                       final Executor executor);

    /**
     * Update stream store to mark the transaction as aborted.
     *
     * @param scope    scope
     * @param stream   stream
     * @param txId     transaction id
     * @param context  operation context
     * @param executor callers executor
     * @return transaction status
     */
    CompletableFuture<TxnStatus> abortTransaction(final String scope, final String stream,
                                                  final UUID txId, final OperationContext context,
                                                  final Executor executor);

    /**
     * Method to retrive all currently active transactions from the metadata store.
     *
     * @param scope    scope of stream
     * @param stream   name of stream
     * @param context  operation context
     * @param executor callers executor
     * @return map of txId to TxRecord
     */
    CompletableFuture<Map<UUID, ActiveTxnRecord>> getActiveTxns(final String scope, final String stream, final OperationContext context, final Executor executor);

    /**
     * Adds specified resource as a child of current host's hostId node.
     * This is idempotent operation.
     *
     * @param hostId      Host identifier.
     * @param txn         Tracked transaction resource.
     * @param version     Version of tracked transaction's node.
     * @return            A future that completes on completion of the operation.
     */
    CompletableFuture<Void> addTxnToIndex(final String hostId, final TxnResource txn, final int version);

    /**
     * Removes the specified child node from the specified parent node.
     * This is idempotent operation.
     * If deleteEmptyParent is true and parent has no child after deletion of given child then parent is also deleted.
     *
     * @param hostId            Node whose child is to be removed.
     * @param txn               Transaction resource to remove.
     * @param deleteEmptyParent To delete or not to delete.
     * @return void in future.
     */
    CompletableFuture<Void> removeTxnFromIndex(final String hostId, final TxnResource txn,
                                               final boolean deleteEmptyParent);

    /**
     * Returns a transaction managed by specified host, if one exists.
     *
     * @param hostId Host identifier.
     * @return A transaction managed by specified host, if one exists.
     */
    CompletableFuture<Optional<TxnResource>> getRandomTxnFromIndex(final String hostId);

    /**
     * Fetches version of specified txn stored in the index under specified host.
     *
     * @param hostId    Host identifier.
     * @param resource  Txn resource.
     * @return txn version stored in the index under specified host.
     */
    CompletableFuture<Integer> getTxnVersionFromIndex(final String hostId, final TxnResource resource);

    /**
     * Remove the specified host from the index.
     *
     * @param hostId Host identifier.
     * @return A future indicating completion of removal of the host from index.
     */
    CompletableFuture<Void> removeHostFromIndex(String hostId);

    /**
     * Fetches set of hosts that own some txn.
     *
     * @return set of hosts owning some txn.
     */
    CompletableFuture<Set<String>> listHostsOwningTxn();

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
    CompletableFuture<HistoryRecord> getActiveEpoch(final String scope,
                                                    final String stream,
                                                    final OperationContext context,
                                                    final boolean ignoreCached,
                                                    final Executor executor);

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
    CompletableFuture<HistoryRecord> getEpoch(final String scope,
                                              final String stream,
                                              final int epoch,
                                              final OperationContext context,
                                              final Executor executor);

    /**
     * Api to mark a segment as cold.
     *
     * @param scope         scope for stream
     * @param stream        name of stream
     * @param segmentId segment number
     * @param timestamp     time till which this cold marker is valid.
     * @param context       context in which this operation is taking place.
     * @param executor      callers executor
     * @return Completable future
     */
    CompletableFuture<Void> markCold(final String scope, final String stream, final long segmentId, final long timestamp, final OperationContext context, final Executor executor);

    /**
     * Api to return if a cold marker is set.
     *
     * @param scope    scope for stream
     * @param stream   name of stream
     * @param segmentId   segment nunmber
     * @param context  context in which this operation is taking place.
     * @param executor callers executor
     * @return Completable future Optional of marker's creation time.
     */
    CompletableFuture<Boolean> isCold(final String scope, final String stream, final long segmentId, final OperationContext context, final Executor executor);

    /**
     * Api to clear marker.
     *
     * @param scope    scope for stream
     * @param stream   name of stream
     * @param segmentId   segment nunmber
     * @param context  context in which this operation is taking place.
     * @param executor callers executor
     * @return Completable Future
     */
    CompletableFuture<Void> removeMarker(final String scope, final String stream, final long segmentId, final OperationContext context, final Executor executor);

    /**
     * Get all scale history segments.
     *
     * @param scope    stream scope
     * @param name     stream name.
     * @param executor callers executor
     * @param context  operation context
     * @return currently active segments
     */
    CompletableFuture<List<ScaleMetadata>> getScaleMetadata(final String scope, final String name, final OperationContext context, final Executor executor);

    /**
     * Method to register listener for changes to bucket's ownership.
     *
     * @param listener listener
     */
    void registerBucketOwnershipListener(BucketOwnershipListener listener);

    /**
     * Unregister listeners for bucket ownership.
     */
    void unregisterBucketOwnershipListener();

    /**
     * Method to register listeners for changes to streams under the bucket.
     *
     * @param bucket   bucket
     * @param listener listener
     */
    void registerBucketChangeListener(int bucket, BucketChangeListener listener);

    /**
     * Method to unregister listeners for changes to streams under the bucket.
     *
     * @param bucket bucket
     */
    void unregisterBucketListener(int bucket);

    /**
     * Method to take ownership of a bucket.
     *
     * @param bucket   bucket id
     * @param processId process id
     *@param executor executor  @return future boolean which tells if ownership attempt succeeded or failed.
     */
    CompletableFuture<Boolean> takeBucketOwnership(int bucket, String processId, final Executor executor);

    /**
     * Return all streams in the bucket.
     *
     * @param bucket   bucket id.
     * @param executor executor
     * @return List of scopedStreamName (scope/stream)
     */
    CompletableFuture<List<String>> getStreamsForBucket(final int bucket, final Executor executor);

    /**
     * Add the given stream to appropriate bucket for auto-retention.
     *
     * @param scope           scope
     * @param stream          stream
     * @param retentionPolicy retention policy
     * @param context         operation context
     * @param executor        executor
     * @return future
     */
    CompletableFuture<Void> addUpdateStreamForAutoStreamCut(final String scope, final String stream, final RetentionPolicy retentionPolicy,
                                                            final OperationContext context, final Executor executor);

    /**
     * Remove stream from auto retention bucket.
     *
     * @param scope    scope
     * @param stream   stream
     * @param context  context
     * @param executor executor
     * @return future
     */
    CompletableFuture<Void> removeStreamFromAutoStreamCut(final String scope, final String stream,
                                                          final OperationContext context, final Executor executor);

    /**
     * Add stream cut to retention set of the given stream.
     *
     * @param scope     scope
     * @param stream    stream
     * @param streamCut stream cut to add
     * @param context   context
     * @param executor  executor
     * @return future
     */
    CompletableFuture<Void> addStreamCutToRetentionSet(final String scope, final String stream, final StreamCutRecord streamCut,
                                                       final OperationContext context, final Executor executor);

    /**
     * Get retention set made of stream cuts for the given stream.
     *
     * @param scope    scope
     * @param stream   stream
     * @param context  context
     * @param executor executor
     * @return future
     */
    CompletableFuture<List<StreamCutRecord>> getStreamCutsFromRetentionSet(final String scope, final String stream,
                                                                           final OperationContext context, final Executor executor);

    /**
     * Delete all stream cuts with recording time before the supplied stream cut from the retention set of the stream.
     *
     * @param scope     scope
     * @param stream    stream
     * @param streamCut stream cut to purge from
     * @param context   context
     * @param executor  executor
     * @return future
     */
    CompletableFuture<Void> deleteStreamCutBefore(final String scope, final String stream, final StreamCutRecord streamCut,
                                                  final OperationContext context, final Executor executor);

    /**
     * Method to get size till the supplied stream cut map.
     *
     * @param scope scope name
     * @param stream stream name
     * @param streamCut stream cut to get the size till
     * @param context operation context
     * @param executor executor
     * @return A CompletableFuture which, when completed, will contain size of stream till given streamCut.
     */
    CompletableFuture<Long> getSizeTillStreamCut(final String scope, final String stream, final Map<Long, Long> streamCut,
                                                 final OperationContext context, final ScheduledExecutorService executor);

    /**
     * Method to create committing transaction record in the store for a given stream.
     * Note: this will not throw data exists exception if the committing transaction node already exists.
     *
     * @param scope scope name
     * @param stream stream name
     * @param epoch epoch
     * @param txnsToCommit transactions to commit within the epoch
     * @param context operation context
     * @param executor executor
     * @return A completableFuture which, when completed, mean that the record has been created successfully.
     */
    CompletableFuture<Void> createCommittingTransactionsRecord(final String scope, final String stream, final int epoch, final List<UUID> txnsToCommit,
                                                               final OperationContext context, final ScheduledExecutorService executor);

    /**
     * Method to fetch committing transaction record from the store for a given stream.
     * Note: this will not throw data not found exception if the committing transaction node is not found. Instead
     * it returns null.
     *
     * @param scope scope name
     * @param stream stream name
     * @param context operation context
     * @param executor executor
     * @return A completableFuture which, when completed, will contain committing transaction record if it exists, or null otherwise.
     */
    CompletableFuture<CommittingTransactionsRecord> getCommittingTransactionsRecord(final String scope, final String stream,
                                                                                    final OperationContext context, final ScheduledExecutorService executor);

    /**
     * Method to delete committing transaction record from the store for a given stream.
     *
     * @param scope scope name
     * @param stream stream name
     * @param context operation context
     * @param executor executor
     * @return A completableFuture which, when completed, will mean that deletion of txnCommitNode is complete.
     */
    CompletableFuture<Void> deleteCommittingTransactionsRecord(final String scope, final String stream, final OperationContext context,
                                                               final ScheduledExecutorService executor);

    /**
     * Method to get all transactions in a given epoch. This method returns a map of transaction id to transaction record.
     *
     * @param scope scope
     * @param stream stream
     * @param epoch epoch
     * @param context operation context
     * @param executor executor
     * @return A completableFuture which when completed will contain a map of transaction id and its record.
     */
    CompletableFuture<Map<UUID, ActiveTxnRecord>> getTransactionsInEpoch(final String scope, final String stream, final int epoch,
                                                                         final OperationContext context, final ScheduledExecutorService executor);

    /**
     * This method attempts to create a new Waiting Request node and set the processor's name in the node.
     * If a node already exists, this attempt is ignored.
     *
     * @param scope scope
     * @param stream stream
     * @param processorName name of the request processor that is waiting to get an opportunity for processing.
     * @param context operation context
     * @param executor executor
     * @return CompletableFuture which indicates that a node was either created successfully or records the failure.
     */
    CompletableFuture<Void> createWaitingRequestIfAbsent(String scope, String stream, String processorName, OperationContext context, ScheduledExecutorService executor);

    /**
     * This method fetches existing waiting request processor's name if any. It returns null if no processor is waiting.
     *
     * @param scope scope
     * @param stream stream
     * @param context operation context
     * @param executor executor
     * @return CompletableFuture which has the name of the processor that had requested for a wait, or null if there was no
     * such request.
     */
    CompletableFuture<String> getWaitingRequestProcessor(String scope, String stream, OperationContext context, ScheduledExecutorService executor);

    /**
     * Delete existing waiting request processor if the name of the existing matches suppied processor name.
     *
     * @param scope scope
     * @param stream stream
     * @param processorName processor name which is to be deleted if it matches the name in waiting record in the store.
     * @param context operation context
     * @param executor executor
     * @return CompletableFuture which indicates completion of processing.
     */
    CompletableFuture<Void> deleteWaitingRequestConditionally(String scope, String stream, String processorName, OperationContext context, ScheduledExecutorService executor);
}
