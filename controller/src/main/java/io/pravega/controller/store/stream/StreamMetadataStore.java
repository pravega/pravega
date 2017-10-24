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

import io.pravega.client.stream.StreamConfiguration;
import io.pravega.controller.store.stream.tables.ActiveTxnRecord;
import io.pravega.controller.store.stream.tables.State;
import io.pravega.controller.store.stream.tables.StreamTruncationRecord;
import io.pravega.controller.store.task.TxnResource;
import io.pravega.controller.stream.api.grpc.v1.Controller.CreateScopeStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.DeleteScopeStatus;
import org.apache.commons.lang3.tuple.Pair;

import java.util.AbstractMap.SimpleEntry;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

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
    CompletableFuture<StreamProperty<StreamConfiguration>> getConfigurationProperty(final String scope, final String name,
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
                                            final Map<Integer, Long> streamCut,
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
     * @param scope    stream scope
     * @param name     stream name.
     * @param context  operation context
     * @param executor callers executor
     * @return current truncation record.
     */
    CompletableFuture<StreamTruncationRecord> getTruncationRecord(final String scope, final String name,
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
    CompletableFuture<StreamProperty<StreamTruncationRecord>> getTruncationProperty(final String scope, final String name,
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
    CompletableFuture<Segment> getSegment(final String scope, final String name, final int number, final OperationContext context, final Executor executor);

    /**
     * Returns the total number of segments in the stream.
     *
     * @param scope    stream scope
     * @param name     stream name.
     * @param context  operation context
     * @param executor callers executor
     * @return total number of segments in the stream.
     */
    CompletableFuture<Integer> getSegmentCount(final String scope, final String name, final OperationContext context, final Executor executor);

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
    CompletableFuture<List<Integer>> getActiveSegments(final String scope, final String name, final long timestamp, final OperationContext context, final Executor executor);

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
    CompletableFuture<List<Integer>> getActiveSegmentIds(final String scope,
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
     * @param segmentNumber the segment number
     * @param context       operation context
     * @param executor      callers executor
     * @return segments that immediately follow the specified segment and the segments they follow.
     */
    CompletableFuture<Map<Integer, List<Integer>>> getSuccessors(final String scope, final String streamName,
                                                                 final int segmentNumber, final OperationContext context, final Executor executor);

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
    CompletableFuture<StartScaleResponse> startScale(final String scope, final String name,
                                                            final List<Integer> sealedSegments,
                                                            final List<SimpleEntry<Double, Double>> newRanges,
                                                            final long scaleTimestamp,
                                                            final boolean runOnlyIfStarted,
                                                            final OperationContext context,
                                                            final Executor executor);

    /**
     * Called after new segments are created in SSS.
     *
     * @param scope          stream scope
     * @param name           stream name.
     * @param sealedSegments segments to be sealed
     * @param newSegments    segments that were created as part of startScale
     * @param activeEpoch    scale epoch
     * @param scaleTimestamp timestamp at which scale was requested
     * @param context        operation context
     * @param executor       callers executor
     * @return future
     */
    CompletableFuture<Void> scaleNewSegmentsCreated(final String scope,
                                                    final String name,
                                                    final List<Integer> sealedSegments,
                                                    final List<Segment> newSegments,
                                                    final int activeEpoch,
                                                    final long scaleTimestamp,
                                                    final OperationContext context,
                                                    final Executor executor);

    /**
     * Called after old segments are sealed in pravega.
     *
     * @param scope          stream scope
     * @param name           stream name.
     * @param sealedSegments segments to be sealed
     * @param newSegments    segments that were created as part of startScale
     * @param activeEpoch    scale epoch
     * @param scaleTimestamp timestamp at which scale was requested
     * @param context        operation context
     * @param executor       callers executor
     * @return future
     */
    CompletableFuture<Void> scaleSegmentsSealed(final String scope, final String name,
                                                final List<Integer> sealedSegments,
                                                final List<Segment> newSegments,
                                                final int activeEpoch,
                                                final long scaleTimestamp,
                                                final OperationContext context,
                                                final Executor executor);

    /**
     * Method to delete epoch if scale operation is ongoing.
     * @param scope scope
     * @param stream stream
     * @param epoch epoch to delete
     * @param context context
     * @param executor executor
     * @return returns a pair of segments sealed from previous epoch and new segments added in new epoch
     */
    CompletableFuture<DeleteEpochResponse> tryDeleteEpochIfScaling(final String scope,
                                                                   final String stream,
                                                                   final int epoch,
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
     * @param scaleGracePeriod Maximum time for which client may extend txn lease once
     *                         the scaling operation is initiated on the txn stream.
     * @param context          operation context
     * @param executor         callers executor
     * @return Transaction data along with version information.
     */
    CompletableFuture<VersionedTransactionData> createTransaction(final String scopeName, final String streamName,
                                                                  final UUID txnId,
                                                                  final long lease, final long maxExecutionTime,
                                                                  final long scaleGracePeriod,
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
     * @param epoch    transaction epoch
     * @param txId     transaction id
     * @param context  operation context
     * @param executor callers executor
     * @return transaction status.
     */
    CompletableFuture<TxnStatus> commitTransaction(final String scope, final String stream, final int epoch,
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
     * @param epoch    transaction epoch
     * @param txId     transaction id
     * @param context  operation context
     * @param executor callers executor
     * @return transaction status
     */
    CompletableFuture<TxnStatus> abortTransaction(final String scope, final String stream, final int epoch,
                                                  final UUID txId, final OperationContext context,
                                                  final Executor executor);

    /**
     * Returns a boolean indicating whether any transaction is active on the specified stream.
     *
     * @param scope    scope.
     * @param stream   stream.
     * @param context  operation context
     * @param executor callers executor
     * @return boolean indicating whether any transaction is active on the specified stream.
     */
    CompletableFuture<Boolean> isTransactionOngoing(final String scope, final String stream, final OperationContext context, final Executor executor);

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
     * @return         pair containing currently active epoch of the stream, and active segments in current epoch.
     */
    CompletableFuture<Pair<Integer, List<Integer>>> getActiveEpoch(final String scope,
                                                                   final String stream,
                                                                   final OperationContext context,
                                                                   final boolean ignoreCached,
                                                                   final Executor executor);

    /**
     * Api to mark a segment as cold.
     *
     * @param scope         scope for stream
     * @param stream        name of stream
     * @param segmentNumber segment number
     * @param timestamp     time till which this cold marker is valid.
     * @param context       context in which this operation is taking place.
     * @param executor      callers executor
     * @return Completable future
     */
    CompletableFuture<Void> markCold(final String scope, final String stream, final int segmentNumber, final long timestamp, final OperationContext context, final Executor executor);

    /**
     * Api to return if a cold marker is set.
     *
     * @param scope    scope for stream
     * @param stream   name of stream
     * @param number   segment nunmber
     * @param context  context in which this operation is taking place.
     * @param executor callers executor
     * @return Completable future Optional of marker's creation time.
     */
    CompletableFuture<Boolean> isCold(final String scope, final String stream, final int number, final OperationContext context, final Executor executor);

    /**
     * Api to clear marker.
     *
     * @param scope    scope for stream
     * @param stream   name of stream
     * @param number   segment nunmber
     * @param context  context in which this operation is taking place.
     * @param executor callers executor
     * @return Completable Future
     */
    CompletableFuture<Void> removeMarker(final String scope, final String stream, final int number, final OperationContext context, final Executor executor);

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
     * Method to count number of splits and merges.
     *
     * @param scopeName     stream scope
     * @param streamName    stream name
     * @param executor      callers executor
     * @return              SimpleEntry, number of splits as Key and number of merges as value
     */
    CompletableFuture<SimpleEntry<Long, Long>> findNumSplitsMerges(String scopeName, String streamName, Executor executor);

}
