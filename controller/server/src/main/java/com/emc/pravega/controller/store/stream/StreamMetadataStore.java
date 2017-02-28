/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.controller.store.stream;

import com.emc.pravega.controller.store.stream.tables.ActiveTxRecord;
import com.emc.pravega.controller.store.stream.tables.State;
import com.emc.pravega.controller.stream.api.v1.CreateScopeStatus;
import com.emc.pravega.controller.stream.api.v1.DeleteScopeStatus;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.impl.TxnStatus;

import java.nio.ByteBuffer;
import java.util.AbstractMap.SimpleEntry;
import java.util.List;
import java.util.Map;
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
    CompletableFuture<Boolean> createStream(final String scopeName,
                                            final String streamName,
                                            final StreamConfiguration configuration,
                                            final long createTimestamp,
                                            final OperationContext context,
                                            final Executor executor);

    CompletableFuture<Boolean> setState(String scope, String name,
                                        State state, OperationContext context,
                                        Executor executor);

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
     * @return boolean indicating whether the stream was updated
     */
    CompletableFuture<Boolean> updateConfiguration(final String scope, final String name, final StreamConfiguration configuration,
                                                   final OperationContext context,
                                                   final Executor executor);

    /**
     * Fetches the current stream configuration.
     *
     * @param scope    stream scope
     * @param context  operation context
     * @param name     stream name.
     * @param executor callers executor
     * @return current stream configuration.
     */
    CompletableFuture<StreamConfiguration> getConfiguration(final String scope, final String name,
                                                            final OperationContext context,
                                                            final Executor executor);

    /**
     * Set the stream state to sealed.
     *
     * @param scope    stream scope
     * @param context  operation context
     * @param name     stream name.
     * @param executor callers executor
     * @return boolean indicating whether the stream was updated.
     */
    CompletableFuture<Boolean> setSealed(final String scope, final String name, final OperationContext context, final Executor executor);

    /**
     * Get the stream sealed status.
     *
     * @param scope    stream scope
     * @param context  operation context
     * @param name     stream name.
     * @param executor callers executor
     * @return boolean indicating whether the stream is sealed.
     */
    CompletableFuture<Boolean> isSealed(final String scope, final String name, final OperationContext context, final Executor executor);

    /**
     * Get Segment.
     *
     * @param scope    stream scope
     * @param context  operation context
     * @param name     stream name.
     * @param number   segment number.
     * @param executor callers executor
     * @return segment at given number.
     */
    CompletableFuture<Segment> getSegment(final String scope, final String name, final int number, final OperationContext context, final Executor executor);

    /**
     * Get active segments.
     *
     * @param scope    stream scope
     * @param context  operation context
     * @param name     stream name.
     * @param executor callers executor
     * @return currently active segments
     */
    CompletableFuture<List<Segment>> getActiveSegments(final String scope, final String name, final OperationContext context, final Executor executor);

    /**
     * Get active segments at given timestamp.
     *
     * @param scope     stream scope
     * @param context   operation context
     * @param name      stream name.
     * @param timestamp point in time.
     * @param executor  callers executor
     * @return the list of segments active at timestamp.
     */
    CompletableFuture<SegmentFutures> getActiveSegments(final String scope, final String name, final long timestamp, final OperationContext context, final Executor executor);

    /**
     * Given a segment return a map containing the numbers of the segments immediately succeeding it
     * mapped to a list of the segments they succeed.
     *
     * @param scope         stream scope
     * @param context       operation context
     * @param streamName    stream name.
     * @param segmentNumber the segment number
     * @param executor      callers executor
     * @return segments that immediately follow the specified segment and the segments they follow.
     */
    public CompletableFuture<Map<Integer, List<Integer>>> getSuccessors(final String scope, final String streamName,
                                                                        final int segmentNumber, final OperationContext context, final Executor executor);

    /**
     * Scales in or out the currently set of active segments of a stream.
     *
     * @param scope          stream scope
     * @param context        operation context
     * @param name           stream name.
     * @param sealedSegments segments to be sealed
     * @param newRanges      new key ranges to be added to the stream which maps to a new segment per range in the stream
     * @param scaleTimestamp scaling timestamp, all sealed segments shall have it as their end time and
     *                       all new segments shall have it as their start time.
     * @param executor       callers executor
     * @return the list of newly created segments
     */
    CompletableFuture<List<Segment>> scale(final String scope, final String name,
                                           final List<Integer> sealedSegments,
                                           final List<SimpleEntry<Double, Double>> newRanges,
                                           final long scaleTimestamp, final OperationContext context, final Executor executor);

    /**
     * Method to create a new transaction on a stream.
     *
     * @param scope    stream scope
     * @param context  operation context
     * @param stream   stream
     * @param executor callers executor
     * @return new Transaction Id
     */
    CompletableFuture<UUID> createTransaction(final String scope, final String stream, final OperationContext context, final Executor executor);

    /**
     * Get transaction status from the stream store.
     *
     * @param scope    stream scope
     * @param stream   stream
     * @param txId     transaction id
     * @param context  operation context
     * @param executor callers executor
     * @return
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
     * @return
     */
    CompletableFuture<TxnStatus> commitTransaction(final String scope, final String stream, final UUID txId, final OperationContext context, final Executor executor);

    /**
     * Update stream store to mark transaction as sealed.
     *
     * @param scope    scope
     * @param stream   stream
     * @param txId     transaction id
     * @param commit   commit
     * @param context  operation context
     * @param executor callers executor
     * @return
     */
    CompletableFuture<TxnStatus> sealTransaction(final String scope, final String stream, final UUID txId, final boolean commit, final OperationContext context, final Executor executor);

    /**
     * Update stream store to mark the transaction as aborted.
     *
     * @param scope    scope
     * @param stream   stream
     * @param txId     transaction id
     * @param context  operation context
     * @param executor callers executor
     * @return
     */
    CompletableFuture<TxnStatus> abortTransaction(final String scope, final String stream, final UUID txId, final OperationContext context, final Executor executor);

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
    CompletableFuture<Map<UUID, ActiveTxRecord>> getActiveTxns(final String scope, final String stream, final OperationContext context, final Executor executor);

    /**
     * Api to block new transactions from being created for the specified duration.
     * This is useful in scenarios where a scale operation may end up waiting indefinitely
     * because there are always one or more on-going transactions.
     *
     * @param scope     scope.
     * @param stream    stream.
     * @param timestamp timestamp until block is valid.
     * @param context   context in which this operation is taking place.
     * @param executor  callers executor
     * @return Completable Future
     */
    CompletableFuture<Void> blockTransactions(final String scope, final String stream, final long timestamp,
                                              final OperationContext context, final Executor executor);

    /**
     * Api to unblock creation of transactions.
     *
     * @param scope    scope.
     * @param stream   name.
     * @param context  context.
     * @param executor callers executor
     * @return List of active transactions.
     */
    CompletableFuture<Void> unblockTransactions(final String scope, final String stream, final OperationContext context, final Executor executor);

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

    CompletableFuture<Void> checkpoint(final String readerGroup, final String readerId, final ByteBuffer checkpointBlob);

    CompletableFuture<ByteBuffer> readCheckpoint(final String readerGroup, final String readerId);
}
