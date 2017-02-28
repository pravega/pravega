/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.store.stream;

import com.emc.pravega.controller.store.stream.tables.ActiveTxRecordWithStream;
import com.emc.pravega.controller.stream.api.v1.CreateScopeStatus;
import com.emc.pravega.controller.stream.api.v1.DeleteScopeStatus;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.impl.TxnStatus;

import java.util.AbstractMap.SimpleEntry;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Stream Metadata.
 */
public interface StreamMetadataStore {

    /**
     * Creates a new stream with the given name and configuration.
     *
     * @param scopeName       scope name
     * @param streamName      stream name
     * @param configuration   stream configuration
     * @param createTimestamp stream creation timestamp
     * @return boolean indicating whether the stream was created
     */
    CompletableFuture<Boolean> createStream(final String scopeName,
                                            final String streamName,
                                            final StreamConfiguration configuration,
                                            final long createTimestamp);

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
     * @param scopeName     scope name.
     * @param streamName    stream name.
     * @param configuration new stream configuration
     * @return boolean indicating whether the stream was updated
     */
    CompletableFuture<Boolean> updateConfiguration(final String scopeName, final String streamName, final StreamConfiguration configuration);

    /**
     * Fetches the current stream configuration.
     *
     * @param scopeName  scope name.
     * @param streamName stream name.
     * @return current stream configuration.
     */
    CompletableFuture<StreamConfiguration> getConfiguration(final String scopeName, final String streamName);

    /**
     * Set the stream state to sealed.
     *
     * @param scopeName  scope name.
     * @param streamName stream name.
     * @return boolean indicating whether the stream was updated.
     */
    CompletableFuture<Boolean> setSealed(final String scopeName, final String streamName);

    /**
     * Get the stream sealed status.
     *
     * @param scopeName  scope name.
     * @param streamName stream name.
     * @return boolean indicating whether the stream is sealed.
     */
    CompletableFuture<Boolean> isSealed(final String scopeName, final String streamName);

    /**
     * Get Segment.
     *
     * @param scopeName  scope name.
     * @param streamName stream name.
     * @param number     segment number.
     * @return segment at given number.
     */
    CompletableFuture<Segment> getSegment(final String scopeName, final String streamName, final int number);

    /**
     * Get active segments.
     *
     * @param scopeName  scope name.
     * @param streamName stream name.
     * @return currently active segments
     */
    CompletableFuture<List<Segment>> getActiveSegments(final String scopeName, final String streamName);

    /**
     * Get active segments at given timestamp.
     *
     * @param scopeName  scope name.
     * @param streamName stream name.
     * @param timestamp  point in time.
     * @return the list of segments active at timestamp.
     */
    CompletableFuture<SegmentFutures> getActiveSegments(final String scopeName, final String streamName, final long timestamp);

    /**
     * Given a segment return a map containing the numbers of the segments immediately succeeding it
     * mapped to a list of the segments they succeed.
     *
     * @param scopeName     scope name.
     * @param streamName    the stream name.
     * @param segmentNumber the segment number
     * @return segments that immediately follow the specified segment and the segments they follow.
     */
    CompletableFuture<Map<Integer, List<Integer>>> getSuccessors(final String scopeName,
                                                                        final String streamName,
                                                                        final int segmentNumber);

    /**
     * Scales in or out the currently set of active segments of a stream.
     *
     * @param scopeName      scope name.
     * @param streamName     stream name.
     * @param sealedSegments segments to be sealed
     * @param newRanges      new key ranges to be added to the stream which maps to a new segment per range in the stream
     * @param scaleTimestamp scaling timestamp, all sealed segments shall have it as their end time and
     *                       all new segments shall have it as their start time.
     * @return the list of newly created segments
     */
    CompletableFuture<List<Segment>> scale(final String scopeName,
                                           final String streamName,
                                           final List<Integer> sealedSegments,
                                           final List<SimpleEntry<Double, Double>> newRanges,
                                           final long scaleTimestamp);

    /**
     * Method to create a new transaction on a stream.
     *
     * @param scopeName  Scope
     * @param streamName Stream
     * @param lease      Time for which transaction shall remain open with sending any heartbeat.
     * @param maxExecutionTime Maximum time for which client may extend txn lease.
     * @param scaleGracePeriod Maximum time for which client may extend txn lease once
     *                         the scaling operation is initiated on the txn stream.
     * @return Transaction data along with version information.
     */
    CompletableFuture<VersionedTransactionData> createTransaction(final String scopeName, final String streamName,
                                                                  final long lease, final long maxExecutionTime,
                                                                  final long scaleGracePeriod);

    /**
     * Heartbeat to keep the transaction open for at least lease amount of time.
     *
     * @param scopeName  Scope
     * @param streamName Stream
     * @param txId       Transaction identifier
     * @param lease      Lease duration in ms
     * @return Transaction data along with version information.
     */
    CompletableFuture<VersionedTransactionData> pingTransaction(final String scopeName, final String streamName,
                                                                final UUID txId, final long lease);

    /**
     * Fetch transaction metadata along with its version.
     *
     * @param scopeName  scope
     * @param streamName stream
     * @param txId       transaction id
     * @return           transaction metadata along with its version.
     */
    CompletableFuture<VersionedTransactionData> getTransactionData(String scopeName, String streamName, UUID txId);

    /**
     * Get transaction status from the stream store.
     *
     * @param scopeName  scope
     * @param streamName stream
     * @param txId       transaction id
     * @return           Transaction status.
     */
    CompletableFuture<TxnStatus> transactionStatus(final String scopeName, final String streamName, final UUID txId);

    /**
     * Update stream store to mark transaction as committed.
     *
     * @param scopeName  scope
     * @param streamName stream
     * @param txId       transaction id
     * @return           Transaction status.
     */
    CompletableFuture<TxnStatus> commitTransaction(final String scopeName, final String streamName, final UUID txId);

    /**
     * Update stream store to mark transaction as sealed.
     *
     * @param scope  scope
     * @param stream stream
     * @param txId   transaction id
     * @param commit Boolean indicating whether to change txn state to committing or aborting.
     * @param version Expected version of the transaction record in the store.
     * @return        Transaction status.
     */
    CompletableFuture<TxnStatus> sealTransaction(final String scope, final String stream, final UUID txId,
                                                 final boolean commit, final Optional<Integer> version);

    /**
     * Update stream store to mark the transaction as aborted.
     *
     * @param scopeName  scope
     * @param streamName stream
     * @param txId       transaction id
     * @return           Transaction status.
     */
    CompletableFuture<TxnStatus> abortTransaction(final String scopeName, final String streamName, final UUID txId);

    /**
     * Returns a boolean indicating whether any transaction is active on the specified stream.
     *
     * @param scopeName  scope.
     * @param streamName stream.
     * @return boolean indicating whether any transaction is active on the specified stream.
     */
    CompletableFuture<Boolean> isTransactionOngoing(final String scopeName, final String streamName);

    /**
     * Returns all active transactions for all streams.
     * This is used for periodically identifying timedout transactions which can be aborted
     *
     * @return List of active transactions.
     */
    CompletableFuture<List<ActiveTxRecordWithStream>> getAllActiveTx();
}
