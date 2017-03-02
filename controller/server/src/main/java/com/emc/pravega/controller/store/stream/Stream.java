/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.controller.store.stream;

import com.emc.pravega.controller.store.stream.tables.ActiveTxRecord;
import com.emc.pravega.controller.store.stream.tables.State;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.impl.TxnStatus;

import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Properties of a stream and operations that can be performed on it.
 * Identifier for a stream is its name.
 */
interface Stream {

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
    CompletableFuture<Boolean> create(final StreamConfiguration configuration, final long createTimestamp);

    /**
     * Updates the configuration of an existing stream.
     *
     * @param configuration new stream configuration.
     * @return boolean indicating whether the stream was updated.
     */
    CompletableFuture<Boolean> updateConfiguration(final StreamConfiguration configuration);

    /**
     * Fetches the current stream configuration.
     *
     * @return current stream configuration.
     */
    CompletableFuture<StreamConfiguration> getConfiguration();

    /**
     * Update the state of the stream.
     *
     * @return boolean indicating whether the state of stream is updated.
     */
    CompletableFuture<Boolean> updateState(final State state);

    /**
     * Get the state of the stream.
     *
     * @return state othe given stream.
     */
    CompletableFuture<State> getState();

    /**
     * Fetches details of specified segment.
     *
     * @param number segment number.
     * @return segment at given number.
     */
    CompletableFuture<Segment> getSegment(final int number);

    /**
     * @param number segment number.
     * @return successors of specified segment.
     */
    CompletableFuture<List<Integer>> getSuccessors(final int number);

    /**
     * @param number segment number.
     * @return successors of specified segment mapped to the list of their predecessors
     */
    CompletableFuture<Map<Integer, List<Integer>>> getSuccessorsWithPredecessors(final int number);

    /**
     * @param number segment number.
     * @return predecessors of specified segment
     */
    CompletableFuture<List<Integer>> getPredecessors(final int number);

    /**
     * @return currently active segments
     */
    CompletableFuture<List<Integer>> getActiveSegments();

    /**
     * @param timestamp point in time.
     * @return the list of segments active at timestamp.
     */
    CompletableFuture<List<Integer>> getActiveSegments(final long timestamp);

    /**
     * Scale the stream by sealing few segments and creating few segments
     *
     * @param sealedSegments segments to be sealed
     * @param newRanges      key ranges of new segments to be created
     * @param scaleTimestamp scaling timestamp
     * @return sequence of newly created segments
     */
    CompletableFuture<List<Segment>> scale(final List<Integer> sealedSegments,
                                           final List<AbstractMap.SimpleEntry<Double, Double>> newRanges,
                                           final long scaleTimestamp);


    /**
     * Sets cold marker which is valid till the specified time stamp.
     * It creates a new marker if none is present or updates the previously set value.
     *
     * @param segmentNumber segment number to be marked as cold.
     * @param timestamp     time till when the marker is valid.
     * @return future
     */
    CompletableFuture<Void> setColdMarker(int segmentNumber, long timestamp);

    /**
     * Returns if a cold marker is set. Otherwise returns null.
     *
     * @param segmentNumber segment to check for cold.
     * @return future of either timestamp till when the marker is valid or null.
     */
    CompletableFuture<Long> getColdMarker(int segmentNumber);

    /**
     * Remove the cold marker for the segment.
     *
     * @param segmentNumber segment.
     * @return future
     */
    CompletableFuture<Void> removeColdMarker(int segmentNumber);

    /**
     * Method to start new transaction creation
     *
     * @return
     */
    CompletableFuture<VersionedTransactionData> createTransaction(final long lease, final long maxExecutionTime,
                                                                  final long scaleGracePeriod);


    /**
     * Heartbeat method to keep transaction open for at least lease amount of time.
     *
     * @param txId Transaction identifier.
     * @param lease Lease period in ms.
     * @return Transaction metadata along with its version.
     */
    CompletableFuture<VersionedTransactionData> pingTransaction(final UUID txId, final long lease);

    /**
     * Fetch transaction metadata along with its version.
     *
     * @param txId transaction id.
     * @return transaction metadata along with its version.
     */
    CompletableFuture<VersionedTransactionData> getTransactionData(UUID txId);

    /**
     * Seal given transaction
     *
     * @param txId
     * @return
     */
    CompletableFuture<TxnStatus> sealTransaction(final UUID txId, final boolean commit, final Optional<Integer> version);

    /**
     * Returns transaction's status
     *
     * @param txId
     * @return
     */
    CompletableFuture<TxnStatus> checkTransactionStatus(final UUID txId);

    /**
     * Commits a transaction
     * If already committed, return TxnStatus.Committed
     * If aborted, throw OperationOnTxNotAllowedException
     *
     * @param txId
     * @return
     */
    CompletableFuture<TxnStatus> commitTransaction(final UUID txId) throws OperationOnTxNotAllowedException;

    /**
     * Commits a transaction
     * If already aborted, return TxnStatus.Aborted
     * If committed, throw OperationOnTxNotAllowedException
     *
     * @param txId
     * @return
     */
    CompletableFuture<TxnStatus> abortTransaction(final UUID txId) throws OperationOnTxNotAllowedException;

    /**
     * Return whether any transaction is active on the stream.
     *
     * @return a boolean indicating whether a transaction is active on the stream.
     * Returns the number of transactions ongoing for the stream.
     */
    CompletableFuture<Integer> getNumberOfOngoingTransactions();

    CompletableFuture<Map<UUID, ActiveTxRecord>> getActiveTxns();

    /**
     * Refresh the stream object. Typically to be used to invalidate any caches.
     * This allows us reuse of stream object without having to recreate a new stream object for each new operation
     */
    void refresh();
}
