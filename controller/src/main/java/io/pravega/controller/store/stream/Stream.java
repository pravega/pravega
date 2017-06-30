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

import io.pravega.controller.store.stream.tables.ActiveTxnRecord;
import io.pravega.controller.store.stream.tables.State;
import io.pravega.client.stream.StreamConfiguration;
import org.apache.commons.lang3.tuple.Pair;

import java.util.AbstractMap;
import java.util.AbstractMap.SimpleEntry;
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
    CompletableFuture<CreateStreamResponse> create(final StreamConfiguration configuration, final long createTimestamp);

    /**
     * Deletes an already SEALED stream.
     *
     * @return boolean indicating success.
     */
    CompletableFuture<Void> delete();

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
     * Returns the total number segments in the stream.
     *
     * @return total number of segments in the stream.
     */
    CompletableFuture<Integer> getSegmentCount();

    /**
     * @param number segment number.
     * @return successors of specified segment.
     */
    CompletableFuture<List<Integer>> getSuccessors(final int number);

    CompletableFuture<List<ScaleMetadata>> getScaleMetadata();

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
     * Returns the active segments in the specified epoch.
     *
     * @param epoch epoch number.
     * @return list of numbers of segments active in the specified epoch.
     */
    CompletableFuture<List<Integer>> getActiveSegments(int epoch);

    /**
     * Called to start metadata updates to stream store wrt new scale event.
     *
     * @param newRanges      key ranges of new segments to be created
     * @param scaleTimestamp scaling timestamp
     * @param runOnlyIfStarted run only if scale is started
     * @return sequence of newly created segments
     */
    CompletableFuture<StartScaleResponse> startScale(final List<Integer> sealedSegments,
                                                     final List<AbstractMap.SimpleEntry<Double, Double>> newRanges,
                                                     final long scaleTimestamp,
                                                     final boolean runOnlyIfStarted);

    /**
     * Called after new segment creation is complete.
     *
     * @param epoch epoch
     * @return future
     */
    CompletableFuture<Boolean> scaleTryDeleteEpoch(final int epoch);

    /**
     * Called after new segment creation is complete and previous epoch is successfully deleted.
     *
     * @param sealedSegments segments to be sealed
     * @param newSegments    segments created
     * @param epoch
     *@param scaleTimestamp scaling timestamp  @return future
     */
    CompletableFuture<Void> scaleNewSegmentsCreated(final List<Integer> sealedSegments,
                                                    final List<Integer> newSegments,
                                                    final int epoch,
                                                    final long scaleTimestamp);

    /**
     * Called after sealing old segments is complete.
     *
     * @param sealedSegments segments to be sealed
     * @param newSegments    segments created
     * @param activeEpoch    activeEpoch
     *@param scaleTimestamp scaling timestamp  @return future
     */
    CompletableFuture<Void> scaleOldSegmentsSealed(final List<Integer> sealedSegments,
                                                   final List<Integer> newSegments,
                                                   int activeEpoch, final long scaleTimestamp);

    /**
     * Returns the latest sets of segments created and removed by doing a diff of last two epochs.
     * @return returns a pair of list of segments sealed and list of segments created in latest(including ongoing) scale event.
     */
    CompletableFuture<Pair<List<Integer>, List<Integer>>> latestScaleData();

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
     * @return Details of created transaction.
     */
    CompletableFuture<VersionedTransactionData> createTransaction(final UUID txnId,
                                                                  final long lease,
                                                                  final long maxExecutionTime,
                                                                  final long scaleGracePeriod);


    /**
     * Heartbeat method to keep transaction open for at least lease amount of time.
     *
     * @param txnData Transaction data.
     * @param lease Lease period in ms.
     * @return Transaction metadata along with its version.
     */
    CompletableFuture<VersionedTransactionData> pingTransaction(final VersionedTransactionData txnData, final long lease);

    /**
     * Fetch transaction metadata along with its version.
     *
     * @param txId transaction id.
     * @return transaction metadata along with its version.
     */
    CompletableFuture<VersionedTransactionData> getTransactionData(UUID txId);

    /**
     * Seal a given transaction.
     *
     * @param txId    transaction identifier.
     * @param commit  whether to commit or abort the specified transaction.
     * @param version optional expected version of transaction data node to validate before updating it.
     * @return        a pair containing transaction status and its epoch.
     */
    CompletableFuture<SimpleEntry<TxnStatus, Integer>> sealTransaction(final UUID txId,
                                                                       final boolean commit,
                                                                       final Optional<Integer> version);

    /**
     * Returns transaction's status
     *
     * @param txId transaction identifier.
     * @return     transaction status.
     */
    CompletableFuture<TxnStatus> checkTransactionStatus(final UUID txId);

    /**
     * Commits a transaction.
     * If already committed, return TxnStatus.Committed.
     * If aborting/aborted, return a failed future with IllegalStateException.
     *
     * @param epoch transaction epoch.
     * @param txId  transaction identifier.
     * @return      transaction status.
     */
    CompletableFuture<TxnStatus> commitTransaction(final int epoch, final UUID txId);

    /**
     * Aborts a transaction.
     * If already aborted, return TxnStatus.Aborted.
     * If committing/committed, return a failed future with IllegalStateException.
     *
     * @param epoch transaction epoch.
     * @param txId  transaction identifier.
     * @return      transaction status.
     */
    CompletableFuture<TxnStatus> abortTransaction(final int epoch, final UUID txId);

    /**
     * Return whether any transaction is active on the stream.
     *
     * @return a boolean indicating whether a transaction is active on the stream.
     * Returns the number of transactions ongoing for the stream.
     */
    CompletableFuture<Integer> getNumberOfOngoingTransactions();

    CompletableFuture<Map<UUID, ActiveTxnRecord>> getActiveTxns();

    /**
     * Returns the latest stream epoch.
     * @return latest stream epoch.
     */
    CompletableFuture<Pair<Integer, List<Integer>>> getLatestEpoch();

    /**
     * Returns the currently active stream epoch.
     * @return currently active stream epoch.
     */
    CompletableFuture<Pair<Integer, List<Integer>>> getActiveEpoch();

    /**
     * Refresh the stream object. Typically to be used to invalidate any caches.
     * This allows us reuse of stream object without having to recreate a new stream object for each new operation
     */
    void refresh();
}
