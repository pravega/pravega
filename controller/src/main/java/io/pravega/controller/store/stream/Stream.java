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
import io.pravega.controller.store.stream.tables.CommittingTransactionsRecord;
import io.pravega.controller.store.stream.tables.EpochTransitionRecord;
import io.pravega.controller.store.stream.tables.HistoryRecord;
import io.pravega.controller.store.stream.tables.State;
import io.pravega.controller.store.stream.tables.StreamConfigurationRecord;
import io.pravega.controller.store.stream.tables.StreamCutRecord;
import io.pravega.controller.store.stream.tables.StreamTruncationRecord;

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
     * Starts updating the configuration of an existing stream.
     *
     * @param configuration new stream configuration.
     * @return future of new StreamConfigWithVersion.
     */
    CompletableFuture<Void> startUpdateConfiguration(final StreamConfiguration configuration);

    /**
     * Completes an ongoing updates configuration of an existing stream.
     *
     * @return future of new StreamConfigWithVersion.
     */
    CompletableFuture<Void> completeUpdateConfiguration();

    /**
     * Fetches the current stream configuration.
     *
     * @return current stream configuration.
     */
    CompletableFuture<StreamConfiguration> getConfiguration();

    /**
     * Fetches the current stream configuration.
     *
     * @param ignoreCached ignore cached
     *
     * @return current stream configuration.
     */
    CompletableFuture<StreamConfigurationRecord> getConfigurationRecord(boolean ignoreCached);

    /**
     * Starts truncating an existing stream.
     *
     * @param streamCut new stream cut.
     * @return future of new StreamProperty.
     */
    CompletableFuture<Void> startTruncation(final Map<Long, Long> streamCut);

    /**
     * Completes an ongoing stream truncation.
     *
     * @return future of operation.
     */
    CompletableFuture<Void> completeTruncation();

    /**
     * Fetches the current stream cut.
     *
     * @param ignoreCached ignore cached
     *
     * @return current stream cut.
     */
    CompletableFuture<StreamTruncationRecord> getTruncationRecord(boolean ignoreCached);

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
     * @param ignoreCached ignore cached value and fetch from store
     */
    CompletableFuture<State> getState(boolean ignoreCached);

    /**
     * If state is matches the given state then reset it back to State.ACTIVE
     *
     * @param state state to compare
     * @return Future which when completed has reset the state
     */
    CompletableFuture<Void> resetStateConditionally(State state);

    /**
     * Fetches details of specified segment.
     *
     * @param segmentId segment number.
     * @return segment at given number.
     */
    CompletableFuture<Segment> getSegment(final long segmentId);

    CompletableFuture<List<ScaleMetadata>> getScaleMetadata();

    /**
     * @param segmentId segment number.
     * @return successors of specified segment mapped to the list of their predecessors
     */
    CompletableFuture<Map<Long, List<Long>>> getSuccessorsWithPredecessors(final long segmentId);

    /**
     * Method to get all segments between given stream cuts.
     * Either from or to can be either well formed stream cuts OR empty sets indicating unbounded cuts.
     * If from is unbounded, then head is taken as from, and if to is unbounded then tail is taken as the end.
     *
     * @param from from stream cut.
     * @param to to stream cut.
     * @return Future which when completed gives list of segments between given streamcuts.
     */
    CompletableFuture<List<Segment>> getSegmentsBetweenStreamCuts(final Map<Long, Long> from, final Map<Long, Long> to);

    /**
     * @return currently active segments
     */
    CompletableFuture<List<Long>> getActiveSegments();

    /**
     * @param timestamp point in time.
     * @return the list of segments active at timestamp.
     */
    CompletableFuture<List<Long>> getActiveSegments(final long timestamp);

    /**
     * Returns the active segments in the specified epoch.
     *
     * @param epoch epoch number.
     * @return list of numbers of segments active in the specified epoch.
     */
    CompletableFuture<List<Long>> getActiveSegments(int epoch);

    /**
     * Called to start metadata updates to stream store wrt new scale event.
     *
     * @param newRanges      key ranges of new segments to be created
     * @param scaleTimestamp scaling timestamp
     * @param runOnlyIfStarted run only if scale is started
     * @return sequence of newly created segments
     */
    CompletableFuture<EpochTransitionRecord> startScale(final List<Long> sealedSegments,
                                                        final List<AbstractMap.SimpleEntry<Double, Double>> newRanges,
                                                        final long scaleTimestamp,
                                                        final boolean runOnlyIfStarted);
    
    /**
     * Called after epochTransition entry is created. Implementation of this method should create new segments that are
     * specified in epochTransition in stream metadata tables.
     *
     * @param isManualScale flag to indicate if epoch transition should be migrated to latest epoch
     * @return Future, which when completed will indicate that new segments are created in the metadata store or wouldl
     * have failed with appropriate exception.
     */
    CompletableFuture<Void> scaleCreateNewSegments(boolean isManualScale);

    /**
     * Called after new segment creation is complete.
     */
    CompletableFuture<Void> scaleNewSegmentsCreated();

    /**
     * Called after sealing old segments is complete.
     *
     * @param sealedSegmentSizes sealed segments with absolute sizes
     */
    CompletableFuture<Void> scaleOldSegmentsSealed(Map<Long, Long> sealedSegmentSizes);

    /**
     * This method is called from Rolling transaction workflow after new transactions that are duplicate of active transactions
     * have been created successfully in segment store.
     * This method will update metadata records for epoch to add two new epochs, one for duplicate txn epoch where transactions
     * are merged and the other for duplicate active epoch.
     *
     * @param sealedTxnEpochSegments sealed segments from intermediate txn epoch with size at the time of sealing.
     * @param transactionEpoch epoch for transactions that need to be rolled over.
     * @param time timestamp
     *
     * @return CompletableFuture which upon completion will indicate that we have successfully created new epoch entries.
     */
    CompletableFuture<Void> rollingTxnNewSegmentsCreated(Map<Long, Long> sealedTxnEpochSegments, int transactionEpoch, long time);

    /**
     * This is the final step of rolling transaction and is called after old segments are sealed in segment store.
     * This should complete the epoch transition in the metadata store.
     *
     * @param sealedActiveEpochSegments sealed segments from active epoch with size at the time of sealing.
     * @param activeEpoch active epoch at the time when rolling transaction was started.
     * @param time sealed segments from active epoch with size at the time of sealing.
     * @return CompletableFuture which upon successful completion will indicate that rolling transaction is complete.
     */
    CompletableFuture<Void> rollingTxnActiveEpochSealed(Map<Long, Long> sealedActiveEpochSegments, int activeEpoch, long time);

    /**
     * Sets cold marker which is valid till the specified time stamp.
     * It creates a new marker if none is present or updates the previously set value.
     *
     * @param segmentId segment number to be marked as cold.
     * @param timestamp     time till when the marker is valid.
     * @return future
     */
    CompletableFuture<Void> setColdMarker(long segmentId, long timestamp);

    /**
     * Returns if a cold marker is set. Otherwise returns null.
     *
     * @param segmentId segment to check for cold.
     * @return future of either timestamp till when the marker is valid or null.
     */
    CompletableFuture<Long> getColdMarker(long segmentId);

    /**
     * Remove the cold marker for the segment.
     *
     * @param segmentId segment.
     * @return future
     */
    CompletableFuture<Void> removeColdMarker(long segmentId);

    /**
     * Method to generate new transaction Id.
     * This takes the latest epoch and increments the counter to compose a TransactionId with 32 bit MSB as epoch number and
     * 96 bit LSB as counter. 96 bit lsb is represented as msb32bit integer and lsb64 long
     * @param msb32Bit 32 bit msb for the counter
     * @param lsb64Bit 64 bit lsb for the counter
     * @return Completable Future which when completed contains a unique txn id within context of this stream.
     */
    CompletableFuture<UUID> generateNewTxnId(int msb32Bit, long lsb64Bit);

    /**
     * Method to start new transaction creation
     *
     * @return Details of created transaction.
     */
    CompletableFuture<VersionedTransactionData> createTransaction(final UUID txnId,
                                                                  final long lease,
                                                                  final long maxExecutionTime);


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
     * @param txId  transaction identifier.
     * @return      transaction status.
     */
    CompletableFuture<TxnStatus> commitTransaction(final UUID txId);

    /**
     * Aborts a transaction.
     * If already aborted, return TxnStatus.Aborted.
     * If committing/committed, return a failed future with IllegalStateException.
     *
     * @param txId  transaction identifier.
     * @return      transaction status.
     */
    CompletableFuture<TxnStatus> abortTransaction(final UUID txId);

    /**
     * Return whether any transaction is active on the stream.
     *
     * @return a boolean indicating whether a transaction is active on the stream.
     * Returns the number of transactions ongoing for the stream.
     */
    CompletableFuture<Integer> getNumberOfOngoingTransactions();

    CompletableFuture<Map<UUID, ActiveTxnRecord>> getActiveTxns();

    /**
     * Returns the currently active stream epoch.
     *
     * @param ignoreCached if ignore cache is set to true then fetch the value from the store. 
     * @return currently active stream epoch.
     */
    CompletableFuture<HistoryRecord> getActiveEpoch(boolean ignoreCached);

    /**
     * Returns the epoch record corresponding to supplied epoch.
     *
     * @param epoch epoch to retrieve record for
     * @return CompletableFuture which on completion will have the epoch record corresponding to the given epoch
     */
    CompletableFuture<HistoryRecord> getEpochRecord(int epoch);

    /**
     * Method to get stream size till the given stream cut
     *
     * @param streamCut stream cut
     * @return A CompletableFuture, that when completed, will contain size of stream till given cut.
     */
    CompletableFuture<Long> getSizeTillStreamCut(Map<Long, Long> streamCut);

    /**
     *Add a new Stream cut to retention set.
     *
     * @param streamCut stream cut record to add
     * @return future of operation
     */
    CompletableFuture<Void> addStreamCutToRetentionSet(final StreamCutRecord streamCut);

    /**
     * Get all stream cuts stored in the retention set.
     *
     * @return list of stream cut records
     */
    CompletableFuture<List<StreamCutRecord>> getRetentionStreamCuts();

    /**
     * Delete all stream cuts in the retention set that preceed the supplied stream cut.
     * Before is determined based on "recordingTime" for the stream cut.
     *
     * @param streamCut stream cut
     * @return future of operation
     */
    CompletableFuture<Void> deleteStreamCutBefore(final StreamCutRecord streamCut);

    /**
     * Method to fetch committing transaction record from the store for a given stream.
     * Note: this will not throw data not found exception if the committing transaction node is not found. Instead
     * it returns null.
     *
     * @param epoch epoch
     * @param txnsToCommit transactions to commit within the epoch
     * @return A completableFuture which, when completed, will contain committing transaction record if it exists, or null otherwise.
     */
    CompletableFuture<Void> createCommittingTransactionsRecord(final int epoch, final List<UUID> txnsToCommit);

    /**
     * Method to fetch committing transaction record from the store for a given stream.
     * Note: this will not throw data not found exception if the committing transaction node is not found. Instead
     * it returns null.
     *
     * @return A completableFuture which, when completed, will contain committing transaction record if it exists, or null otherwise.
     */
    CompletableFuture<CommittingTransactionsRecord> getCommittingTransactionsRecord();

    /**
     * Method to delete committing transaction record from the store for a given stream.
     *
     * @return A completableFuture which, when completed, will mean that deletion of txnCommitNode is complete.
     */
    CompletableFuture<Void> deleteCommittingTransactionsRecord();

    /**
     * Method to get all transactions in a given epoch.
     *
     * @param epoch epoch for which transactions are to be retrieved.
     * @return A completableFuture which when completed will contain a map of transaction id and its record.
     */
    CompletableFuture<Map<UUID, ActiveTxnRecord>> getTransactionsInEpoch(final int epoch);

    /**
     * This method attempts to create a new Waiting Request node and set the processor's name in the node.
     * If a node already exists, this attempt is ignored.
     *
     * @param processorName name of the request processor that is waiting to get an opportunity for processing.
     * @return CompletableFuture which indicates that a node was either created successfully or records the failure.
     */
    CompletableFuture<Void> createWaitingRequestIfAbsent(String processorName);

    /**
     * This method fetches existing waiting request processor's name if any. It returns null if no processor is waiting.
     *
     * @return CompletableFuture which has the name of the processor that had requested for a wait, or null if there was no
     * such request.
     */
    CompletableFuture<String> getWaitingRequestProcessor();

    /**
     * Delete existing waiting request processor if the name of the existing matches suppied processor name.
     *
     * @param processorName processor whose record is to be deleted.
     * @return CompletableFuture which indicates completion of processing.
     */
    CompletableFuture<Void> deleteWaitingRequestConditionally(String processorName);

    /**
     * Refresh the stream object. Typically to be used to invalidate any caches.
     * This allows us reuse of stream object without having to recreate a new stream object for each new operation
     */
    void refresh();
}
