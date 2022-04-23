/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.controller.store.stream;

import com.google.common.collect.ImmutableMap;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.controller.store.Version;
import io.pravega.controller.store.VersionedMetadata;
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
import io.pravega.controller.store.stream.records.StreamSubscriber;

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
    CompletableFuture<CreateStreamResponse> create(final StreamConfiguration configuration, final long createTimestamp, 
                                                   final int startingSegmentNumber, final OperationContext context);

    /**
     * Deletes an already SEALED stream.
     *
     * @return boolean indicating success.
     */
    CompletableFuture<Void> delete(OperationContext context);

    /**
     * Api to get creation time of the stream.
     * 
     * @return CompletableFuture which, upon completion, has the creation time of the stream. 
     */
    CompletableFuture<Long> getCreationTime(OperationContext context);
    
    /**
     * Starts updating the configuration of an existing stream.
     *
     * @param configuration new stream configuration.
     * @return future of new StreamConfigWithVersion.
     */
    CompletableFuture<Void> startUpdateConfiguration(final StreamConfiguration configuration, OperationContext context);

    /**
     * Completes an ongoing updates configuration of an existing stream.
     *
     * @return future of new StreamConfigWithVersion.
     * @param existing
     */
    CompletableFuture<Void> completeUpdateConfiguration(VersionedMetadata<StreamConfigurationRecord> existing, OperationContext context);

    /**
     * Fetches the current stream configuration.
     *
     * @return current stream configuration.
     */
    CompletableFuture<StreamConfiguration> getConfiguration(OperationContext context);

    /**
     * Fetches the current stream configuration.
     *
     * @return current stream configuration.
     */
    CompletableFuture<VersionedMetadata<StreamConfigurationRecord>> getVersionedConfigurationRecord(OperationContext context);

    /**
     * Create subscribers record for storing metadata about Stream Subscribers.
     * Also add this new Subscriber to the Record.
     * @param subscriber first subscriber to be added the SubscribersRecord in Stream Metadata.
     * @return future of operation.
     */
    CompletableFuture<Void> addSubscriber(String subscriber, long generation, OperationContext context);

    /**
     * Fetches the record corresponding to the subscriber
     * @return record holding information about this subscriber for the Stream.
     */
    CompletableFuture<VersionedMetadata<StreamSubscriber>> getSubscriberRecord(String subscriber, OperationContext context);

    /**
     * Fetches names for all subscribers of the Stream
     * @return iterator to iterate over all subscribers for the Stream.
     */
    CompletableFuture<List<String>> listSubscribers(OperationContext context);

    /**
     * Update subscribers record for the Stream.
     * @param previous - Subscriber Record that would be replaced by this update API
     * @param subscriber  subscriber name.
     * @param streamCut - new subscriber streamcut
     * @return future of operation.
     */
    CompletableFuture<Void> updateSubscriberStreamCut(final VersionedMetadata<StreamSubscriber> previous,
                                                      final String subscriber, long generation, 
                                                      final ImmutableMap<Long, Long> streamCut, OperationContext context);

    /**
     * Remove subscriber from list of Subscribers for the Stream.
     * @param subscriber  subscriber to be removed.
     * @return future of operation.
     */
    CompletableFuture<Void> deleteSubscriber(final String subscriber, final long generation, OperationContext context);

    /**
     * Starts truncating an existing stream.
     *
     * @param streamCut new stream cut.
     * @return future of new StreamProperty.
     */
    CompletableFuture<Void> startTruncation(final Map<Long, Long> streamCut, OperationContext context);

    /**
     * Completes an ongoing stream truncation.
     *
     * @return future of operation.
     * @param record
     */
    CompletableFuture<Void> completeTruncation(VersionedMetadata<StreamTruncationRecord> record, OperationContext context);

    /**
     * Fetches the current stream cut.
     *
     * @return current stream cut.
     */
    CompletableFuture<VersionedMetadata<StreamTruncationRecord>> getTruncationRecord(OperationContext context);

    /**
     * Api to get the current state with its current version.
     *
     * @return Future which when completed has the versioned state.
     */
    CompletableFuture<VersionedMetadata<State>> getVersionedState(OperationContext context);

    /**
     * Update the state of the stream.
     *
     * @return boolean indicating whether the state of stream is updated.
     */
    CompletableFuture<Void> updateState(final State state, OperationContext context);

    /**
     * Api to update versioned state as a CAS operation.
     *
     * @param state desired state
     * @return Future which when completed contains the updated state and version if successful or exception otherwise.
     */
    CompletableFuture<VersionedMetadata<State>> updateVersionedState(final VersionedMetadata<State> state, 
                                                                     final State newState, OperationContext context);
    
    /**
     * Get the state of the stream.
     *
     * @return state othe given stream.
     * @param ignoreCached ignore cached value and fetch from store
     * @param context
     */
    CompletableFuture<State> getState(boolean ignoreCached, OperationContext context);

    /**
     * Fetches details of specified segment.
     *
     * @param segmentId segment number.
     * @return segment at given number.
     */
    CompletableFuture<StreamSegmentRecord> getSegment(final long segmentId, OperationContext context);

    /**
     * Fetches all segment ids in the stream between head of the stream and tail of the stream. 
     *
     * @return Future which when completed contains a list of all segments in the stream.
     */
    CompletableFuture<Set<Long>> getAllSegmentIds(OperationContext context);

    /**
     * Api to get all scale metadata records between given time ranges. 
     * @param from from time
     * @param to to time
     * @return Future which when completed contains a list of scale metadata records corresponding to all scale operations 
     * between given time ranges. 
     */
    CompletableFuture<List<ScaleMetadata>> getScaleMetadata(long from, long to, OperationContext context);

    /**
     * @param segmentId segment number.
     * @return successors of specified segment mapped to the list of their predecessors
     */
    CompletableFuture<Map<StreamSegmentRecord, List<Long>>> getSuccessorsWithPredecessors(final long segmentId, OperationContext context);

    /**
     * Method to get all segments between given stream cuts.
     * Either from or to can be either well formed stream cuts OR empty sets indicating unbounded cuts.
     * If from is unbounded, then head is taken as from, and if to is unbounded then tail is taken as the end.
     *
     * @param from from stream cut.
     * @param to to stream cut.
     * @return Future which when completed gives list of segments between given streamcuts.
     */
    CompletableFuture<List<StreamSegmentRecord>> getSegmentsBetweenStreamCuts(final Map<Long, Long> from, final Map<Long, Long> to, OperationContext context);

    /**
     * Method to validate stream cut based on its definition - disjoint sets that cover the entire range of keyspace.
     * @param streamCut stream cut to validate.
     * @return Future which when completed has the result of validation check (true for valid and false for illegal streamCuts).
     */
    CompletableFuture<Boolean> isStreamCutValid(Map<Long, Long> streamCut, OperationContext context);

    /**
     * Method to get segments at the current tail of the stream.
     * 
     * @return Future which when completed will contain currently active segments
     */
    CompletableFuture<List<StreamSegmentRecord>> getActiveSegments(OperationContext context);
    
    /**
     * Method to get segments at the head of the stream.
     * 
     * @return Future which when completed will contain segments at head of stream with offsets
     */
    CompletableFuture<Map<StreamSegmentRecord, Long>> getSegmentsAtHead(OperationContext context);
    
    /**
     * Returns the active segments in the specified epoch.
     *
     * @param epoch epoch number.
     * @return list of numbers of segments active in the specified epoch.
     */
    CompletableFuture<List<StreamSegmentRecord>> getSegmentsInEpoch(int epoch, OperationContext context);

    /**
     * Method to get versioned Epoch Transition Record from store.
     * 
     * @return Future which when completed contains existing epoch transition record with version
     */
    CompletableFuture<VersionedMetadata<EpochTransitionRecord>> getEpochTransition(OperationContext context);

    /**
     * Reset the given epoch transition to EMPTY.
     * 
     * @param record  existing versioned record.
     * @return A future which when completed would have reset the epoch transition to empty.                 
     */
    CompletableFuture<VersionedMetadata<EpochTransitionRecord>> resetEpochTransition(
            VersionedMetadata<EpochTransitionRecord> record, OperationContext context);

    /**
     * Called to start metadata updates to stream store with respect to new scale request. This method should only update
     * the epochTransition record to reflect current request. It should not initiate the scale workflow. 
     * This should be called for both auto scale and manual scale. 
     * In case of rolling transactions, this record may become invalid and can be discarded during the startScale phase
     * of scale workflow. 
     *
     * @param sealedSegments segments to seal
     * @param newRanges      key ranges of new segments to be created
     * @param scaleTimestamp scaling timestamp
     * @param record existing epoch transition record
     * @return Future which when completed will encapsulate the epoch transition record with its version. 
     */
    CompletableFuture<VersionedMetadata<EpochTransitionRecord>> submitScale(final List<Long> sealedSegments,
                                                                            final List<Map.Entry<Double, Double>> newRanges,
                                                                            final long scaleTimestamp,
                                                                            final VersionedMetadata<EpochTransitionRecord> record,
                                                                            final OperationContext context);

    /**
     * Method to start a new scale. This method will check if epoch transition record is consistent or if
     * a rolling transaction has rendered it inconsistent with the state in store.
     * For manual scale this method will migrate the epoch transaction. For auto scale, it will discard any
     * inconsistent record and reset the state.
     *
     * Note: the state management is outside the purview of this method and should be done explicitly by the caller. 
     * 
     * @param isManualScale  flag to indicate that the processing is being performed for manual scale
     * @param record previous versioned record
     * @param state  previous versioned state
     * @return future Future which when completed contains updated epoch transition record with version or exception otherwise.
     * @throws IllegalStateException if epoch transition is inconsistent. 
     */
    CompletableFuture<VersionedMetadata<EpochTransitionRecord>> startScale(final boolean isManualScale,
                                                                           final VersionedMetadata<EpochTransitionRecord> record,
                                                                           final VersionedMetadata<State> state, 
                                                                           final OperationContext context);
    
    /**
     * This method is called after new segment creation is complete in segment store. The store should update its metadata 
     * such that it can return successors for segmentsToSeal if required. This should require store to create new epoch
     * record corresponding to these new segments in idempotent fashion. 
     * 
     * @param record  existing versioned record
     * @return Future, which when completed will indicate successful and idempotent update of metadata corresponding to
     * new segments information created in the store. 
     */
    CompletableFuture<VersionedMetadata<EpochTransitionRecord>> scaleCreateNewEpoch(VersionedMetadata<EpochTransitionRecord> record, 
                                                                                    OperationContext context);


    /**
     * Called after sealing old segments is complete in segment store. 
     * The implementation of this method should update epoch metadata for the given scale input in an idempotent fashion
     * such that active epoch at least reflects the new epoch updated by this method's call. 
     *
     * @param sealedSegmentSizes sealed segments with absolute sizes
     * @param record existing epoch transition record
     * @return Future, which when completed will indicate successful and idempotent metadata update corresponding to
     * sealing of old segments in the store. 
     */
    CompletableFuture<Void> scaleOldSegmentsSealed(Map<Long, Long> sealedSegmentSizes, VersionedMetadata<EpochTransitionRecord> record, 
                                                   OperationContext context);

    /**
     * Called at the end of scale workflow to let the store know to complete the scale. This should reset the epoch transition
     * record to signal completion of scale workflow. 
     * Note: the state management is outside the purview of this method and should be done explicitly by the caller. 
     * 
     * @param record  existing versioned record.
     * @return A future which when completed indicates the completion current scale workflow.                 
     */
    CompletableFuture<Void> completeScale(VersionedMetadata<EpochTransitionRecord> record, OperationContext context);

    /**
     * Api to indicate to store to start rolling transaction. 
     * The store attempts to update CommittingTransactionsRecord with details about rolling transaction information, 
     * specifically updating active epoch in the aforesaid record. 
     * 
     * @param activeEpoch active epoch
     * @param existing versioned committing transactions record that has to be updated
     * @return A future which when completed will capture updated versioned committing transactions record that represents 
     * an ongoing rolling transaction.
     */
    CompletableFuture<VersionedMetadata<CommittingTransactionsRecord>> startRollingTxn(int activeEpoch,
                                                                                       VersionedMetadata<CommittingTransactionsRecord> existing,
                                                                                       OperationContext context);

    /**
     * This method is called from Rolling transaction workflow after new transactions that are duplicate of active transactions
     * have been created successfully in segment store.
     * This method will update metadata records for epoch to add two new epochs, one for duplicate txn epoch where transactions
     * are merged and the other for duplicate active epoch.
     *
     * @param sealedTxnEpochSegments sealed segments from intermediate txn epoch with size at the time of sealing.
     * @param time timestamp
     *
     * @return CompletableFuture which upon completion will indicate that we have successfully created new epoch entries.
     */
    CompletableFuture<Void> rollingTxnCreateDuplicateEpochs(Map<Long, Long> sealedTxnEpochSegments,
                                                           long time, VersionedMetadata<CommittingTransactionsRecord> existing, 
                                                            OperationContext context);

    /**
     * This is the final step of rolling transaction and is called after old segments are sealed in segment store.
     * Post completion of this step, the active epoch should be updated to reflect end of this rolling transaction. 
     *
     * @param sealedActiveEpochSegments sealed segments from active epoch with size at the time of sealing.
     * @return CompletableFuture which upon successful completion will indicate that rolling transaction is complete.
     */
    CompletableFuture<Void> completeRollingTxn(Map<Long, Long> sealedActiveEpochSegments, 
                                               VersionedMetadata<CommittingTransactionsRecord> existing, 
                                               OperationContext context);

    /**
     * Sets cold marker which is valid till the specified time stamp.
     * It creates a new marker if none is present or updates the previously set value.
     *
     * @param segmentId segment number to be marked as cold.
     * @param timestamp     time till when the marker is valid.
     * @return future
     */
    CompletableFuture<Void> setColdMarker(long segmentId, long timestamp, OperationContext context);

    /**
     * Returns if a cold marker is set. Otherwise returns null.
     *
     * @param segmentId segment to check for cold.
     * @return future of either timestamp till when the marker is valid or null.
     */
    CompletableFuture<Long> getColdMarker(long segmentId, OperationContext context);

    /**
     * Remove the cold marker for the segment.
     *
     * @param segmentId segment.
     * @return future
     */
    CompletableFuture<Void> removeColdMarker(long segmentId, OperationContext context);

    /**
     * Method to generate new transaction Id.
     * This takes the latest epoch and increments the counter to compose a TransactionId with 32 bit MSB as epoch number and
     * 96 bit LSB as counter. 96 bit lsb is represented as msb32bit integer and lsb64 long
     * @param msb32Bit 32 bit msb for the counter
     * @param lsb64Bit 64 bit lsb for the counter
     * @return Completable Future which when completed contains a unique txn id within context of this stream.
     */
    CompletableFuture<UUID> generateNewTxnId(int msb32Bit, long lsb64Bit, OperationContext context);

    /**
     * Method to start new transaction creation
     *
     * @return Details of created transaction.
     */
    CompletableFuture<VersionedTransactionData> createTransaction(final UUID txnId,
                                                                  final long lease,
                                                                  final long maxExecutionTime, OperationContext context);


    /**
     * Heartbeat method to keep transaction open for at least lease amount of time.
     *
     * @param txnData Transaction data.
     * @param lease Lease period in ms.
     * @return Transaction metadata along with its version.
     */
    CompletableFuture<VersionedTransactionData> pingTransaction(final VersionedTransactionData txnData, final long lease, 
                                                                OperationContext context);

    /**
     * Fetch transaction metadata along with its version.
     *
     * @param txId transaction id.
     * @return transaction metadata along with its version.
     */
    CompletableFuture<VersionedTransactionData> getTransactionData(UUID txId, OperationContext context);

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
                                                                       final Optional<Version> version,
                                                                       final String writerId,
                                                                       final long timestamp,
                                                                       final OperationContext context);

    /**
     * Returns transaction's status
     *
     * @param txId transaction identifier.
     * @return     transaction status.
     */
    CompletableFuture<TxnStatus> checkTransactionStatus(final UUID txId, OperationContext context);

    /**
     * Aborts a transaction.
     * If already aborted, return TxnStatus.Aborted.
     * If committing/committed, return a failed future with IllegalStateException.
     *
     * @param txId  transaction identifier.
     * @return      transaction status.
     */
    CompletableFuture<TxnStatus> abortTransaction(final UUID txId, OperationContext context);

    /**
     * Return whether any transaction is active on the stream.
     *
     * @return a boolean indicating whether a transaction is active on the stream.
     * Returns the number of transactions ongoing for the stream.
     */
    CompletableFuture<Long> getNumberOfOngoingTransactions(OperationContext context);

    /**
     * Api to get all active transactions as a map of transaction id to Active transaction record
     * @return A future which upon completion has a map of transaction ids to transaction metadata for all active transactions
     * on the stream.
     */
    CompletableFuture<Map<UUID, ActiveTxnRecord>> getActiveTxns(OperationContext context);

    /**
     * API to retrieve List transaction in completed (COMMITTED/ABORTED) state
     * from most recent batch.
     *
     * @param context Operational context.
     * @return Map having transactionId and transaction status.
     */
    CompletableFuture<Map<UUID, TxnStatus>> listCompletedTxns(final OperationContext context);

    /**
     * Returns the currently active stream epoch.
     *
     * @param ignoreCached if ignore cache is set to true then fetch the value from the store. 
     * @return currently active stream epoch.
     */
    CompletableFuture<EpochRecord> getActiveEpoch(boolean ignoreCached, OperationContext context);

    /**
     * Returns the epoch record corresponding to supplied epoch.
     *
     * @param epoch epoch to retrieve record for
     * @return CompletableFuture which on completion will have the epoch record corresponding to the given epoch
     */
    CompletableFuture<EpochRecord> getEpochRecord(int epoch, OperationContext context);

    /**
     * Method to get stream size till the given stream cut
     *
     * @param streamCut stream cut
     * @return A CompletableFuture, that when completed, will contain size of stream till given cut.
     */
    CompletableFuture<Long> getSizeTillStreamCut(Map<Long, Long> streamCut, Optional<StreamCutRecord> reference,
                                                 OperationContext context);

    /**
     *Add a new Stream cut to retention set.
     *
     * @param streamCut stream cut record to add
     * @return future of operation
     */
    CompletableFuture<Void> addStreamCutToRetentionSet(final StreamCutRecord streamCut, OperationContext context);

    /**
     * Get all stream cuts stored in the retention set.
     *
     * @return list of stream cut records
     */
    CompletableFuture<RetentionSet> getRetentionSet(OperationContext context);

    /**
     * Method to retrieve stream cut record corresponding to reference record.
     * 
     * @param record reference record.
     * @return Future which when completed will contain requested stream cut record.
     */
    CompletableFuture<StreamCutRecord> getStreamCutRecord(StreamCutReferenceRecord record, OperationContext context);
        
    /**
     * Delete all stream cuts in the retention set that preceed the supplied stream cut.
     * Before is determined based on "recordingTime" for the stream cut.
     *
     * @param streamCut stream cut
     * @return future of operation
     */
    CompletableFuture<Void> deleteStreamCutBefore(final StreamCutReferenceRecord streamCut, OperationContext context);

    /**
     * Method to fetch committing transaction record from the store for a given stream.
     * Note: this will not throw data not found exception if the committing transaction node is not found. Instead
     * it returns null.
     * @param limit maximum number of transactions to include in a commit batch 
     * @return A completableFuture which, when completed, will contain committing transaction record if it exists, or null otherwise.
     */
    CompletableFuture<Map.Entry<VersionedMetadata<CommittingTransactionsRecord>, List<VersionedTransactionData>>> startCommittingTransactions(int limit,
                                                                                                   OperationContext context);

    /**
     * Method to fetch committing transaction record from the store for a given stream.
     * Note: this will not throw data not found exception if the committing transaction node is not found. Instead
     * it returns null.
     *
     * @return A completableFuture which, when completed, will contain committing transaction record if it exists, or null otherwise.
     */
    CompletableFuture<VersionedMetadata<CommittingTransactionsRecord>> getVersionedCommitTransactionsRecord(OperationContext context);

    /**
     * Method to reset committing transaction record from the store for a given stream.
     * This method is also responsible for marking all involved transactions as committed. 
     * It also generates marks for writers if applicable before marking the said transactions 
     * as committed. 
     *
     * @return A completableFuture which, when completed, will mean that deletion of txnCommitNode is complete.
     * @param record existing versioned record.
     */
    CompletableFuture<Void> completeCommittingTransactions(VersionedMetadata<CommittingTransactionsRecord> record,
                                                           OperationContext context,
                                                           Map<String, TxnWriterMark> writerMarks);
    
    /**
     * This method attempts to create a new Waiting Request node and set the processor's name in the node.
     * If a node already exists, this attempt is ignored.
     *
     * @param processorName name of the request processor that is waiting to get an opportunity for processing.
     * @return CompletableFuture which indicates that a node was either created successfully or records the failure.
     */
    CompletableFuture<Void> createWaitingRequestIfAbsent(String processorName, OperationContext context);

    /**
     * This method fetches existing waiting request processor's name if any. It returns null if no processor is waiting.
     *
     * @return CompletableFuture which has the name of the processor that had requested for a wait, or null if there was no
     * such request.
     */
    CompletableFuture<String> getWaitingRequestProcessor(OperationContext context);

    /**
     * Delete existing waiting request processor if the name of the existing matches suppied processor name.
     *
     * @param processorName processor whose record is to be deleted.
     * @return CompletableFuture which indicates completion of processing.
     */
    CompletableFuture<Void> deleteWaitingRequestConditionally(String processorName, OperationContext context);

    /**
     * Method to record writer's mark in the metadata store. If this is a known writer, its mark is updated if it advances 
     * both time and position from the previously recorded position.    
     * @param writer writer id
     * @param timestamp mark timestamp
     * @param position writer position 
     * @return A completableFuture, which when completed, will have recorded the new mark for the writer. 
     */
    CompletableFuture<WriterTimestampResponse> noteWriterMark(String writer, long timestamp, Map<Long, Long> position,
                                                              OperationContext context);

    /**
     * Method to set a writer to be shutting down for its writer mark record.
     * 
     * @param writer writer id
     * @return A completableFuture, which when completed, will have shutdown the writer.  
     */
    CompletableFuture<Void> shutdownWriter(String writer, OperationContext context);

    /**
     * Method to remove writer specific metadata from the metadata store if existing writermark matches given writermark. 
     * Remove method is idempotent. So if writer doesnt exist, this method will declare success. 
     * @param writer writer id
     * @param writerMark writer mark
     * @return A completableFuture, which when completed, will have removed writer metadata. 
     */
    CompletableFuture<Void> removeWriter(String writer, WriterMark writerMark, OperationContext context);

    /**
     * Method to retrieve writer's latest recorded mark.  
     * @param writer writer id
     * @return A completableFuture, which when completed, will contain writer's mark.  
     */
    CompletableFuture<WriterMark> getWriterMark(String writer, OperationContext context);

    /**
     * Method to retrieve latest recorded mark for all known writers.  
     * @return A completableFuture, which when completed, will contain map of writer to respective marks.  
     */
    CompletableFuture<Map<String, WriterMark>> getAllWriterMarks(OperationContext context);

    /**
     * Refresh the stream object. Typically to be used to invalidate any caches.
     * This allows us reuse of stream object without having to recreate a new stream object for each new operation
     */
    void refresh();

    /**
     * Method to get the requested chunk of the HistoryTimeSeries.
     *
     * @param chunkNumber chunk number.
     * @return Completable future that, upon completion, holds the requested HistoryTimeSeries chunk.
     */
    CompletableFuture<HistoryTimeSeries> getHistoryTimeSeriesChunk(int chunkNumber, OperationContext context);

    /**
     * Method to get the requested shard of sealed segments map.
     *
     * @param shardNumber shard number.
     * @return Completable future that, upon completion, holds the requested sealed segment map shard.
     */
    CompletableFuture<SealedSegmentsMapShard> getSealedSegmentSizeMapShard(int shardNumber, OperationContext context);

    /**
     * Method to get epoch in which a segment was sealed.
     * Returns a negative number if segment is not sealed.  
     *
     * @param segmentId  segment id.
     * @return Completable future that, upon completion, holds the epoch in which the segment was sealed.
     */
    CompletableFuture<Integer> getSegmentSealedEpoch(long segmentId, OperationContext context);

    /**
     * Compares two Stream cuts and returns StreamCutComparison:
     * A streamcut is considered greater than other if for all key ranges the segment/offset in one streamcut is ahead of 
     * second streamcut. 
     * 
     * @param cut1 streamcut to check
     * @param cut2 streamcut to check against. 
     * @param context Operation Context
     * @return CompletableFuture which, upon completion, will contain comparison result of streamcut1 and streamcut2.
     */
    CompletableFuture<StreamCutComparison> compareStreamCuts(Map<Long, Long> cut1, Map<Long, Long> cut2, OperationContext context);

    /**
     * Finds the latest streamcutreference record from retentionset that is strictly before the supplied streamcut.
     * 
     * @param streamCut streamcut to check
     * @return A completable future which when completed will the reference record to the latest stream cut from retention set which
     * is strictly before the supplied streamcut. 
     */
    CompletableFuture<StreamCutReferenceRecord> findStreamCutReferenceRecordBefore(Map<Long, Long> streamCut,
                                                                                   RetentionSet retentionSet, 
                                                                                   OperationContext context);

    /**
     * Finds the cumulative number of splits and merges in this Stream till this epoch.
     *
     * @param epochRecord EpochRecord till which to compute splits/merges.
     * @return A completable future which when completed will return an Entry<Key, Value>,
     * where 'Key' is number of splits and 'Value' is number of merges in the Stream till this epoch.
     */
    CompletableFuture<SimpleEntry<Long, Long>> getSplitMergeCountsTillEpoch(EpochRecord epochRecord, OperationContext context);
}
