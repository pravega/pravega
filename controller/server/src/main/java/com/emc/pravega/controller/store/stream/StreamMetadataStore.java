/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.store.stream;

import com.emc.pravega.controller.store.stream.tables.ActiveTxRecordWithStream;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.impl.TxnStatus;

import java.util.AbstractMap.SimpleEntry;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Stream Metadata.
 */
//TODO: Add scope to most methods.
public interface StreamMetadataStore {

    /**
     * Creates a new stream with the given name and configuration.
     *
     * @param name          stream name.
     * @param configuration stream configuration.
     * @param createTimestamp stream creation timestamp.
     * @return boolean indicating whether the stream was created
     */
    CompletableFuture<Boolean> createStream(final String name,
                                            final StreamConfiguration configuration,
                                            final long createTimestamp);

    /**
     * Updates the configuration of an existing stream.
     *
     * @param name          stream name.
     * @param configuration new stream configuration
     * @return boolean indicating whether the stream was updated
     */
    CompletableFuture<Boolean> updateConfiguration(final String name, final StreamConfiguration configuration);

    /**
     * Fetches the current stream configuration.
     *
     * @param name stream name.
     * @return current stream configuration.
     */
    CompletableFuture<StreamConfiguration> getConfiguration(final String name);

    /**
     * Set the stream state to sealed.
     * @param name stream name.
     * @return boolean indicating whether the stream was updated.
     */
    CompletableFuture<Boolean> setSealed(final String name);

    /**
     * Get the stream sealed status.
     * @param name stream name.
     * @return boolean indicating whether the stream is sealed.
     */
    CompletableFuture<Boolean> isSealed(final String name);

    /**
     * Get Segment.
     *  @param name   stream name.
     * @param number segment number.
     * @return segment at given number.
     */
    CompletableFuture<Segment> getSegment(final String name, final int number);

    /**
     * Get active segments.
     *
     * @param name stream name.
     * @return currently active segments
     */
    CompletableFuture<List<Segment>> getActiveSegments(final String name);

    /**
     * Get active segments at given timestamp.
     *
     * @param name      stream name.
     * @param timestamp point in time.
     * @return the list of segments active at timestamp.
     */
    CompletableFuture<SegmentFutures> getActiveSegments(final String name, final long timestamp);

    /**
     * Given a segment return a map containing the numbers of the segments immediately succeeding it
     * mapped to a list of the segments they succeed.
     *
     * @param streamName the stream name.
     * @param segmentNumber the segment number
     * @return segments that immediately follow the specified segment and the segments they follow.
     */
    public CompletableFuture<Map<Integer, List<Integer>>> getSuccessors(final String streamName,
            final int segmentNumber);

    /**
     * Scales in or out the currently set of active segments of a stream.
     *
     * @param name           stream name.
     * @param sealedSegments segments to be sealed
     * @param newRanges      new key ranges to be added to the stream which maps to a new segment per range in the stream
     * @param scaleTimestamp scaling timestamp, all sealed segments shall have it as their end time and
     *                       all new segments shall have it as their start time.
     * @return the list of newly created segments
     */
    CompletableFuture<List<Segment>> scale(final String name,
                                           final List<Integer> sealedSegments,
                                           final List<SimpleEntry<Double, Double>> newRanges,
                                           final long scaleTimestamp);

    /**
     * Method to create a new transaction on a stream.
     *
     * @param scope  scope
     * @param stream stream
     * @return new Transaction Id
     */
    CompletableFuture<UUID> createTransaction(final String scope, final String stream);

    /**
     * Get transaction status from the stream store.
     *
     * @param scope  scope
     * @param stream stream
     * @param txId   transaction id
     * @return
     */
    CompletableFuture<TxnStatus> transactionStatus(final String scope, final String stream, final UUID txId);

    /**
     * Update stream store to mark transaction as committed.
     *
     * @param scope  scope
     * @param stream stream
     * @param txId   transaction id
     * @return
     */
    CompletableFuture<TxnStatus> commitTransaction(final String scope, final String stream, final UUID txId);

    /**
     * Update stream store to mark transaction as sealed.
     *
     * @param scope  scope
     * @param stream stream
     * @param txId   transaction id
     * @return
     */
    CompletableFuture<TxnStatus> sealTransaction(final String scope, final String stream, final UUID txId);

    /**
     * Update stream store to mark the transaction as aborted.
     *
     * @param scope  scope
     * @param stream stream
     * @param txId   transaction id
     * @return
     */
    CompletableFuture<TxnStatus> abortTransaction(final String scope, final String stream, final UUID txId);

    /**
     * Returns a boolean indicating whether any transaction is active on the specified stream.
     * @param scope  scope.
     * @param stream stream.
     * @return boolean indicating whether any transaction is active on the specified stream.
     */
    CompletableFuture<Boolean> isTransactionOngoing(final String scope, final String stream);

    /**
     * Returns all active transactions for all streams.
     * This is used for periodically identifying timedout transactions which can be aborted
     *
     * @return
     */
    CompletableFuture<List<ActiveTxRecordWithStream>> getAllActiveTx();
}
