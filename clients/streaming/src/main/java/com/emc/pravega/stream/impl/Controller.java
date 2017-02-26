/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.stream.impl;

import com.emc.pravega.common.netty.PravegaNodeUri;
import com.emc.pravega.controller.stream.api.v1.CreateStreamStatus;
import com.emc.pravega.controller.stream.api.v1.ScaleResponse;
import com.emc.pravega.controller.stream.api.v1.UpdateStreamStatus;
import com.emc.pravega.stream.EventStreamWriter;
import com.emc.pravega.stream.Segment;
import com.emc.pravega.stream.Stream;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.Transaction;
import com.emc.pravega.stream.TxnFailedException;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Stream Controller APIs.
 */
public interface Controller {

    // Controller Apis for administrative action for streams

    /**
     * Api to create stream.
     *
     * @param streamConfig Stream configuration
     * @return Status of create stream operation.
     */
    CompletableFuture<CreateStreamStatus> createStream(final StreamConfiguration streamConfig);

    /**
     * Api to alter stream.
     *
     * @param streamConfig Stream configuration to updated
     * @return Status of update stream operation.
     */
    CompletableFuture<UpdateStreamStatus> alterStream(final StreamConfiguration streamConfig);

    /**
     * Api to seal stream.
     * 
     * @param scope Scope
     * @param streamName Stream name
     * @return Status of update stream operation.
     */
    CompletableFuture<UpdateStreamStatus> sealStream(final String scope, final String streamName);

    /**
     * API to merge or split stream segments.
     * 
     * @param stream Stream object.
     * @param sealedSegments List of segments to be sealed.
     * @param newKeyRanges Key ranges after scaling the stream.
     * @return Status of scale operation.
     */
    CompletableFuture<ScaleResponse> scaleStream(final Stream stream, final List<Integer> sealedSegments,
            final Map<Double, Double> newKeyRanges);

    // Controller Apis called by pravega producers for getting stream specific information

    /**
     * Api to get list of current segments for the stream to write to.
     * 
     * @param scope Scope
     * @param streamName Stream name
     * @return Current stream segments.
     */
    CompletableFuture<StreamSegments> getCurrentSegments(final String scope, final String streamName);

    /**
     * Api to create a new transaction. The transaction timeout is relative to the creation time.
     * 
     * @param stream stream name
     * @param lease Time for which transaction shall remain open with sending any heartbeat.
     * @param maxExecutionTime Maximum time for which client may extend txn lease.
     * @param scaleGracePeriod Maximum time for which client may extend txn lease once
     *                         the scaling operation is initiated on the txn stream.
     * @return Transaction identifier.
     */
    CompletableFuture<UUID> createTransaction(final Stream stream, final long lease, final long maxExecutionTime,
                                              final long scaleGracePeriod);

    /**
     * API to send transaction heartbeat and increase the transaction timeout by lease amount of milliseconds.
     *
     * @param stream stream name
     * @param txId transaction id
     * @param lease Time for which transaction shall remain open with sending any heartbeat.
     */
    CompletableFuture<Void> pingTransaction(final Stream stream, final UUID txId, final long lease);

    /**
     * Commits a transaction, atomically committing all events to the stream, subject to the
     * ordering guarantees specified in {@link EventStreamWriter}. Will fail with
     * {@link TxnFailedException} if the transaction has already been committed or aborted.
     * 
     * @param stream Stream name
     * @param txId Transaction id
     * @return Void or TxnFailedException
     */
    CompletableFuture<Void> commitTransaction(final Stream stream, final UUID txId);

    /**
     * Aborts a transaction. No events written to it may be read, and no further events may be
     * written. Will fail with {@link TxnFailedException} if the transaction has already been
     * committed or aborted.
     * 
     * @param stream Stream name
     * @param txId Transaction id
     * @return Void or TxnFailedException
     */
    CompletableFuture<Void> abortTransaction(final Stream stream, final UUID txId);

    /**
     * Returns the status of the specified transaction.
     * 
     * @param stream Stream name
     * @param txId Transaction id
     * @return Transaction status.
     */
    CompletableFuture<Transaction.Status> checkTransactionStatus(final Stream stream, final UUID txId);

    // Controller Apis that are called by readers

    /**
     * Given a timestamp and a number of readers, returns a position object for each reader that collectively
     * include all of the segments that exist at that time in the stream.
     *
     * @param stream Name
     * @param timestamp Timestamp for getting position objects
     * @param count Number of position objects
     * @return List of position objects.
     */
    CompletableFuture<List<PositionInternal>> getPositions(final Stream stream, final long timestamp, final int count);

    /**
     * Returns a Map containing each of the segments that are successors to the segment requested mapped to a
     * list of their predecessors.
     * 
     * In the event of a scale up the newly created segments contain a subset of the keyspace of the original
     * segment and their only predecessor is the segment that was split. Example: If there are two segments A
     * and B. A scaling event split A into two new segments C and D. The successors of A are C and D. So
     * calling this method with A would return {C -> A, D -> A}
     * 
     * In the event of a scale down there would be one segment the succeeds multiple. So it would contain the
     * union of the keyspace of its predecessors. So calling with that segment would map to multiple segments.
     * Example: If there are two segments A and B. A and B are merged into a segment C. The successor of A is
     * C. so calling this method with A would return {C -> {A, B}}
     * 
     * If a segment has not been sealed, it may not have successors now even though it might in the future.
     * The successors to a sealed segment are always known and returned. Example: If there is only one segment
     * A and it is not sealed, and no scaling events have occurred calling this with a would return an empty
     * map.
     * 
     * @param segment The segment whose successors should be looked up.
     * @return A mapping from Successor to the list of all of the Successor's predecessors
     */
    CompletableFuture<Map<Segment, List<Integer>>> getSuccessors(final Segment segment);

    // Controller Apis that are called by writers and readers

    /**
     * Given a segment return the endpoint that currently is the owner of that segment.
     * <p>
     * This is called when a reader or a writer needs to determine which host/server it needs to contact to
     * read and write, respectively. The result of this function can be cached until the endpoint is
     * unreachable or indicates it is no longer the owner.
     *
     * @param qualifiedSegmentName The name of the segment. Usually obtained from
     *        {@link Segment#getScopedName()}.
     * @return Pravega node URI.
     */
    CompletableFuture<PravegaNodeUri> getEndpointForSegment(final String qualifiedSegmentName);

}
