/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.stream.impl;

import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.StreamCut;
import io.pravega.client.stream.Transaction;
import io.pravega.client.stream.TxnFailedException;
import io.pravega.common.util.AsyncIterator;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Stream Controller APIs.
 */
public interface Controller extends AutoCloseable {

    // Controller Apis for administrative action for streams

    /**
     * API to create a scope. The future completes with true in the case the scope did not exist
     * when the controller executed the operation. In the case of a re-attempt to create the
     * same scope, the future completes with false to indicate that the scope existed when the
     * controller executed the operation.
     *
     * @param scopeName Scope name.
     * @return A future which will throw if the operation fails, otherwise returning a boolean to
     *         indicate that the scope was added because it did not already exist.
     */
    CompletableFuture<Boolean> createScope(final String scopeName);

    /**
     * Gets an async iterator on streams in scope.
     *
     * @param scopeName The name of the scope for which to list streams in.
     * @return An AsyncIterator which can be used to iterate over all Streams in the scope. 
     */
    AsyncIterator<Stream> listStreams(final String scopeName);

    /**
     * API to delete a scope. Note that a scope can only be deleted in the case is it empty. If
     * the scope contains at least one stream, then the delete request will fail.
     *
     * @param scopeName Scope name.
     * @return A future which will throw if the operation fails, otherwise returning a boolean to
     *         indicate that the scope was removed because it existed.
     */
    CompletableFuture<Boolean> deleteScope(final String scopeName);

    /**
     * API to create a stream. The future completes with true in the case the stream did not
     * exist when the controller executed the operation. In the case of a re-attempt to create
     * the same stream, the future completes with false to indicate that the stream existed when
     * the controller executed the operation.
     * 
     * @param scope Scope
     * @param streamName Stream name
     * @param streamConfig Stream configuration
     * @return A future which will throw if the operation fails, otherwise returning a boolean to
     *         indicate that the stream was added because it did not already exist.
     */
    CompletableFuture<Boolean> createStream(final String scope, final String streamName, final StreamConfiguration streamConfig);

    /**
     * API to update the configuration of a stream.
     * @param scope Scope
     * @param streamName Stream name
     * @param streamConfig Stream configuration to updated
     * @return A future which will throw if the operation fails, otherwise returning a boolean to
     *         indicate that the stream was updated because the config is now different from before.
     */
    CompletableFuture<Boolean> updateStream(final String scope, final String streamName, final StreamConfiguration streamConfig);

    /**
     * API to Truncate stream. This api takes a stream cut point which corresponds to a cut in
     * the stream segments which is consistent and covers the entire key range space.
     *
     * @param scope      Scope
     * @param streamName Stream
     * @param streamCut  Stream cut to updated
     * @return A future which will throw if the operation fails, otherwise returning a boolean to
     * indicate that the stream was truncated at the supplied cut.
     */
    CompletableFuture<Boolean> truncateStream(final String scope, final String streamName, final StreamCut streamCut);

    /**
     * API to seal a stream.
     * 
     * @param scope Scope
     * @param streamName Stream name
     * @return A future which will throw if the operation fails, otherwise returning a boolean to
     *         indicate that the stream was sealed because it was not previously.
     */
    CompletableFuture<Boolean> sealStream(final String scope, final String streamName);

    /**
     * API to delete a stream. Only a sealed stream can be deleted.
     *
     * @param scope      Scope name.
     * @param streamName Stream name.
     * @return A future which will throw if the operation fails, otherwise returning a boolean to
     *         indicate that the stream was removed because it existed.
     */
    CompletableFuture<Boolean> deleteStream(final String scope, final String streamName);

    /**
     * API to request start of scale operation on controller. This method returns a future that will complete when
     * controller service accepts the scale request.
     *
     * @param stream Stream object.
     * @param sealedSegments List of segments to be sealed.
     * @param newKeyRanges Key ranges after scaling the stream.
     * @return A future which will throw if the operation fails, otherwise returning a boolean to
     *         indicate that the scaling was started or not.
     */
    CompletableFuture<Boolean> startScale(final Stream stream, final List<Long> sealedSegments,
                                           final Map<Double, Double> newKeyRanges);

    /**
     * API to merge or split stream segments. This call returns a future that completes when either the scale
     * operation is completed on controller service (succeeded or failed) or the specified timeout elapses.
     * 
     * @param stream Stream object.
     * @param sealedSegments List of segments to be sealed.
     * @param newKeyRanges Key ranges after scaling the stream.
     * @param executorService executor to be used for busy waiting.
     * @return A Cancellable request object which can be used to get the future for scale operation or cancel the scale operation.
     */
    CancellableRequest<Boolean> scaleStream(final Stream stream, final List<Long> sealedSegments,
                                           final Map<Double, Double> newKeyRanges,
                                           final ScheduledExecutorService executorService);

    /**
     * API to check the status of scale for a given epoch.
     *
     * @param stream Stream object.
     * @param scaleEpoch stream's epoch for which the scale was started.
     * @return True if scale completed, false otherwise.
     */
    CompletableFuture<Boolean> checkScaleStatus(final Stream stream, int scaleEpoch);


    // Controller Apis called by pravega producers for getting stream specific information

    /**
     * API to get list of current segments for the stream to write to.
     * 
     * @param scope Scope
     * @param streamName Stream name
     * @return Current stream segments.
     */
    CompletableFuture<StreamSegments> getCurrentSegments(final String scope, final String streamName);

    /**
     * API to create a new transaction. The transaction timeout is relative to the creation time.
     * 
     * @param stream           Stream name
     * @param lease            Time for which transaction shall remain open with sending any heartbeat.
     * @return                 Transaction id.
     */
    CompletableFuture<TxnSegments> createTransaction(final Stream stream, final long lease);

    /**
     * API to send transaction heartbeat and increase the transaction timeout by lease amount of milliseconds.
     *
     * @param stream     Stream name
     * @param txId       Transaction id
     * @param lease      Time for which transaction shall remain open with sending any heartbeat.
     * @return           Transaction.PingStatus or PingFailedException
     */
    CompletableFuture<Transaction.PingStatus> pingTransaction(final Stream stream, final UUID txId, final long lease);

    /**
     * Commits a transaction, atomically committing all events to the stream, subject to the
     * ordering guarantees specified in {@link EventStreamWriter}. Will fail with
     * {@link TxnFailedException} if the transaction has already been committed or aborted.
     * 
     * @param stream Stream name
     * @param writerId The writer that is comiting the transaction.
     * @param timestamp The timestamp the writer provided for the commit (or null if they did not specify one).
     * @param txId Transaction id
     * @return Void or TxnFailedException
     */
    CompletableFuture<Void> commitTransaction(final Stream stream, final String writerId, final Long timestamp, final UUID txId);

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
     * Given a timestamp and a stream returns segments and offsets that were present at that time in the stream.
     *
     * @param stream Name of the stream
     * @param timestamp Timestamp for getting segments
     * @return A map of segments to the offset within them.
     */
    CompletableFuture<Map<Segment, Long>> getSegmentsAtTime(final Stream stream, final long timestamp);

    /**
     * Returns StreamSegmentsWithPredecessors containing each of the segments that are successors to the segment
     * requested mapped to a list of their predecessors.
     * 
     * In the event of a scale up the newly created segments contain a subset of the keyspace of the original
     * segment and their only predecessor is the segment that was split. Example: If there are two segments A
     * and B. A scaling event split A into two new segments C and D. The successors of A are C and D. So
     * calling this method with A would return {C &rarr; A, D &rarr; A}
     * 
     * In the event of a scale down there would be one segment the succeeds multiple. So it would contain the
     * union of the keyspace of its predecessors. So calling with that segment would map to multiple segments.
     * Example: If there are two segments A and B. A and B are merged into a segment C. The successor of A is
     * C. so calling this method with A would return {C &rarr; {A, B}}
     * 
     * If a segment has not been sealed, it may not have successors now even though it might in the future.
     * The successors to a sealed segment are always known and returned. Example: If there is only one segment
     * A and it is not sealed, and no scaling events have occurred calling this with a would return an empty
     * map.
     * 
     * @param segment The segment whose successors should be looked up.
     * @return A mapping from Successor to the list of all of the Successor's predecessors
     */
    CompletableFuture<StreamSegmentsWithPredecessors> getSuccessors(final Segment segment);
    
    /**
     * Returns all the segments that come after the provided cutpoint. 
     * 
     * @param from The position from which to find the remaining bytes.
     * @return The segments beyond a given cut position.
     */
    CompletableFuture<StreamSegmentSuccessors> getSuccessors(StreamCut from);

    /**
     * Returns all the segments from the fromStreamCut till toStreamCut.
     *
     * @param fromStreamCut From stream cut.
     * @param toStreamCut To stream cut.
     * @return list of segments.
     */
    CompletableFuture<StreamSegmentSuccessors> getSegments(StreamCut fromStreamCut, StreamCut toStreamCut);

    // Controller Apis that are called by writers and readers

    /**
     * Checks to see if a segment exists and is not sealed.
     * 
     * @param segment The segment to verify.
     * @return true if the segment exists and is open or false if it is not.
     */
    CompletableFuture<Boolean> isSegmentOpen(final Segment segment);
    
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
    
    /**
     * Notifies that the specified writer has noted the provided timestamp when it was at
     * lastWrittenPosition.
     * 
     * This is called by writers via {@link EventStreamWriter#noteTime(long)} or
     * {@link Transaction#commit(long)}. The controller should aggrigate this information and write
     * it to the stream's marks segment so that it read by readers who will in turn ultimately
     * surface this information through the {@link EventStreamReader#getCurrentTimeWindow(Stream)} API.
     * 
     * @param writer The name of the writer. (User defined)
     * @param stream The stream the timestamp is associated with.
     * @param timestamp The new timestamp for the writer on the stream.
     * @param lastWrittenPosition The position the writer was at when it noted the time.
     */
    CompletableFuture<Void> noteTimestampFromWriter(String writer, Stream stream, long timestamp, WriterPosition lastWrittenPosition);

    /**
     * Notifies the controller that the specified writer is shutting down gracefully and no longer
     * needs to be considered for calculating entries for the marks segment. This may not be called
     * in the event that writer crashes. 
     * 
     * @param writerId The name of the writer. (User defined)
     * @param stream The stream the writer was on.
     */
    CompletableFuture<Void> removeWriter(String writerId, Stream stream);

    /**
     * Closes controller client.
     * @see java.lang.AutoCloseable#close()
     */
    @Override
    void close();

    /**
     * Refreshes an expired/non-existent delegation token.
     * @param scope         Scope of the stream.
     * @param streamName    Name of the stream.
     * @return              The delegation token for the given stream.
     */
    CompletableFuture<String> getOrRefreshDelegationTokenFor(String scope, String streamName);
}
