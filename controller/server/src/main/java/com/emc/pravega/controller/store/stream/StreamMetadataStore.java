/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
     * @param scope Scope name
     * @return boolean whether the scope was created
     */
    CompletableFuture<Boolean> createScope(final String scope);

    /**
     * Deletes a Scope if contains no streams.
     *
     * @param scope Name of scope to be deleted
     * @return boolean whether the scope was deleted
     */
    CompletableFuture<Boolean> deleteScope(final String scope);

    /**
     * List existing streams in scopes.
     *
     * @param scope Name of the scope
     * @return List of streams in scope
     */
    CompletableFuture<List<Stream>> listStreamsInScope(final String scope);

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
    public CompletableFuture<Map<Integer, List<Integer>>> getSuccessors(final String scopeName,
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
     * @param scopeName  scope
     * @param streamName stream
     * @return new Transaction Id
     */
    CompletableFuture<UUID> createTransaction(final String scopeName, final String streamName);

    /**
     * Get transaction status from the stream store.
     *
     * @param scopeName  scope
     * @param streamName stream
     * @param txId       transaction id
     * @return
     */
    CompletableFuture<TxnStatus> transactionStatus(final String scopeName, final String streamName, final UUID txId);

    /**
     * Update stream store to mark transaction as committed.
     *
     * @param scopeName  scope
     * @param streamName stream
     * @param txId       transaction id
     * @return
     */
    CompletableFuture<TxnStatus> commitTransaction(final String scopeName, final String streamName, final UUID txId);

    /**
     * Update stream store to mark transaction as sealed.
     *
     * @param scopeName  scope
     * @param streamName stream
     * @param txId       transaction id
     * @return
     */
    CompletableFuture<TxnStatus> sealTransaction(final String scopeName, final String streamName, final UUID txId);

    /**
     * Update stream store to mark the transaction as aborted.
     *
     * @param scopeName  scope
     * @param streamName stream
     * @param txId       transaction id
     * @return
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
     * @return
     */
    CompletableFuture<List<ActiveTxRecordWithStream>> getAllActiveTx();
}
