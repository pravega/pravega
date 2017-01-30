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

import java.nio.ByteBuffer;
import java.util.AbstractMap.SimpleEntry;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

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
     * Get next segments.
     *
     * @param name              stream name.
     * @param completedSegments completely read segments.
     * @param currentSegments   current consumer positions.
     * @return new consumer positions including new (current or future) segments that can be read from.
     */
    CompletableFuture<List<SegmentFutures>> getNextSegments(final String name,
                                                            final Set<Integer> completedSegments,
                                                            final List<SegmentFutures> currentSegments);

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
    CompletableFuture<TxnStatus> dropTransaction(final String scope, final String stream, final UUID txId);

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

    CompletableFuture<Void> setMarker(String scope, String stream, int segmentNumber, long timestamp);

    CompletableFuture<Optional<Long>> getMarker(String scope, String stream, int number);

    CompletableFuture<Void> removeMarker(String scope, String stream, int number);

    CompletableFuture<Void> checkpoint(String readerId, String readerGroup, ByteBuffer serialize);

    CompletableFuture<Optional<ByteBuffer>> readCheckpoint(String readerId, String readerGroup);
}
