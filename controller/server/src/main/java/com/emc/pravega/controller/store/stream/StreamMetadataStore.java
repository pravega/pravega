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

import com.emc.pravega.controller.store.stream.tables.ActiveTxRecord;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.impl.TxnStatus;

import java.nio.ByteBuffer;
import java.util.AbstractMap.SimpleEntry;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * Stream Metadata.
 */
public interface StreamMetadataStore {

    /**
     * Method to create an operation context. A context ensures that multiple calls to store for the same data are avoided
     * within the same operation. All api signatures are changed to accept context. If context is supplied, the data will be
     * looked up within the context and, upon a cache miss, will be fetched from the external store and cached within the context.
     * Once an operation completes, the context is discarded.
     *
     * @param scope
     * @param name
     * @return
     */
    StreamContext createContext(final String scope, final String name);

    /**
     * Creates a new stream with the given name and configuration.
     *
     * @param name            stream name.
     * @param configuration   stream configuration.
     * @param createTimestamp stream creation timestamp.
     * @return boolean indicating whether the stream was created
     */
    CompletableFuture<Boolean> createStream(final String scope, final String name,
                                            final StreamConfiguration configuration,
                                            final long createTimestamp, final StreamContext context);

    /**
     * Updates the configuration of an existing stream.
     *
     * @param name          stream name.
     * @param configuration new stream configuration
     * @return boolean indicating whether the stream was updated
     */
    CompletableFuture<Boolean> updateConfiguration(final String scope, final String name, final StreamConfiguration configuration, final StreamContext context);

    /**
     * Fetches the current stream configuration.
     *
     * @param name stream name.
     * @return current stream configuration.
     */
    CompletableFuture<StreamConfiguration> getConfiguration(final String scope, final String name, final StreamContext context);

    /**
     * Set the stream state to sealed.
     *
     * @param name stream name.
     * @return boolean indicating whether the stream was updated.
     */
    CompletableFuture<Boolean> setSealed(final String scope, final String name, final StreamContext context);

    /**
     * Get the stream sealed status.
     *
     * @param name stream name.
     * @return boolean indicating whether the stream is sealed.
     */
    CompletableFuture<Boolean> isSealed(final String scope, final String name, final StreamContext context);

    /**
     * Get Segment.
     *
     * @param name   stream name.
     * @param number segment number.
     * @return segment at given number.
     */
    CompletableFuture<Segment> getSegment(final String scope, final String name, final int number, final StreamContext context);

    /**
     * Get active segments.
     *
     * @param name stream name.
     * @return currently active segments
     */
    CompletableFuture<List<Segment>> getActiveSegments(final String scope, final String name, final StreamContext context);

    /**
     * Get active segments at given timestamp.
     *
     * @param name      stream name.
     * @param timestamp point in time.
     * @return the list of segments active at timestamp.
     */
    CompletableFuture<SegmentFutures> getActiveSegments(final String scope, final String name, final long timestamp, final StreamContext context);

    /**
     * Get next segments.
     *
     * @param name              stream name.
     * @param completedSegments completely read segments.
     * @param currentSegments   current consumer positions.
     * @return new consumer positions including new (current or future) segments that can be read from.
     */
    CompletableFuture<List<SegmentFutures>> getNextSegments(final String scope, final String name,
                                                            final Set<Integer> completedSegments,
                                                            final List<SegmentFutures> currentSegments, final StreamContext context);

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
    CompletableFuture<List<Segment>> scale(final String scope, final String name,
                                           final List<Integer> sealedSegments,
                                           final List<SimpleEntry<Double, Double>> newRanges,
                                           final long scaleTimestamp, final StreamContext context);

    /**
     * Method to create a new transaction on a stream.
     *
     * @param scope  scope
     * @param stream stream
     * @return new Transaction Id
     */
    CompletableFuture<UUID> createTransaction(final String scope, final String stream, final StreamContext context);

    /**
     * Get transaction status from the stream store.
     *
     * @param scope  scope
     * @param stream stream
     * @param txId   transaction id
     * @return
     */
    CompletableFuture<TxnStatus> transactionStatus(final String scope, final String stream, final UUID txId, final StreamContext context);

    /**
     * Update stream store to mark transaction as committed.
     *
     * @param scope  scope
     * @param stream stream
     * @param txId   transaction id
     * @return
     */
    CompletableFuture<TxnStatus> commitTransaction(final String scope, final String stream, final UUID txId, final StreamContext context);

    /**
     * Update stream store to mark transaction as sealed.
     *
     * @param scope  scope
     * @param stream stream
     * @param txId   transaction id
     * @return
     */
    CompletableFuture<TxnStatus> sealTransaction(final String scope, final String stream, final UUID txId, final StreamContext context);

    /**
     * Update stream store to mark the transaction as aborted.
     *
     * @param scope  scope
     * @param stream stream
     * @param txId   transaction id
     * @return
     */
    CompletableFuture<TxnStatus> dropTransaction(final String scope, final String stream, final UUID txId, final StreamContext context);

    /**
     * Returns a boolean indicating whether any transaction is active on the specified stream.
     *
     * @param scope  scope.
     * @param stream stream.
     * @return boolean indicating whether any transaction is active on the specified stream.
     */
    CompletableFuture<Boolean> isTransactionOngoing(final String scope, final String stream, final StreamContext context);

    /**
     * Api to block new transactions from being created for the specified duration.
     * This is useful in scenarios where a scale operation may end up waiting indefinitely because there are always one or more on-going transactions.
     *
     * @param scope   scope.
     * @param stream  stream.
     * @param context context in which this operation is taking place.
     * @return Completable Future
     */
    CompletableFuture<Void> blockTransactions(final String scope, final String stream, final StreamContext context);

    CompletableFuture<Void> unblockTransactions(String scope, String stream, StreamContext context);

    /**
     * Api to mark a segment as cold.
     *
     * @param scope         scope for stream
     * @param stream        name of stream
     * @param segmentNumber segment number
     * @param timestamp     time at which request for marking a segment cold was generated.
     * @param context       context in which this operation is taking place.
     * @return Completable future
     */
    CompletableFuture<Void> setMarker(final String scope, final String stream, final int segmentNumber, final long timestamp, final StreamContext context);

    /**
     * Api to return if a cold marker is set.
     *
     * @param scope   scope for stream
     * @param stream  name of stream
     * @param number  segment nunmber
     * @param context context in which this operation is taking place.
     * @return Completable future Optional of marker's creation time.
     */
    CompletableFuture<Optional<Long>> getMarker(final String scope, final String stream, final int number, final StreamContext context);

    /**
     * Api to clear marker.
     *
     * @param scope   scope for stream
     * @param stream  name of stream
     * @param number  segment nunmber
     * @param context context in which this operation is taking place.
     * @return Completable Future
     */
    CompletableFuture<Void> removeMarker(final String scope, final String stream, final int number, final StreamContext context);

    /**
     * Api to store checkpoint for controller in the persistent store.
     *
     * @param id        id to uniquely identify the controller instance
     * @param group     group to which controller instance belongs to
     * @param serialize Serialized checkpoint data
     * @return Completable Future
     */
    CompletableFuture<Void> checkpoint(final String id, final String group, final ByteBuffer serialize);

    /**
     * Api to retrieve checkpointed data.
     *
     * @param id    id to uniquely identify the controller instance
     * @param group group to which controller instance belongs to
     * @return Completable future of serialized checkpointed data
     */
    CompletableFuture<Optional<ByteBuffer>> readCheckpoint(final String id, final String group);

    CompletableFuture<Map<UUID, ActiveTxRecord>> getActiveTxns(String scope, String stream, StreamContext context);
}
