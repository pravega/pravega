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
interface Stream extends OperationContext {

    String getScope();

    String getName();

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


    CompletableFuture<Void> setMarker(int segmentNumber, long timestamp);

    CompletableFuture<Optional<Long>> getMarker(int segmentNumber);

    CompletableFuture<Void> removeMarker(int segmentNumber);

    CompletableFuture<Void> blockTransactions();

    CompletableFuture<Void> unblockTransactions();

    /**
     * Method to start new transaction creation
     *
     * @return
     */
    CompletableFuture<UUID> createTransaction();

    /**
     * Seal given transaction
     *
     * @param txId
     * @return
     */
    CompletableFuture<TxnStatus> sealTransaction(final UUID txId);

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
     */
    CompletableFuture<Boolean> isTransactionOngoing();

    CompletableFuture<Map<UUID, ActiveTxRecord>> getActiveTxns();

    /**
     * Refresh the stream object. Typically to be used to invalidate any caches.
     * This allows us reuse of stream object without having to recreate a new stream object for each new operation
     */
    void refresh();
}
