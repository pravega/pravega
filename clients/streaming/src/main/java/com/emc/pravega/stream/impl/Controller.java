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

package com.emc.pravega.stream.impl;

import com.emc.pravega.common.netty.PravegaNodeUri;
import com.emc.pravega.controller.stream.api.v1.CreateStreamStatus;
import com.emc.pravega.controller.stream.api.v1.ScaleResponse;
import com.emc.pravega.controller.stream.api.v1.TransactionStatus;
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
     * @param streamConfig stream configuration
     * @return status of create stream operation.
     */
    CompletableFuture<CreateStreamStatus> createStream(final StreamConfiguration streamConfig);

    /**
     * Api to alter stream.
     *
     * @param streamConfig stream configuration to updated
     * @return status of update stream operation.
     */
    CompletableFuture<UpdateStreamStatus> alterStream(final StreamConfiguration streamConfig);

    /**
     * Api to seal stream.
     * @param scope scope
     * @param streamName stream name
     * @return status of update stream operation.
     */
    CompletableFuture<UpdateStreamStatus> sealStream(final String scope, final String streamName);

    /**
     * API to merge or split stream segments.
     * @param stream stream object.
     * @param sealedSegments list of segments to be sealed.
     * @param newKeyRanges key ranges after scaling the stream.
     * @return status of scale operation.
     */
    CompletableFuture<ScaleResponse> scaleStream(final Stream stream,
                                                 final List<Integer> sealedSegments,
                                                 final Map<Double, Double> newKeyRanges);

    // Controller Apis called by pravega producers for getting stream specific information

    /**
     * Api to get list of current segments for the stream to write to.
     * @param scope scope
     * @param streamName stream name
     * @return current stream segments.
     */
    CompletableFuture<StreamSegments> getCurrentSegments(final String scope, final String streamName);

    /**
     * Api to create a new transaction.
     * The transaction timeout is relative to the creation time.
     * @param stream stream name
     * @param timeout tx timeout
     * @return transaction identifier.
     */
    CompletableFuture<UUID> createTransaction(final Stream stream, final long timeout);

    /**
     * Commits a transaction, atomically committing all events to the stream, subject to the
     * ordering guarantees specified in {@link EventStreamWriter}. Will fail with {@link TxnFailedException}
     * if the transaction has already been committed or aborted.
     * 
     * @param stream stream name
     * @param txId transaction id
     * @return status of commit transaction operation.
     */
    CompletableFuture<TransactionStatus> commitTransaction(final Stream stream, final UUID txId);

    /**
     * Drops a transaction. No events written to it may be read, and no further events may be written.
     * @param stream stream name
     * @param txId transaction id
     * @return status of drop transaction operation.
     */
    CompletableFuture<TransactionStatus> dropTransaction(final Stream stream, final UUID txId);

    /**
     * Returns the status of the specified transaction.
     * @param stream stream name
     * @param txId transaction id
     * @return transaction status.
     */
    CompletableFuture<Transaction.Status> checkTransactionStatus(final Stream stream, final UUID txId);

    // Controller Apis that are called by readers

    /**
     * Returns list of position objects by distributing available segments at the
     * given timestamp into requested number of position objects.
     *
     * @param stream name
     * @param timestamp timestamp for getting position objects
     * @param count number of position objects
     * @return list of position objects.
     */
    CompletableFuture<List<PositionInternal>> getPositions(final Stream stream, final long timestamp, final int count);

    /**
     * Called by readers upon reaching end of segment on some segment in its position obejct.
     *
     * @param stream stream name
     * @param positions current position objects
     * @return list of updated position objects.
     */
    CompletableFuture<List<PositionInternal>> updatePositions(final Stream stream, final List<PositionInternal> positions);

    //Controller Apis that are called by writers and readers

    /**
     * Given a segment return the endpoint that currently is the owner of that segment.
     * <p>
     * The result of this function can be cached until the endpoint is unreachable or indicates it
     * is no longer the owner.
     *
     * @param qualifiedSegmentName The name of the segment. Usually obtained from {@link Segment#getScopedName()}.
     */
    CompletableFuture<PravegaNodeUri> getEndpointForSegment(final String qualifiedSegmentName);

    // Controller Apis that are called by Pravega host

    /**
     * Given a segment number, check if the segment is created and not sealed.
     *
     * @param scope scope
     * @param stream stream
     * @param segmentNumber segment number
     * @return boolean indicating validity of segment.
     */
    CompletableFuture<Boolean> isSegmentValid(final String scope,
                                              final String stream,
                                              final int segmentNumber);
}
