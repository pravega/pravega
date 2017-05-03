/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
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

import io.pravega.controller.store.stream.tables.ActiveTxnRecord;
import io.pravega.controller.store.stream.tables.State;
import io.pravega.controller.stream.api.grpc.v1.Controller.CreateScopeStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.DeleteScopeStatus;
import io.pravega.client.stream.StreamConfiguration;

import java.util.AbstractMap.SimpleEntry;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

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
     * @param scope Stream scope.
     * @param name  Stream name.
     * @return Return a streamContext
     */
    OperationContext createContext(final String scope, final String name);

    /**
     * Creates a new stream with the given name and configuration.
     *
     * @param scopeName       scope name
     * @param streamName      stream name
     * @param configuration   stream configuration
     * @param createTimestamp stream creation timestamp
     * @param context         operation context
     * @param executor        callers executor
     * @return boolean indicating whether the stream was created
     */
    CompletableFuture<Boolean> createStream(final String scopeName,
                                            final String streamName,
                                            final StreamConfiguration configuration,
                                            final long createTimestamp,
                                            final OperationContext context,
                                            final Executor executor);

    CompletableFuture<Void> deleteStream(final String scopeName,
                                         final String streamName,
                                         final OperationContext context,
                                         final Executor executor);

    CompletableFuture<Boolean> setState(String scope, String name,
                                        State state, OperationContext context,
                                        Executor executor);

    /**
     * Creates a new scope with the given name.
     *
     * @param scopeName Scope name
     * @return null on success and exception on failure.
     */
    CompletableFuture<CreateScopeStatus> createScope(final String scopeName);

    /**
     * Deletes a Scope if contains no streams.
     *
     * @param scopeName Name of scope to be deleted
     * @return null on success and exception on failure.
     */
    CompletableFuture<DeleteScopeStatus> deleteScope(final String scopeName);

    /**
     * Retrieve configuration of scope.
     *
     * @param scopeName Name of scope.
     * @return Returns configuration of scope.
     */
    CompletableFuture<String> getScopeConfiguration(final String scopeName);

    /**
     * List existing streams in scopes.
     *
     * @param scopeName Name of the scope
     * @return List of streams in scope
     */
    CompletableFuture<List<StreamConfiguration>> listStreamsInScope(final String scopeName);

    /**
     * List Scopes in cluster.
     *
     * @return List of scopes
     */
    CompletableFuture<List<String>> listScopes();

    /**
     * Updates the configuration of an existing stream.
     *
     * @param scope         stream scope
     * @param name          stream name.
     * @param configuration new stream configuration.
     * @param context       operation context
     * @param executor      callers executor
     * @return boolean indicating whether the stream was updated
     */
    CompletableFuture<Boolean> updateConfiguration(final String scope, final String name, final StreamConfiguration configuration,
                                                   final OperationContext context,
                                                   final Executor executor);

    /**
     * Fetches the current stream configuration.
     *
     * @param scope    stream scope
     * @param name     stream name.
     * @param context  operation context
     * @param executor callers executor
     * @return current stream configuration.
     */
    CompletableFuture<StreamConfiguration> getConfiguration(final String scope, final String name,
                                                            final OperationContext context,
                                                            final Executor executor);

    /**
     * Set the stream state to sealed.
     *
     * @param scope    stream scope
     * @param name     stream name.
     * @param context  operation context
     * @param executor callers executor
     * @return boolean indicating whether the stream was updated.
     */
    CompletableFuture<Boolean> setSealed(final String scope, final String name, final OperationContext context, final Executor executor);

    /**
     * Get the stream sealed status.
     *
     * @param scope    stream scope
     * @param name     stream name.
     * @param context  operation context
     * @param executor callers executor
     * @return boolean indicating whether the stream is sealed.
     */
    CompletableFuture<Boolean> isSealed(final String scope, final String name, final OperationContext context, final Executor executor);

    /**
     * Get Segment.
     *
     * @param scope    stream scope
     * @param name     stream name.
     * @param number   segment number.
     * @param context  operation context
     * @param executor callers executor
     * @return segment at given number.
     */
    CompletableFuture<Segment> getSegment(final String scope, final String name, final int number, final OperationContext context, final Executor executor);

    /**
     * Returns the total number of segments in the stream.
     *
     * @param scope    stream scope
     * @param name     stream name.
     * @param context  operation context
     * @param executor callers executor
     * @return total number of segments in the stream.
     */
    CompletableFuture<Integer> getSegmentCount(final String scope, final String name, final OperationContext context, final Executor executor);

    /**
     * Get active segments.
     *
     * @param scope    stream scope
     * @param name     stream name.
     * @param executor callers executor
     * @param context  operation context
     * @return currently active segments
     */
    CompletableFuture<List<Segment>> getActiveSegments(final String scope, final String name, final OperationContext context, final Executor executor);

    /**
     * Get active segments at given timestamp.
     *
     * @param scope     stream scope
     * @param name      stream name.
     * @param timestamp point in time.
     * @param context   operation context
     * @param executor  callers executor
     * @return the list of segments numbers active at timestamp.
     */
    CompletableFuture<List<Integer>> getActiveSegments(final String scope, final String name, final long timestamp, final OperationContext context, final Executor executor);

    /**
     * Given a segment return a map containing the numbers of the segments immediately succeeding it
     * mapped to a list of the segments they succeed.
     *
     * @param scope         stream scope
     * @param streamName    stream name.
     * @param segmentNumber the segment number
     * @param context       operation context
     * @param executor      callers executor
     * @return segments that immediately follow the specified segment and the segments they follow.
     */
    CompletableFuture<Map<Integer, List<Integer>>> getSuccessors(final String scope, final String streamName,
                                                                 final int segmentNumber, final OperationContext context, final Executor executor);

    /**
     * Scales in or out the currently set of active segments of a stream.
     *
     * @param scope          stream scope
     * @param name           stream name.
     * @param newRanges      new key ranges to be added to the stream which maps to a new segment per range in the stream
     * @param sealedSegments segments to be sealed
     * @param scaleTimestamp timestamp at which scale was requested
     * @param context        operation context
     * @param executor       callers executor
     * @return the list of newly created segments
     */
    CompletableFuture<List<Segment>> startScale(final String scope, final String name,
                                                final List<Integer> sealedSegments,
                                                final List<SimpleEntry<Double, Double>> newRanges,
                                                final long scaleTimestamp,
                                                final OperationContext context,
                                                final Executor executor);

    /**
     * Called after new segments are created in pravega.
     *
     * @param scope          stream scope
     * @param name           stream name.
     * @param sealedSegments segments to be sealed
     * @param newSegments    segments that were created as part of startScale
     * @param scaleTimestamp timestamp at which scale was requested
     * @param context        operation context
     * @param executor       callers executor
     * @return future
     */
    CompletableFuture<Void> scaleNewSegmentsCreated(final String scope, final String name,
                                                    final List<Integer> sealedSegments,
                                                    final List<Segment> newSegments,
                                                    final long scaleTimestamp,
                                                    final OperationContext context,
                                                    final Executor executor);

    /**
     * Called after old segments are sealed in pravega.
     *
     * @param scope          stream scope
     * @param name           stream name.
     * @param sealedSegments segments to be sealed
     * @param newSegments    segments that were created as part of startScale
     * @param scaleTimestamp timestamp at which scale was requested
     * @param context        operation context
     * @param executor       callers executor
     * @return future
     */
    CompletableFuture<Void> scaleSegmentsSealed(final String scope, final String name,
                                                final List<Integer> sealedSegments,
                                                final List<Segment> newSegments,
                                                final long scaleTimestamp,
                                                final OperationContext context,
                                                final Executor executor);

    /**
     * Method to create a new transaction on a stream.
     *
     * @param scopeName        Scope
     * @param streamName       Stream
     * @param lease            Time for which transaction shall remain open with sending any heartbeat.
     * @param maxExecutionTime Maximum time for which client may extend txn lease.
     * @param scaleGracePeriod Maximum time for which client may extend txn lease once
     *                         the scaling operation is initiated on the txn stream.
     * @param context          operation context
     * @param executor         callers executor
     * @return Transaction data along with version information.
     */
    CompletableFuture<VersionedTransactionData> createTransaction(final String scopeName, final String streamName,
                                                                  final long lease, final long maxExecutionTime,
                                                                  final long scaleGracePeriod,
                                                                  final OperationContext context,
                                                                  final Executor executor);

    /**
     * Heartbeat to keep the transaction open for at least lease amount of time.
     *
     * @param scopeName  Scope
     * @param streamName Stream
     * @param txId       Transaction identifier
     * @param lease      Lease duration in ms
     * @param context    operation context
     * @param executor   callers executor
     * @return Transaction data along with version information.
     */
    CompletableFuture<VersionedTransactionData> pingTransaction(final String scopeName, final String streamName,
                                                                final UUID txId, final long lease,
                                                                final OperationContext context, final Executor executor);

    /**
     * Fetch transaction metadata along with its version.
     *
     * @param scopeName  scope
     * @param streamName stream
     * @param txId       transaction id
     * @param context    operation context
     * @param executor   callers executor
     * @return transaction metadata along with its version.
     */
    CompletableFuture<VersionedTransactionData> getTransactionData(String scopeName, String streamName, UUID txId,
                                                                   final OperationContext context,
                                                                   final Executor executor);

    /**
     * Get transaction status from the stream store.
     *
     * @param scope    stream scope
     * @param stream   stream
     * @param txId     transaction id
     * @param context  operation context
     * @param executor callers executor
     * @return transaction status.
     */
    CompletableFuture<TxnStatus> transactionStatus(final String scope, final String stream, final UUID txId, final OperationContext context, final Executor executor);

    /**
     * Update stream store to mark transaction as committed.
     *
     * @param scope    scope
     * @param stream   stream
     * @param txId     transaction id
     * @param context  operation context
     * @param executor callers executor
     * @return transaction status.
     */
    CompletableFuture<TxnStatus> commitTransaction(final String scope, final String stream, final UUID txId, final OperationContext context, final Executor executor);

    /**
     * Update stream store to mark transaction as sealed.
     *
     * @param scope    scope
     * @param stream   stream
     * @param txId     transaction id
     * @param commit   Boolean indicating whether to change txn state to committing or aborting.
     * @param version  Expected version of the transaction record in the store.
     * @param context  operation context
     * @param executor callers executor
     * @return Transaction status.
     */
    CompletableFuture<TxnStatus> sealTransaction(final String scope, final String stream, final UUID txId,
                                                 final boolean commit, final Optional<Integer> version,
                                                 final OperationContext context, final Executor executor);

    /**
     * Update stream store to mark the transaction as aborted.
     *
     * @param scope    scope
     * @param stream   stream
     * @param txId     transaction id
     * @param context  operation context
     * @param executor callers executor
     * @return transaction status
     */
    CompletableFuture<TxnStatus> abortTransaction(final String scope, final String stream, final UUID txId, final OperationContext context, final Executor executor);

    /**
     * Returns a boolean indicating whether any transaction is active on the specified stream.
     *
     * @param scope    scope.
     * @param stream   stream.
     * @param context  operation context
     * @param executor callers executor
     * @return boolean indicating whether any transaction is active on the specified stream.
     */
    CompletableFuture<Boolean> isTransactionOngoing(final String scope, final String stream, final OperationContext context, final Executor executor);

    /**
     * Method to retrive all currently active transactions from the metadata store.
     *
     * @param scope    scope of stream
     * @param stream   name of stream
     * @param context  operation context
     * @param executor callers executor
     * @return map of txId to TxRecord
     */
    CompletableFuture<Map<UUID, ActiveTxnRecord>> getActiveTxns(final String scope, final String stream, final OperationContext context, final Executor executor);

    /**
     * Api to mark a segment as cold.
     *
     * @param scope         scope for stream
     * @param stream        name of stream
     * @param segmentNumber segment number
     * @param timestamp     time till which this cold marker is valid.
     * @param context       context in which this operation is taking place.
     * @param executor      callers executor
     * @return Completable future
     */
    CompletableFuture<Void> markCold(final String scope, final String stream, final int segmentNumber, final long timestamp, final OperationContext context, final Executor executor);

    /**
     * Api to return if a cold marker is set.
     *
     * @param scope    scope for stream
     * @param stream   name of stream
     * @param number   segment nunmber
     * @param context  context in which this operation is taking place.
     * @param executor callers executor
     * @return Completable future Optional of marker's creation time.
     */
    CompletableFuture<Boolean> isCold(final String scope, final String stream, final int number, final OperationContext context, final Executor executor);

    /**
     * Api to clear marker.
     *
     * @param scope    scope for stream
     * @param stream   name of stream
     * @param number   segment nunmber
     * @param context  context in which this operation is taking place.
     * @param executor callers executor
     * @return Completable Future
     */
    CompletableFuture<Void> removeMarker(final String scope, final String stream, final int number, final OperationContext context, final Executor executor);
}
