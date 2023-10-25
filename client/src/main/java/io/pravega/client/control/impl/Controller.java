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
package io.pravega.client.control.impl;

import io.pravega.client.admin.KeyValueTableInfo;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.StreamCut;
import io.pravega.client.stream.Transaction;
import io.pravega.client.stream.TransactionInfo;
import io.pravega.client.stream.TxnFailedException;
import io.pravega.client.stream.impl.StreamSegmentSuccessors;
import io.pravega.client.stream.impl.StreamSegments;
import io.pravega.client.stream.impl.StreamSegmentsWithPredecessors;
import io.pravega.client.stream.impl.TxnSegments;
import io.pravega.client.stream.impl.WriterPosition;
import io.pravega.client.tables.KeyValueTableConfiguration;
import io.pravega.client.tables.impl.KeyValueTableSegments;
import io.pravega.common.util.AsyncIterator;
import io.pravega.shared.protocol.netty.BucketType;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.shared.security.auth.AccessOperation;

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
     * Check if scope exists. 
     * 
     * @param scopeName name of scope. 
     * @return CompletableFuture which when completed will indicate if scope exists or not. 
     */
    CompletableFuture<Boolean> checkScopeExists(final String scopeName);
    
    /**
     * Gets an async iterator on scopes.
     *
     * @return An AsyncIterator which can be used to iterate over all scopes. 
     */
    AsyncIterator<String> listScopes();

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
     * Gets an async iterator on streams in scope.
     *
     * @param scopeName The name of the scope for which to list streams in.
     * @param tag The Stream tag.
     * @return An AsyncIterator which can be used to iterate over all Streams in the scope.
     */
    AsyncIterator<Stream> listStreamsForTag(final String scopeName, final String tag);

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
     * API to delete a scope recursively. This method once invoked will cause failure of
     * create operation on Stream/RG/KVT within the scope if the scope is in sealed state
     *
     * @param scopeName Scope name.
     * @return A future which will throw if the operation fails, otherwise returning a boolean to
     *         indicate that the scope was removed because it existed.
     */
    CompletableFuture<Boolean> deleteScopeRecursive(final String scopeName);

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
     * Check if stream exists. 
     *
     * @param scopeName name of scope. 
     * @param streamName name of stream. 
     * @return CompletableFuture which when completed will indicate if stream exists or not. 
     */
    CompletableFuture<Boolean> checkStreamExists(final String scopeName, final String streamName);

    /**
     * Fetch the current Stream Configuration. This includes the {@link io.pravega.client.stream.ScalingPolicy},
     * {@link io.pravega.client.stream.RetentionPolicy} and tags for the given stream.
     *
     * @param scopeName name of scope.
     * @param streamName name of stream.
     * @return CompletableFuture which returns the current stream configuration.
     */
    CompletableFuture<StreamConfiguration> getStreamConfiguration(final String scopeName, final String streamName);

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
     * API create a ReaderGroup.
     * @param scopeName Scope name for Reader Group.
     * @param rgName Stream name.
     * @param config ReaderGroup configuration.
     * @throws IllegalArgumentException if Stream does not exist.
     * @return A future which will throw if the operation fails, otherwise returning a boolean to
     *         indicate that the subscriber was updated in Stream Metadata.
     */
    CompletableFuture<ReaderGroupConfig> createReaderGroup(final String scopeName, final String rgName, ReaderGroupConfig config);

    /**
     * API to update a ReaderGroup config.
     * @param scopeName Scope name for Reader Group.
     * @param rgName Stream name.
     * @param config ReaderGroup configuration.
     * @throws IllegalArgumentException if Stream does not exist.
     * @throws ReaderGroupConfigRejectedException if the provided ReaderGroupConfig is invalid
     * @return A future which will throw if the operation fails, otherwise
     *         the subscriber was updated in Stream Metadata and a long indicating
     *         the updated config generation is returned.
     */
    CompletableFuture<Long> updateReaderGroup(final String scopeName, final String rgName, ReaderGroupConfig config);

    /**
     * API to get Reader Group Configuration.
     * @param scope Scope name for Reader Group.
     * @param rgName Stream name.
     * @throws IllegalArgumentException if ReaderGroup does not exist.
     * @return A future which will throw if the operation fails, otherwise returns configuration of the Reader Group.
     */
    CompletableFuture<ReaderGroupConfig> getReaderGroupConfig(final String scope, final String rgName);

    /**
     * API to delete a Reader Group.
     * @param scope Scope name for Reader Group.
     * @param rgName Reader Group name.
     * @param readerGroupId Unique Id for this readerGroup.
     * @return A future which will throw if the operation fails, otherwise returns configuration of the Reader Group.
     */
    CompletableFuture<Boolean> deleteReaderGroup(final String scope, final String rgName, final UUID readerGroupId);

    /**
     * Get list of Subscribers for the Stream.
     * @param scope Scope name
     * @param streamName Stream name
     * @return List of StreamSubscribers.
     */
    CompletableFuture<List<String>> listSubscribers(final String scope, final String streamName);

    /**
     * API to update the truncation StreamCut for a particular Subscriber on Controller.
     * Used when Stream has Consumption Based Retention Policy configured.
     * @param scope Scope name
     * @param streamName Stream name
     * @param subscriber Name/Id that uniquely identifies a Stream Subscriber.
     * @param readerGroupId Reader Group Id.
     * @param generation subscriber generation number.
     * @param streamCut StreamCut at which Stream can be Truncated for a Consumption based retention policy
     * @throws IllegalArgumentException if Stream/Subscriber does not exist, or StreamCut is not valid.
     * @return A future which will throw if the operation fails, otherwise returning a boolean to
     *         indicate that the subscribers position was updated in Stream Metadata.
     */
    CompletableFuture<Boolean> updateSubscriberStreamCut(final String scope, final String streamName, final String subscriber,
                                                         final UUID readerGroupId, final long generation, final StreamCut streamCut);

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
     * API to get list of segments for given epoch.
     *
     * @param scope Scope
     * @param streamName Stream name
     * @param epoch Epoch number.
     * @return Stream segments for a given Epoch.
     */
    CompletableFuture<StreamSegments> getEpochSegments(final String scope, final String streamName, int epoch);

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
     * @param writerId The writer that is committing the transaction.
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

    /**
     * Get list of TransactionInfo for the Stream having status COMMITTED/ABORTED from most recent batch.
     * TransactionInfo contains unique transactionId, status of transaction and stream.
     * This API can return maximum 500 records.
     *
     * @param stream The name of the stream for which to list transactionInfo.
     * @return List of TransactionInfo.
     */
    CompletableFuture<List<TransactionInfo>> listCompletedTransactions(final Stream stream);

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
     * Obtains a delegation token from the server.
     *
     * @param scope Scope of the stream.
     * @param streamName Name of the stream.
     * @param accessOperation The requested permission.
     * @return The delegation token for the given stream.
     */
    CompletableFuture<String> getOrRefreshDelegationTokenFor(String scope, String streamName, AccessOperation accessOperation);

    //region KeyValueTables

    /**
     * API to create a KeyValueTable. The future completes with true in the case the KeyValueTable did not
     * exist when the controller executed the operation. In the case of a re-attempt to create
     * the same KeyValueTable, the future completes with false to indicate that the KeyValueTable existed when
     * the controller executed the operation.
     *
     * @param scope     Scope
     * @param kvtName   KeyValueTable name
     * @param kvtConfig The {@link KeyValueTableConfiguration} to create with.
     * @return A future which will throw if the operation fails, otherwise returning a boolean to
     * indicate that the stream was added because it did not already exist.
     */
    CompletableFuture<Boolean> createKeyValueTable(final String scope, final String kvtName, final KeyValueTableConfiguration kvtConfig);

    /**
     * Gets an {@link AsyncIterator} on KeyValueTables in scope.
     *
     * @param scopeName The name of the scope for which to list KeyValueTables in.
     * @return An {@link AsyncIterator} which can be used to iterate over all KeyValueTables in the scope.
     */
    AsyncIterator<KeyValueTableInfo> listKeyValueTables(final String scopeName);

    /**
     * API to get the {@link KeyValueTableConfiguration}.
     *
     * @param scope   Scope
     * @param kvtName KeyValueTable name
     * @throws IllegalArgumentException if the key-value table does not exist.
     * @return A future which will throw if the operation fails, otherwise returning the KeyValueTableConfiguration of
     * the corresponding KeyValueTable name.
     */
    CompletableFuture<KeyValueTableConfiguration> getKeyValueTableConfiguration(final String scope, final String kvtName);

    /**
     * API to delete a KeyValueTable. Only a sealed KeyValueTable can be deleted.
     *
     * @param scope   Scope name.
     * @param kvtName KeyValueTable name.
     * @return A future which will throw if the operation fails, otherwise returning a boolean to
     * indicate that the KeyValueTable was removed because it existed.
     */
    CompletableFuture<Boolean> deleteKeyValueTable(final String scope, final String kvtName);

    /**
     * API to get list of current segments for the KeyValueTable to write to.
     *
     * @param scope   Scope
     * @param kvtName KeyValueTable name
     * @return Current KeyValueTable segments.
     */
    CompletableFuture<KeyValueTableSegments> getCurrentSegmentsForKeyValueTable(final String scope, final String kvtName);

    /**
     * API to get controller to bucket mapping.
     *
     * @param bucketType   Bucket type.
     * @return A map having controller as key and list of bucket assigned to controller as value.
     */
    CompletableFuture<Map<String, List<Integer>>> getControllerToBucketMapping(final BucketType bucketType);

    /**
     * API to force cache refresh in case data is stale.
     *
     * @param segmentName   name of the segment
     * @param errNodeUri Stale {@link PravegaNodeUri} entry in cache, this would be used in comparison
     * with the existing value to determine if update is needed or not.
     */
    void updateStaleValueInCache(String segmentName, PravegaNodeUri errNodeUri);

    //endregion
}
