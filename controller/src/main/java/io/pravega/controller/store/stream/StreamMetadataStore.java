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
package io.pravega.controller.store.stream;

import com.google.common.collect.ImmutableMap;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.controller.store.Version;
import io.pravega.controller.store.VersionedMetadata;
import io.pravega.controller.store.stream.records.ActiveTxnRecord;
import io.pravega.controller.store.stream.records.CommittingTransactionsRecord;
import io.pravega.controller.store.stream.records.EpochRecord;
import io.pravega.controller.store.stream.records.EpochTransitionRecord;
import io.pravega.controller.store.stream.records.HistoryTimeSeries;
import io.pravega.controller.store.stream.records.ReaderGroupConfigRecord;
import io.pravega.controller.store.stream.records.RetentionSet;
import io.pravega.controller.store.stream.records.SealedSegmentsMapShard;
import io.pravega.controller.store.stream.records.StreamConfigurationRecord;
import io.pravega.controller.store.stream.records.StreamCutRecord;
import io.pravega.controller.store.stream.records.StreamCutReferenceRecord;
import io.pravega.controller.store.stream.records.StreamSegmentRecord;
import io.pravega.controller.store.stream.records.StreamSubscriber;
import io.pravega.controller.store.stream.records.StreamTruncationRecord;
import io.pravega.controller.store.stream.records.WriterMark;
import io.pravega.controller.store.task.TxnResource;
import io.pravega.controller.stream.api.grpc.v1.Controller.CreateScopeStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.DeleteScopeStatus;
import io.pravega.shared.controller.event.ControllerEvent;
import org.apache.commons.lang3.tuple.Pair;

import java.util.AbstractMap.SimpleEntry;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Stream Metadata.
 */
public interface StreamMetadataStore extends AutoCloseable {

    /**
     * Method to create an operation context for an operation intended on a specific scope. An operation context
     * encapsulates a particular operation which is tracked against the specified request id. A context object also
     * ensures that the store requests using the same context object optimizes on the number of store calls that are made
     * by reusing previously retrieved values from the cache.
     *
     * @param scope Stream scope.
     * @param requestId requestId.
     * @return Return a streamContext
     */
    OperationContext createScopeContext(final String scope, long requestId);

    /**
     * Method to create an operation context for an operation intended on a specific stream. An operation context
     * encapsulates a particular operation which is tracked against the specified request id. A context object also
     * ensures that the store requests using the same context object optimizes on the number of store calls that are made
     * by reusing previously retrieved values from the cache.
     *
     * @param scope Stream scope.
     * @param name  Stream name.
     * @param requestId requestId.
     * @return Return a streamContext
     */
    OperationContext createStreamContext(final String scope, final String name, long requestId);

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
    CompletableFuture<CreateStreamResponse> createStream(final String scopeName,
                                            final String streamName,
                                            final StreamConfiguration configuration,
                                            final long createTimestamp,
                                            final OperationContext context,
                                            final Executor executor);

    /**
     * Api to check if a stream exists in the store or not.
     * @param scopeName scope name
     * @param streamName stream name
     * @param context Operation context
     * @param executor executor
     * @return true if stream exists, false otherwise
     */
    CompletableFuture<Boolean> checkStreamExists(final String scopeName,
                                                 final String streamName,
                                                 final OperationContext context,
                                                 final Executor executor);

    /**
     * Api to check if a stream exists in the store or not.
     * @param scopeName scope name
     * @param context operation context
     * @param executor executor
     * @return true if stream exists, false otherwise
     */
    CompletableFuture<Boolean> checkScopeExists(final String scopeName, OperationContext context, Executor executor);

    /**
     * Api to check if a scope exists in the deleting scope table or not.
     * @param scopeName scope name
     * @param context operation context
     * @param executor executor
     * @return true if scope exists, false otherwise
     */
    CompletableFuture<Boolean> isScopeSealed(final String scopeName, OperationContext context, Executor executor);

    /**
     * Api to get creation time for the stream. 
     * 
     * @param scopeName       scope name
     * @param streamName      stream name
     * @param context         operation context
     * @param executor        callers executor
     * @return CompletableFuture, which when completed, will contain the creation time of the stream. 
     */
    CompletableFuture<Long> getCreationTime(final String scopeName,
                                            final String streamName,
                                            final OperationContext context,
                                            final Executor executor);
    
    /**
     * Api to Delete the stream related metadata.
     *
     * @param scopeName       scope name
     * @param streamName      stream name
     * @param context         operation context
     * @param executor        callers executor
     * @return future
     */
    CompletableFuture<Void> deleteStream(final String scopeName,
                                         final String streamName,
                                         final OperationContext context,
                                         final Executor executor);

    /**
     * Api to set the state for stream in metadata.
     * @param scope scope name
     * @param name stream name
     * @param state stream state
     * @param context operation context
     * @param executor callers executor
     * @return Future of boolean if state update succeeded.
     */
    CompletableFuture<Void> setState(final String scope, final String name,
                                     final State state, final OperationContext context,
                                     final Executor executor);

    /**
     * Api to get the state for stream from metadata.
     *
     * @param scope scope name
     * @param name stream name
     * @param ignoreCached ignore cached value and fetch from store.
     * @param context operation context
     * @param executor callers executor
     * @return Future of boolean if state update succeeded.
     */
    CompletableFuture<State> getState(final String scope, final String name, final boolean ignoreCached,
                                      final OperationContext context, final Executor executor);

    /**
     * Api to get the current state with its current version.
     *
     * @param scope scope
     * @param name stream
     * @param context operation context
     * @param executor executor
     * @return Future which when completed has the versioned state.
     */
    CompletableFuture<VersionedMetadata<State>> getVersionedState(final String scope, final String name,
                                                                  final OperationContext context, final Executor executor);

    /**
     * Api to update versioned state as a CAS operation.
     *
     * @param scope scope
     * @param name stream
     * @param state desired state
     * @param previous current state with version
     * @param context operation context
     * @param executor executor
     * @return Future which when completed contains the updated state and version if successful or exception otherwise.
     */
    CompletableFuture<VersionedMetadata<State>> updateVersionedState(final String scope, final String name,
                                                                     final State state,
                                                                     final VersionedMetadata<State> previous,
                                                                     final OperationContext context,
                                                                     final Executor executor);

    /**
     * Creates a new scope with the given name.
     *
     * @param scopeName Scope name
     * @param context operation context
     * @param executor executor
     *
     * @return null on success and exception on failure.
     */
    CompletableFuture<CreateScopeStatus> createScope(final String scopeName, final OperationContext context, Executor executor);

    /**
     * Deletes a Scope if contains no streams.
     *
     * @param scopeName Name of scope to be deleted
     * @param context operation context
     * @param executor executor
     * @return null on success and exception on failure.
     */
    CompletableFuture<DeleteScopeStatus> deleteScope(final String scopeName, final OperationContext context, Executor executor);

    /**
     * Deletes a Scope if contains no streams.
     *
     * @param scopeName Name of scope to be deleted
     * @param context operation context
     * @param executor executor
     * @return null on success and exception on failure.
     */
    CompletableFuture<DeleteScopeStatus> deleteScopeRecursive(final String scopeName, final OperationContext context,
                                                                                  final Executor executor);

    /**
     * Retrieve configuration of scope.
     *
     * @param scopeName Name of scope.
     * @param context operation context
     * @param executor executor
     * @return Returns configuration of scope.
     */
    CompletableFuture<String> getScopeConfiguration(final String scopeName, final OperationContext context, Executor executor);

    /**
     * List existing streams in scopes.
     *
     * @param scopeName Name of the scope
     * @param context operation context
     * @param executor executor
     * @return A map of streams in scope to their configurations
     */
    CompletableFuture<Map<String, StreamConfiguration>> listStreamsInScope(final String scopeName, final OperationContext context,
                                                                           final Executor executor);

    /**
     * List existing streams in scopes with pagination. This api continues listing streams from the supplied continuation token
     * and returns a count limited list of streams and a new continuation token.
     *
     * @param scopeName Name of the scope
     * @param continuationToken continuation token
     * @param limit limit on number of streams to return.
     * @param context operation context
     * @param executor executor
     * @return A pair of list of streams in scope with the continuation token. 
     */
    CompletableFuture<Pair<List<String>, String>> listStream(final String scopeName, final String continuationToken,
                                                             final int limit, final Executor executor, OperationContext context);

    /**
     * List streams with the provided tag inside a scope with pagination. This api continues listing streams from the supplied continuation token
     * and returns list of streams and a new continuation token.
     *
     * @param scopeName Name of the scope
     * @param tag Stream tag.
     * @param continuationToken continuation token
     * @param context operation context
     * @param executor executor
     * @return A pair of list of streams in scope with the continuation token.
     */
    CompletableFuture<Pair<List<String>, String>> listStreamsForTag(final String scopeName, final String tag, final String continuationToken,
                                                                    final Executor executor, OperationContext context);

    /**
     * List Scopes in cluster.
     *
     * @param requestId requestId
     * @param executor executor service.
     * @return List of scopes
     */
    CompletableFuture<List<String>> listScopes(Executor executor, long requestId);

    /**
     * List scopes with pagination. This api continues listing scopes from the supplied continuation token
     * and returns a count limited list of scopes and a new continuation token.
     *
     * @param continuationToken continuation token
     * @param limit limit on number of scopes to return.
     * @param executor executor
     * @param requestId request id
     * @return A pair of list of scopes with the continuation token.
     */
    CompletableFuture<Pair<List<String>, String>> listScopes(final String continuationToken,
                                                             final int limit, final Executor executor, long requestId);
    
    /**
     * Updates the configuration of an existing stream.
     *
     * @param scope         stream scope
     * @param name          stream name.
     * @param configuration new stream configuration.
     * @param context       operation context
     * @param executor      callers executor
     * @return Future of operation
     */
    CompletableFuture<Void> startUpdateConfiguration(final String scope,
                                                     final String name,
                                                     final StreamConfiguration configuration,
                                                     final OperationContext context,
                                                     final Executor executor);

    /**
     * Complete an ongoing update of stream configuration.
     *
     * @param scope         stream scope
     * @param name          stream name.
     * @param existing      versioned StreamConfigurationRecord
     * @param context       operation context
     * @param executor      callers executor
     * @return future of operation
     */
    CompletableFuture<Void> completeUpdateConfiguration(final String scope,
                                                        final String name,
                                                        final VersionedMetadata<StreamConfigurationRecord> existing,
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
     * Fetches the current stream configuration.
     *
     * @param scope        stream scope
     * @param name         stream name.
     * @param context      operation context
     * @param executor     callers executor
     * @return current stream configuration.
     */
    CompletableFuture<VersionedMetadata<StreamConfigurationRecord>> getConfigurationRecord(final String scope, final String name,
                                                                                           final OperationContext context,
                                                                                           final Executor executor);

    /**
     * Add Stream tags into the IndexTable.
     *
     * @param scope        stream scope
     * @param name         stream name.
     * @param config       stream configuration.
     * @param context      operation context
     * @param executor     callers executor
     * @return Future of the operation.
     */
    CompletableFuture<Void> addStreamTagsToIndex(final String scope, final String name, final StreamConfiguration config, final OperationContext context, final Executor executor);

    /**
     * Remove Stream tags from the IndexTable.
     * @param scope stream scope.
     * @param name stream name.
     * @param tagsRemoved tags to be removed.
     * @param context operation context.
     * @param executor callers executor.
     * @return Future of the operation.
     */
    CompletableFuture<Void> removeTagsFromIndex(final String scope, final String name, Set<String> tagsRemoved, final OperationContext context, final Executor executor);

    /**
     * Method to create an operation context for ReaderGroup. A context ensures that multiple calls to store for the same
     * data are avoided within the same operation.
     * All api signatures are changed to accept context. If context is supplied, the data will be
     * looked up within the context and, upon a cache miss, will be fetched from the external store and cached within the context.
     *
     * @param scope Reader Group scope.
     * @param name  Reader Group name.
     * @param requestId request id
     * @return Return a Reader Group Context
     */
    OperationContext createRGContext(final String scope, final String name, long requestId);

    /**
     * Api to get the versioned state for Reader Group from metadata.
     *
     * @param scope scope name
     * @param name Reader Group name
     * @param ignoreCached ignore cached value and fetch from store.
     * @param context operation context
     * @param executor callers executor
     * @return Future which when completed has the VersionedState.
     */
    CompletableFuture<VersionedMetadata<ReaderGroupState>> getVersionedReaderGroupState(final String scope, final String name,
                                                                                        final boolean ignoreCached,
                                                                                        final OperationContext context,
                                                                                        final Executor executor);

    /**
     * Api to get the state for Reader Group from metadata.
     *
     * @param scope scope name
     * @param name stream name
     * @param ignoreCached ignore cached value and fetch from store.
     * @param context operation context
     * @param executor callers executor
     * @return Future of ReaderGroupState.
     */
    CompletableFuture<ReaderGroupState> getReaderGroupState(final String scope, final String name, final boolean ignoreCached,
                                                            final OperationContext context, final Executor executor);

    /**
     * Add ReaderGroup to scope.
     *
     * @param scopeName       scope name
     * @param rgName          Reader Group name
     * @param readerGroupId   Reader Group Identifier
     * @param executor        Executor
     * @param context         operation context
     * @return boolean indicating whether the stream was created
     */
    CompletableFuture<Void> addReaderGroupToScope(final String scopeName,
                                              final String rgName,
                                              final UUID readerGroupId, final OperationContext context, final Executor executor);

    /**
     * Creates a new stream with the given name and configuration.
     *
     * @param scopeName       scope name
     * @param rgName          Reader Group name
     * @param configuration   Reader Group configuration
     * @param createTimestamp Reader Group creation timestamp
     * @param context         Reader Group operation context
     * @param executor        callers executor
     * @return boolean indicating whether the stream was created
     */
    CompletableFuture<Void> createReaderGroup(final String scopeName,
                                              final String rgName,
                                              final ReaderGroupConfig configuration,
                                              final long createTimestamp,
                                              final OperationContext context,
                                              final Executor executor);

    /**
     * Updates the configuration of an existing Reader Group.
     *
     * @param scope         Reader Group scope
     * @param name          Reader Group name.
     * @param configuration new Reader Group configuration.
     * @param context       operation context
     * @param executor      callers executor
     * @return Future of operation
     */
    CompletableFuture<Void> startRGConfigUpdate(final String scope,
                                                final String name,
                                                final ReaderGroupConfig configuration,
                                                final OperationContext context,
                                                final Executor executor);

    /**
     * Completes the update of Reader Group Configuration.
     *
     * @param scope         Reader Group scope
     * @param name          Reader Group name.
     * @param configRecord  Reader Group Config record.
     * @param context       operation context.
     * @param executor      callers executor.
     * @return Future of operation
     */
    CompletableFuture<Void> completeRGConfigUpdate(final String scope,
                                                    final String name,
                                                    final VersionedMetadata<ReaderGroupConfigRecord> configRecord,
                                                    final OperationContext context,
                                                    final Executor executor);

    /**
     * Delete a Reader Group with the given scope and name.
     *
     * @param scopeName       scope name
     * @param rgName          Reader Group name
     * @param context         Reader Group operation context
     * @param executor        callers executor
     * @return boolean indicating whether the stream was created
     */
    CompletableFuture<Void> deleteReaderGroup(final String scopeName,
                                              final String rgName,
                                              final OperationContext context,
                                              final Executor executor);

    /**
     * Api to update versioned state of ReaderGroup as a CAS operation.
     *
     * @param scope scope name.
     * @param name ReaderGroup name.
     * @param state desired state
     * @param previous current state with version
     * @param context operation context
     * @param executor executor
     * @return Future which when completed contains the updated state and version if successful or exception otherwise.
     */
    CompletableFuture<VersionedMetadata<ReaderGroupState>> updateReaderGroupVersionedState(
            final String scope, final String name, final ReaderGroupState state,
            final VersionedMetadata<ReaderGroupState> previous,
            final OperationContext context, final Executor executor);

    /**
     * Fetches the current ReaderGroup configuration.
     *
     * @param scope    ReaderGroup scope
     * @param name     ReaderGroup name.
     * @param context  operation context
     * @param executor callers executor
     * @return current ReaderGroup configuration record.
     */
    CompletableFuture<VersionedMetadata<ReaderGroupConfigRecord>> getReaderGroupConfigRecord(final String scope, final String name,
                                                                                             final OperationContext context,
                                                                                             final Executor executor);

    /**
     * Creates a new subscribers record in metadata for an existing stream.
     *
     * @param scopeName         stream scope name.
     * @param streamName        stream name.
     * @param subscriber        new subscriber Reader Group.
     * @param generation        subscriber generation number.
     * @param context           operation context
     * @param executor          callers executor
     * @return Future of operation
     */
    CompletableFuture<Void> addSubscriber(final String scopeName, final String streamName, String subscriber, long generation,
                                             final OperationContext context, final Executor executor);

    /**
     * Deletes the subscriber Reader Group from Stream Metadata.
     *
     * @param scope         stream scope
     * @param name          stream name.
     * @param subscriber    subscriber Reader Group to be removed.
     * @param generation    subscriber generation.
     * @param context       operation context
     * @param executor      callers executor
     * @return Future of operation
     */
    CompletableFuture<Void> deleteSubscriber(final String scope,
                                             final String name,
                                             final String subscriber,
                                             final long generation,
                                             final OperationContext context,
                                             final Executor executor);

    /**
     * Updates the subscribers metadata for an existing stream.
     *
     * @param scope         stream scope
     * @param name          stream name.
     * @param subscriber new stream subscriber.
     * @param generation subscriber generation.
     * @param streamCut     new truncation streamcut of subscriber.
     * @param previousRecord previous truncation streamcut of subscriber.
     * @param context       operation context
     * @param executor      callers executor
     * @return Future of operation
     */
    CompletableFuture<Void> updateSubscriberStreamCut(final String scope,
                                                     final String name,
                                                     final String subscriber,
                                                     final long generation,
                                                     final ImmutableMap<Long, Long> streamCut,
                                                     final VersionedMetadata<StreamSubscriber> previousRecord,
                                                     final OperationContext context,
                                                     final Executor executor);

    /**
     * Fetches the current stream subscribers record.
     *
     * @param scope        stream scope
     * @param name         stream name.
     * @param subscriber   subscriber name.
     * @param context      operation context.
     * @param executor     callers executor.
     * @return current stream configuration.
     */
    CompletableFuture<VersionedMetadata<StreamSubscriber>> getSubscriber(final String scope, final String name,
                                                                                       final String subscriber,
                                                                                       final OperationContext context,
                                                                                       final Executor executor);

    /**
     * List scopes with pagination. This api continues listing scopes from the supplied continuation token
     * and returns a count limited list of scopes and a new continuation token.
     *
     * @param scope scope name.
     * @param stream stream for which to list subscribers.
     * @param context operation context.
     * @param executor executor.
     * @return A list of subscribers for Stream.
     */
    CompletableFuture<List<String>> listSubscribers(final String scope, final String stream,
                                                    final OperationContext context, final Executor executor);


    /**
     * Start new stream truncation.
     *
     * @param scope         stream scope
     * @param name          stream name.
     * @param streamCut     new stream cut.
     * @param context       operation context.
     * @param executor      callers executor.
     * @return future of operation.
     */
    CompletableFuture<Void> startTruncation(final String scope,
                                            final String name,
                                            final Map<Long, Long> streamCut,
                                            final OperationContext context,
                                            final Executor executor);

    /**
     * Complete an ongoing stream truncation.
     *
     * @param scope               stream scope
     * @param name                stream name.
     * @param record              versioned record
     * @param context             operation context.
     * @param executor            callers executor.
     * @return boolean indicating whether the stream was updated
     */
    CompletableFuture<Void> completeTruncation(final String scope,
                                               final String name,
                                               final VersionedMetadata<StreamTruncationRecord> record,
                                               final OperationContext context,
                                               final Executor executor);

    /**
     * Fetches the current stream cut.
     *
     * @param scope        stream scope
     * @param name         stream name.
     * @param context      operation context
     * @param executor     callers executor
     * @return current truncation property.
     */
    CompletableFuture<VersionedMetadata<StreamTruncationRecord>> getTruncationRecord(final String scope, final String name,
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
    CompletableFuture<Void> setSealed(final String scope, final String name, final OperationContext context,
                                      final Executor executor);

    /**
     * Get the stream sealed status.
     *
     * @param scope    stream scope
     * @param name     stream name.
     * @param context  operation context
     * @param executor callers executor
     * @return boolean indicating whether the stream is sealed.
     */
    CompletableFuture<Boolean> isSealed(final String scope, final String name, final OperationContext context,
                                        final Executor executor);

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
    CompletableFuture<StreamSegmentRecord> getSegment(final String scope, final String name, final long number,
                                                      final OperationContext context, final Executor executor);

    /**
     * Api to get all segments in the stream. 
     *
     * @param scope    stream scope
     * @param name     stream name.
     * @param context  operation context
     * @param executor callers executor
     *                 
     * @return Future, which when complete will contain a list of all segments in the stream. 
     */
    CompletableFuture<Set<Long>> getAllSegmentIds(final String scope, final String name, final OperationContext context,
                                                  final Executor executor);

    /**
     * Get active segments.
     *
     * @param scope    stream scope
     * @param name     stream name.
     * @param executor callers executor
     * @param context  operation context
     * @return currently active segments
     */
    CompletableFuture<List<StreamSegmentRecord>> getActiveSegments(final String scope, final String name,
                                                                   final OperationContext context, final Executor executor);
    
    /**
     * Returns the segments at the head of the stream.
     *
     * @param scope    scope.
     * @param stream   stream.
     * @param context  operation context
     * @param executor callers executor
     * @return         list of active segments in specified epoch.
     */
    CompletableFuture<Map<StreamSegmentRecord, Long>> getSegmentsAtHead(final String scope,
                                                                        final String stream,
                                                                        final OperationContext context,
                                                                        final Executor executor);

    /**
     * Returns the segments in the specified epoch of the specified stream.
     *
     * @param scope    scope.
     * @param stream   stream.
     * @param epoch    epoch.
     * @param context  operation context
     * @param executor callers executor
     * @return         list of active segments in specified epoch.
     */
    CompletableFuture<List<StreamSegmentRecord>> getSegmentsInEpoch(final String scope,
                                                                    final String stream,
                                                                    final int epoch,
                                                                    final OperationContext context,
                                                                    final Executor executor);

    /**
     * Given a segment return a map containing the numbers of the segments immediately succeeding it
     * mapped to a list of the segments they succeed.
     *
     * @param scope         stream scope
     * @param streamName    stream name.
     * @param segmentId the segment number
     * @param context       operation context
     * @param executor      callers executor
     * @return segments that immediately follow the specified segment and the segments they follow.
     */
    CompletableFuture<Map<StreamSegmentRecord, List<Long>>> getSuccessors(final String scope,
                                                                          final String streamName,
                                                                          final long segmentId,
                                                                          final OperationContext context,
                                                                          final Executor executor);

    /**
     * Given two stream cuts, this method return a list of segments that lie between given stream cuts.
     *
     * @param scope      stream scope
     * @param streamName stream name.
     * @param from       from stream cut
     * @param to         to stream cut
     * @param context    operation context
     * @param executor   callers executor
     * @return Future which when completed contains list of segments between given stream cuts.
     */
    CompletableFuture<List<StreamSegmentRecord>> getSegmentsBetweenStreamCuts(final String scope,
                                                                              final String streamName,
                                                                              final Map<Long, Long> from,
                                                                              final Map<Long, Long> to,
                                                                              final OperationContext context,
                                                                              final Executor executor);

    /**
     * Method to validate stream cut based on its definition - disjoint sets that cover the entire range of keyspace.
     *
     * @param scope scope name
     * @param streamName stream name
     * @param streamCut stream cut to validate
     * @param context execution context
     * @param executor executor
     * @return Future which when completed has the result of validation check (true for valid and false for illegal streamCuts).
     */
    CompletableFuture<Boolean> isStreamCutValid(final String scope,
                                                final String streamName,
                                                final Map<Long, Long> streamCut,
                                                final OperationContext context,
                                                final Executor executor);

    /**
     * Api to get Versioned epoch transition record.
     *
     * @param scope scope
     * @param stream stream
     * @param context operation context
     * @param executor executor
     *
     * @return Future which when completed has the versioned epoch transition record.
     */
    CompletableFuture<VersionedMetadata<EpochTransitionRecord>> getEpochTransition(String scope, String stream,
                                                                                   OperationContext context,
                                                                                   ScheduledExecutorService executor);

    /**
     * ResetEpoch transition record back to EMPTY.
     *
     * @param scope          stream scope
     * @param name           stream name.
     * @param record         versioned record
     * @param context        operation context
     * @param executor       callers executor
     * @return A future which when completed indicates that epoch transition record has been reset and holds the updated
     * versioned record.
     */
    CompletableFuture<VersionedMetadata<EpochTransitionRecord>> resetEpochTransition(final String scope, final String name,
                                          final VersionedMetadata<EpochTransitionRecord> record,
                                          final OperationContext context,
                                          final Executor executor);

    /**
     * Called to start metadata updates to stream store with respect to new scale request. This method should only update
     * the epochTransition record to reflect current request. It should not initiate the scale workflow. 
     * In case of rolling transactions, this record may become invalid and can be discarded during the startScale phase
     * of scale workflow. 
     *
     * @param scope          stream scope
     * @param name           stream name.
     * @param newRanges      new key ranges to be added to the stream which maps to a new segment per range in the stream
     * @param sealedSegments segments to be sealed
     * @param scaleTimestamp timestamp at which scale was requested
     * @param record         optionally supply existing epoch transition record. if null is supplied, it will be fetched from the store. 
     * @param context        operation context
     * @param executor       callers executor
     * @return the list of newly created segments
     */
    CompletableFuture<VersionedMetadata<EpochTransitionRecord>> submitScale(final String scope, final String name,
                                                                            final List<Long> sealedSegments,
                                                                            final List<Map.Entry<Double, Double>> newRanges,
                                                                            final long scaleTimestamp,
                                                                            final VersionedMetadata<EpochTransitionRecord> record, 
                                                                            final OperationContext context,
                                                                            final Executor executor);

    /**
     * Method to start a new scale. This method will check if epoch transition record is consistent or if
     * a rolling transaction has rendered it inconsistent with the state in store.
     * For manual scale this method will migrate the epoch transaction. For auto scale, it will discard any
     * inconsistent record and reset the state.
     *
     * @param scope          stream scope
     * @param name           stream name.
     * @param isManualScale  flag to indicate that the processing is being performed for manual scale
     * @param record previous versioned record
     * @param state  previous versioned state
     * @param context        operation context
     * @param executor       callers executor
     * @return future Future which when completed contains updated epoch transition record with version or exception otherwise.
     */
    CompletableFuture<VersionedMetadata<EpochTransitionRecord>> startScale(final String scope,
                                       final String name,
                                       final boolean isManualScale,
                                       final VersionedMetadata<EpochTransitionRecord> record,
                                       final VersionedMetadata<State> state,
                                       final OperationContext context,
                                       final Executor executor);

    /**
     * Called after we have successfully verified epoch transition record and started the scale workflow. 
     * Implementation of this method should create new segments that are specified in epochTransition in stream metadata records.
     *
     * @param scope          stream scope
     * @param name           stream name.
     * @param record         versioned record
     * @param context        operation context
     * @param executor       callers executor
     * @return Future, which when completed will indicate that new segments are created in the metadata store or would
     * have failed with appropriate exception.
     */
    CompletableFuture<VersionedMetadata<EpochTransitionRecord>> scaleCreateNewEpochs(final String scope,
                                                   final String name,
                                                   final VersionedMetadata<EpochTransitionRecord> record,
                                                   final OperationContext context,
                                                   final Executor executor);
    
    /**
     * Called after sealing old segments is complete in segment store. 
     * The implementation of this method should update epoch metadata for the given scale input in an idempotent fashion
     * such that active epoch at least reflects the new epoch updated by this method's call. 
     *
     * @param scope          stream scope
     * @param name           stream name.
     * @param sealedSegmentSizes sealed segments with size at the time of sealing
     * @param record         versioned record
     * @param context        operation context
     * @param executor       callers executor
     * @return Future, which when completed will indicate successful and idempotent metadata update corresponding to
     * sealing of old segments in the store. 
     */
    CompletableFuture<Void> scaleSegmentsSealed(final String scope, final String name,
                                                final Map<Long, Long> sealedSegmentSizes,
                                                final VersionedMetadata<EpochTransitionRecord> record,
                                                final OperationContext context,
                                                final Executor executor);

    /**
     * Called at the end of scale workflow to let the store know to complete the scale. This should reset the epoch transition
     * record to signal completion of scale workflow. 
     * Note: the state management is outside the purview of this method and should be done explicitly by the caller. 
     *
     * @param scope          stream scope
     * @param name           stream name.
     * @param record         versioned record
     * @param context        operation context
     * @param executor       callers executor
     * @return A future which when completed indicates the completion current scale workflow.                 
     */
    CompletableFuture<Void> completeScale(final String scope, final String name,
                                          final VersionedMetadata<EpochTransitionRecord> record,
                                          final OperationContext context,
                                          final Executor executor);

    /**
     * Api to indicate to store to start rolling transaction. 
     * The store attempts to update CommittingTransactionsRecord with details about rolling transaction information, 
     * specifically updating active epoch in the aforesaid record. 
     *
     * @param scope scope
     * @param stream stream
     * @param activeEpoch active epoch
     * @param existing versioned committing transactions record that has to be updated
     * @param context operation context
     * @param executor executor
     * @return A future which when completed will capture updated versioned committing transactions record that represents 
     * an ongoing rolling transaction.
     */
    CompletableFuture<VersionedMetadata<CommittingTransactionsRecord>> startRollingTxn(
            String scope, String stream, int activeEpoch, VersionedMetadata<CommittingTransactionsRecord> existing,
            OperationContext context, ScheduledExecutorService executor);

    /**
     * This method is called from Rolling transaction workflow after new transactions that are duplicate of active transactions
     * have been created successfully in segment store.
     * This method will update metadata records for epoch to add two new epochs, one for duplicate txn epoch where transactions
     * are merged and the other for duplicate active epoch.
     *
     * @param scope          stream scope
     * @param name           stream name.
     * @param sealedTxnEpochSegments sealed segments from intermediate txn epoch with size at the time of sealing
     * @param time           timestamp
     * @param record         previous versioned record
     * @param context        operation context
     * @param executor       callers executor
     * @return CompletableFuture which upon completion will indicate that we have successfully created new epoch entries.
     */
    CompletableFuture<Void> rollingTxnCreateDuplicateEpochs(final String scope,
                                           final String name, Map<Long, Long> sealedTxnEpochSegments,
                                           final long time, final VersionedMetadata<CommittingTransactionsRecord> record,
                                           final OperationContext context, final Executor executor);

    /**
     * This is final step of rolling transaction and is called after old segments are sealed in segment store.
     * This should complete the epoch transition in the metadata store.
     *
     * @param scope          stream scope
     * @param name           stream name.
     * @param sealedActiveEpochSegments sealed segments from active epoch with size at the time of sealing
     * @param record         previous versioned record
     * @param context        operation context
     * @param executor       callers executor
     * @return CompletableFuture which upon successful completion will indicate that rolling transaction is complete.
     */
    CompletableFuture<Void> completeRollingTxn(final String scope, final String name,
                                                                    final Map<Long, Long> sealedActiveEpochSegments, 
                                                                    final VersionedMetadata<CommittingTransactionsRecord> record,
                                                                    final OperationContext context, final Executor executor);

    /**
     * Method to create a new unique transaction id on the stream.
     *
     * @param scopeName        Scope
     * @param streamName       Stream
     *                         the scaling operation is initiated on the txn stream.
     * @param context          operation context
     * @param executor         callers executor
     * @return Future when completed contains a new unique txn id.
     */
    CompletableFuture<UUID> generateTransactionId(final String scopeName, final String streamName,
                                                  final OperationContext context,
                                                  final Executor executor);

    /**
     * Method to create a new transaction on a stream.
     *
     * @param scopeName        Scope
     * @param streamName       Stream
     * @param txnId            Transaction identifier.
     * @param lease            Time for which transaction shall remain open with sending any heartbeat.
     * @param maxExecutionTime Maximum time for which client may extend txn lease.
     * @param context          operation context
     * @param executor         callers executor
     * @return Transaction data along with version information.
     */
    CompletableFuture<VersionedTransactionData> createTransaction(final String scopeName, final String streamName,
                                                                  final UUID txnId,
                                                                  final long lease, final long maxExecutionTime,
                                                                  final OperationContext context,
                                                                  final Executor executor);

    /**
     * Heartbeat to keep the transaction open for at least lease amount of time.
     *
     * @param scopeName  Scope
     * @param streamName Stream
     * @param txData     Transaction data
     * @param lease      Lease duration in ms
     * @param context    operation context
     * @param executor   callers executor
     * @return Transaction data along with version information.
     */
    CompletableFuture<VersionedTransactionData> pingTransaction(final String scopeName, final String streamName,
                                                                final VersionedTransactionData txData, final long lease,
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
    CompletableFuture<TxnStatus> transactionStatus(final String scope, final String stream, final UUID txId,
                                                   final OperationContext context, final Executor executor);

    /**
     * Update stream store to mark transaction as sealed.
     *
     * @param scope    scope
     * @param stream   stream
     * @param txId     transaction id
     * @param commit   Boolean indicating whether to change txn state to committing or aborting.
     * @param version  Expected version of the transaction record in the store.
     * @param writerId writer id. Used only if commit is set to true. 
     * @param timestamp commit timestamp. Valid only when commit flag is true.
     * @param context  operation context
     * @param executor callers executor
     * @return         Pair containing the transaction status after sealing and transaction epoch.
     */
    CompletableFuture<SimpleEntry<TxnStatus, Integer>> sealTransaction(final String scope, final String stream,
                                                                       final UUID txId, final boolean commit,
                                                                       final Optional<Version> version,
                                                                       final String writerId,
                                                                       final long timestamp,
                                                                       final OperationContext context,
                                                                       final Executor executor);

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
    CompletableFuture<TxnStatus> abortTransaction(final String scope, final String stream,
                                                  final UUID txId, final OperationContext context,
                                                  final Executor executor);

    /**
     * Method to retrive all currently active transactions from the metadata store.
     *
     * @param scope    scope of stream
     * @param stream   name of stream
     * @param context  operation context
     * @param executor callers executor
     * @return map of txId to TxRecord
     */
    CompletableFuture<Map<UUID, ActiveTxnRecord>> getActiveTxns(final String scope, final String stream,
                                                                final OperationContext context, final Executor executor);

    /**
     * Method to retrieve List of transaction in completed(COMMITTED/ABORTED) state
     * from most recent batch.
     *
     * @param scope Scope of stream.
     * @param stream Name of stream.
     * @param context Operation context.
     * @param executor Caller executor.
     * @return Map having transactionId and transaction status.
     */
    CompletableFuture<Map<UUID, TxnStatus>> listCompletedTxns(final String scope, final String stream, final OperationContext context, final Executor executor);

    /**
     * Adds specified resource as a child of current host's hostId node.
     * This is idempotent operation.
     *
     * @param hostId      Host identifier.
     * @param txn         Tracked transaction resource.
     * @param version     Version of tracked transaction's node.
     * @return            A future that completes on completion of the operation.
     */
    CompletableFuture<Void> addTxnToIndex(final String hostId, final TxnResource txn, final Version version);

    /**
     * Removes the specified child node from the specified parent node.
     * This is idempotent operation.
     * If deleteEmptyParent is true and parent has no child after deletion of given child then parent is also deleted.
     *
     * @param hostId            Node whose child is to be removed.
     * @param txn               Transaction resource to remove.
     * @param deleteEmptyParent To delete or not to delete.
     * @return void in future.
     */
    CompletableFuture<Void> removeTxnFromIndex(final String hostId, final TxnResource txn,
                                               final boolean deleteEmptyParent);

    /**
     * Returns a transaction managed by specified host, if one exists.
     *
     * @param hostId Host identifier.
     * @return A transaction managed by specified host, if one exists.
     */
    CompletableFuture<Optional<TxnResource>> getRandomTxnFromIndex(final String hostId);

    /**
     * Fetches version of specified txn stored in the index under specified host.
     *
     * @param hostId    Host identifier.
     * @param resource  Txn resource.
     * @return txn version stored in the index under specified host.
     */
    CompletableFuture<Version> getTxnVersionFromIndex(final String hostId, final TxnResource resource);

    /**
     * Remove the specified host from the index.
     *
     * @param hostId Host identifier.
     * @return A future indicating completion of removal of the host from index.
     */
    CompletableFuture<Void> removeHostFromIndex(String hostId);

    /**
     * Fetches set of hosts that own some txn.
     *
     * @return set of hosts owning some txn.
     */
    CompletableFuture<Set<String>> listHostsOwningTxn();

    /**
     * Returns a map of pending tasks that were created by the host but their corresponding event was probably not posted.
     *
     * @param hostId Host identifier.
     * @param limit number of tasks to retrieve from store
     * @return A CompletableFuture which when completed will have a map of tasks to events that should be posted.
     */
    CompletableFuture<Map<String, ControllerEvent>> getPendingsTaskForHost(final String hostId, final int limit);

    /**
     * Remove the specified host from the index.
     *
     * @param hostId Host identifier.
     * @return A future indicating completion of removal of the host from index.
     */
    CompletableFuture<Void> removeHostFromTaskIndex(String hostId);

    /**
     * Fetches set of hosts that own some tasks for which events have to be posted.
     *
     * @return set of hosts owning some pending tasks.
     */
    CompletableFuture<Set<String>> listHostsWithPendingTask();

    /**
     * Returns the currently active epoch of the specified stream.
     *
     * @param scope    scope.
     * @param stream   stream.
     * @param context  operation context
     * @param ignoreCached  boolean indicating whether to use cached value or force fetch from underlying store.
     * @param executor callers executor
     * @return         Completable future that holds active epoch history record upon completion.
     */
    CompletableFuture<EpochRecord> getActiveEpoch(final String scope,
                                                  final String stream,
                                                  final OperationContext context,
                                                  final boolean ignoreCached,
                                                  final Executor executor);

    /**
     * Returns the record for the given epoch of the specified stream.
     *
     * @param scope    scope.
     * @param stream   stream.
     * @param epoch    epoch
     * @param context  operation context
     * @param executor callers executor
     * @return         Completable future that, upon completion, holds epoch history record corresponding to request epoch.
     */
    CompletableFuture<EpochRecord> getEpoch(final String scope,
                                              final String stream,
                                              final int epoch,
                                              final OperationContext context,
                                              final Executor executor);

    /**
     * Api to mark a segment as cold.
     *
     * @param scope         scope for stream
     * @param stream        name of stream
     * @param segmentId segment number
     * @param timestamp     time till which this cold marker is valid.
     * @param context       context in which this operation is taking place.
     * @param executor      callers executor
     * @return Completable future
     */
    CompletableFuture<Void> markCold(final String scope, final String stream, final long segmentId,
                                     final long timestamp, final OperationContext context, final Executor executor);

    /**
     * Api to return if a cold marker is set.
     *
     * @param scope    scope for stream
     * @param stream   name of stream
     * @param segmentId   segment nunmber
     * @param context  context in which this operation is taking place.
     * @param executor callers executor
     * @return Completable future Optional of marker's creation time.
     */
    CompletableFuture<Boolean> isCold(final String scope, final String stream, final long segmentId,
                                      final OperationContext context, final Executor executor);

    /**
     * Api to clear marker.
     *
     * @param scope    scope for stream
     * @param stream   name of stream
     * @param segmentId   segment nunmber
     * @param context  context in which this operation is taking place.
     * @param executor callers executor
     * @return Completable Future
     */
    CompletableFuture<Void> removeMarker(final String scope, final String stream, final long segmentId,
                                         final OperationContext context, final Executor executor);

    /**
     * Get all scale history segments.
     *
     * @param scope    stream scope
     * @param name     stream name.
     * @param from     from time
     * @param to       to
     * @param executor callers executor
     * @param context  operation context
     * @return currently active segments
     */
    CompletableFuture<List<ScaleMetadata>> getScaleMetadata(final String scope, final String name, final long from, 
                                                            final long to, final OperationContext context, final Executor executor);
    
    /**
     * Add stream cut to retention set of the given stream.
     *
     * @param scope     scope
     * @param stream    stream
     * @param streamCut stream cut to add
     * @param context   context
     * @param executor  executor
     * @return future
     */
    CompletableFuture<Void> addStreamCutToRetentionSet(final String scope, final String stream, final StreamCutRecord streamCut,
                                                       final OperationContext context, final Executor executor);

    /**
     * Get retention set made of stream cuts for the given stream.
     *
     * @param scope    scope
     * @param stream   stream
     * @param context  context
     * @param executor executor
     * @return future
     */
    CompletableFuture<RetentionSet> getRetentionSet(final String scope, final String stream,
                                                    final OperationContext context, final Executor executor);

    /**
     * Get stream cut record corresponding to the reference.
     *
     * @param scope    scope
     * @param stream   stream
     * @param reference reference record
     * @param context  context
     * @param executor executor
     * @return future which when completed will contain the stream cut record corresponding to reference record
     */
    CompletableFuture<StreamCutRecord> getStreamCutRecord(final String scope, final String stream, 
                                                          final StreamCutReferenceRecord reference,
                                                          final OperationContext context, final Executor executor);

    /**
     * Delete all stream cuts with recording time before the supplied stream cut from the retention set of the stream.
     *
     * @param scope     scope
     * @param stream    stream
     * @param streamCut stream cut to purge from
     * @param context   context
     * @param executor  executor
     * @return future
     */
    CompletableFuture<Void> deleteStreamCutBefore(final String scope, final String stream, final StreamCutReferenceRecord streamCut,
                                                  final OperationContext context, final Executor executor);

    /**
     * Method to get size till the supplied stream cut map.
     *
     * @param scope scope name
     * @param stream stream name
     * @param streamCut stream cut to get the size till
     * @param reference optional reference record
     * @param context operation context
     * @param executor executor
     * @return A CompletableFuture which, when completed, will contain size of stream till given streamCut.
     */
    CompletableFuture<Long> getSizeTillStreamCut(final String scope, final String stream, final Map<Long, Long> streamCut,
                                                 final Optional<StreamCutRecord> reference, 
                                                 final OperationContext context, final ScheduledExecutorService executor);

    /**
     * Method to create committing transaction record in the store for a given stream.
     * This method may throw data exists exception if the committing transaction node already exists and the epoch in 
     * the request does not match the epoch present in the record. 
     *
     * @param scope scope name
     * @param stream stream name
     * @param limit maximum number of transactions to include
     * @param context operation context
     * @param executor executor
     * @return A completableFuture which, when completed, mean that the record has been created successfully.
     */
    CompletableFuture<Map.Entry<VersionedMetadata<CommittingTransactionsRecord>, List<VersionedTransactionData>>> startCommitTransactions(final String scope,
                                                                                               final String stream,
                                                                                               final int limit,
                                                                                               final OperationContext context,
                                                                                               final ScheduledExecutorService executor);

    /**
     * Method to fetch committing transaction record from the store for a given stream.
     * Note: this will not throw data not found exception if the committing transaction node is not found. Instead
     * it returns null.
     *
     * @param scope scope name
     * @param stream stream name
     * @param context operation context
     * @param executor executor
     * @return A completableFuture which, when completed, will contain committing transaction record if it exists, or null otherwise.
     */
    CompletableFuture<VersionedMetadata<CommittingTransactionsRecord>> getVersionedCommittingTransactionsRecord(
            final String scope, final String stream, final OperationContext context, final ScheduledExecutorService executor);

    /**
     * Method to delete committing transaction record from the store for a given stream.
     *
     * @param scope scope name
     * @param stream stream name
     * @param record versioned record
     * @param context operation context
     * @param executor executor
     * @param writerMarks Mapping of WriterId to Transaction Offset.
     * @return A completableFuture which, when completed, will mean that deletion of txnCommitNode is complete.
     */
    CompletableFuture<Void> completeCommitTransactions(final String scope, final String stream,
                                                       final VersionedMetadata<CommittingTransactionsRecord> record,
                                                       final OperationContext context, final ScheduledExecutorService executor,
                                                       Map<String, TxnWriterMark> writerMarks);

    /**
     * This method attempts to create a new Waiting Request node and set the processor's name in the node.
     * If a node already exists, this attempt is ignored.
     *
     * @param scope scope
     * @param stream stream
     * @param processorName name of the request processor that is waiting to get an opportunity for processing.
     * @param context operation context
     * @param executor executor
     * @return CompletableFuture which indicates that a node was either created successfully or records the failure.
     */
    CompletableFuture<Void> createWaitingRequestIfAbsent(String scope, String stream, String processorName,
                                                         OperationContext context, ScheduledExecutorService executor);

    /**
     * This method fetches existing waiting request processor's name if any. It returns null if no processor is waiting.
     *
     * @param scope scope
     * @param stream stream
     * @param context operation context
     * @param executor executor
     * @return CompletableFuture which has the name of the processor that had requested for a wait, or null if there was no
     * such request.
     */
    CompletableFuture<String> getWaitingRequestProcessor(String scope, String stream, OperationContext context,
                                                         ScheduledExecutorService executor);

    /**
     * Delete existing waiting request processor if the name of the existing matches suppied processor name.
     *
     * @param scope scope
     * @param stream stream
     * @param processorName processor name which is to be deleted if it matches the name in waiting record in the store.
     * @param context operation context
     * @param executor executor
     * @return CompletableFuture which indicates completion of processing.
     */
    CompletableFuture<Void> deleteWaitingRequestConditionally(String scope, String stream, String processorName,
                                                              OperationContext context, ScheduledExecutorService executor);

    /**
     * Method to record writer's mark in the metadata store. If this is a known writer, its mark is updated if it advances 
     * both time and position from the previously recorded position.    
     * @param scope scope name
     * @param stream stream name
     * @param writer writer id
     * @param timestamp mark timestamp
     * @param position writer position 
     * @param context operation context
     * @param executor executor
     * @return A completableFuture, which when completed, will have recorded the new mark for the writer. 
     */
    CompletableFuture<WriterTimestampResponse> noteWriterMark(String scope, String stream, String writer,
                                                              long timestamp, Map<Long, Long> position,
                                                              OperationContext context, Executor executor);

    /**
     * Method to mark a writer for shutdown. A shutdown writer can be revived if newer marks are reported for it.
     * A writer which is marked for shutdown is removed asynchronously after its latest reported mark is included for 
     * a watermarking computation. 
     * 
     * @param scope scope name
     * @param stream stream name
     * @param writer writer id
     * @param context operation context
     * @param executor executor
     * @return A completableFuture, which when completed, will have shutdown writer. 
     */
    CompletableFuture<Void> shutdownWriter(String scope, String stream, String writer, OperationContext context,
                                           Executor executor);

    /**
     * Method to remove writer specific metadata conditionally from the metadata store.  
     * A writer is removed only if the writer mark in the store matches the given writer mark. Remove method is idempotent, 
     * so if writer is not found, its considered removed. 
     * @param scope scope name
     * @param stream stream name
     * @param writer writer id
     * @param writerMark writer mark to match for removal
     * @param context operation context
     * @param executor executor
     * @return A completableFuture, which when completed, will have removed writer metadata. 
     */
    CompletableFuture<Void> removeWriter(String scope, String stream, String writer, WriterMark writerMark,
                                         OperationContext context, Executor executor);

    /**
     * Method to retrieve writer's latest recorded mark.  
     * @param scope scope name
     * @param stream stream name
     * @param writer writer id
     * @param context operation context
     * @param executor executor
     * @return A completableFuture, which when completed, will contain writer's mark.  
     */
    CompletableFuture<WriterMark> getWriterMark(String scope, String stream, String writer, OperationContext context,
                                                Executor executor);

    /**
     * Method to retrieve latest recorded mark for all known writers.  
     * @param scope scope name
     * @param stream stream name
     * @param context operation context
     * @param executor executor
     * @return A completableFuture, which when completed, will contain map of writer to respective marks.  
     */
    CompletableFuture<Map<String, WriterMark>> getAllWriterMarks(String scope, String stream, OperationContext context,
                                                                 Executor executor);
    
    /**
     * Method to get the requested chunk of the HistoryTimeSeries.
     *
     * @param scope      stream scope.
     * @param streamName stream name.
     * @param chunkNumber chunk number.
     * @param context    operation context.
     * @param executor   callers executor.
     * @return Completable future that, upon completion, holds the requested HistoryTimeSeries chunk.
     */
    CompletableFuture<HistoryTimeSeries> getHistoryTimeSeriesChunk(final String scope, final String streamName,
                                                                   final int chunkNumber, final OperationContext context,
                                                                   final Executor executor);

    /**
     * Method to get the requested shard of sealed segments map.
     *
     * @param scope      stream scope.
     * @param streamName stream name.
     * @param shardNumber shard number.
     * @param context    operation context.
     * @param executor   callers executor.
     * @return Completable future that, upon completion, holds the requested sealed segment map shard.
     */
    CompletableFuture<SealedSegmentsMapShard> getSealedSegmentSizeMapShard(final String scope, final String streamName,
                                                                           final int shardNumber, final OperationContext context,
                                                                           final Executor executor);

    /**
     * Method to get epoch in which a segment was sealed.
     *
     * @param scope      stream scope.
     * @param streamName stream name.
     * @param segmentId  segment id.
     * @param context    operation context.
     * @param executor   callers executor.
     * @return Completable future that, upon completion, holds the epoch in which the segment was sealed OR
     * a negative number if segment is not sealed.
     */
    CompletableFuture<Integer> getSegmentSealedEpoch(final String scope, final String streamName, final long segmentId,
                                                     final OperationContext context, final Executor executor);

    /**
     * Compares two Stream cuts and returns StreamCutComparison enum.
     * A streamcut is considered greater than equals another if for all key ranges the segment/offset in one streamcut is ahead of 
     * second streamcut. 
     *
     * @param scope      stream scope.
     * @param streamName stream name.
     * @param streamCut1 Stream cut to check
     * @param streamCut2 Streamcut to check against
     * @param context    operation context.
     * @param executor   callers executor.
     *                   
     * @return A completable future which when completed will hold an integer which will determine the order between 
     * streamcut1 and streamcut2. 
     */
    CompletableFuture<StreamCutComparison> compareStreamCut(final String scope, final String streamName,
                                                Map<Long, Long> streamCut1, Map<Long, Long> streamCut2,
                                                final OperationContext context, final Executor executor);

    /**
     * Finds the latest streamcutreference record from retentionset that is strictly before the supplied streamcut.    
     *
     * @param scope      stream scope.
     * @param streamName stream name.
     * @param streamCut Stream cut to check.
     * @param retentionSet Retention set
     * @param context    operation context.
     * @param executor   callers executor.
     *
     * @return A completable future which when completed will the reference record to the latest stream cut from retention set which
     * is strictly before the supplied streamcut. 
     */
    CompletableFuture<StreamCutReferenceRecord> findStreamCutReferenceRecordBefore(final String scope, final String streamName,
                                                                                   Map<Long, Long> streamCut, final RetentionSet retentionSet,
                                                                                   final OperationContext context, final Executor executor);

    /**
     * Api to check if a ReaderGroup exists in the store or not.
     * @param scope scope name
     * @param rgName Name ReaderGroup name
     * @param context operation context
     * @param executor executor
     * @return true if stream exists, false otherwise
     */
    CompletableFuture<Boolean> checkReaderGroupExists(final String scope, final String rgName, OperationContext context,
                                                      Executor executor);

    /**
     * Api to retrieve ReaderGroup id from the metadata.
     * @param scopeName scope name
     * @param rgName Name ReaderGroup name
     * @param context operation context
     * @param executor executor
     * @return true if stream exists, false otherwise
     */
    CompletableFuture<UUID> getReaderGroupId(final String scopeName, final String rgName, OperationContext context,
                                             Executor executor);

    /**
     * Api to retrieve UUID of scope from the metadata.
     * @param scopeName scope name
     * @param context operation context
     * @param executor executor
     * @return UUID of scope
     */
    CompletableFuture<UUID> getScopeId(final String scopeName, OperationContext context, Executor executor);

    CompletableFuture<Void> sealScope(String scope, OperationContext context, ScheduledExecutorService executor);
}
