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
package io.pravega.controller.server;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import io.pravega.client.control.impl.ModelHelper;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.tables.KeyValueTableConfiguration;
import io.pravega.common.Exceptions;
import io.pravega.common.Timer;
import io.pravega.common.cluster.Cluster;
import io.pravega.common.cluster.ClusterException;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.hash.RandomFactory;
import io.pravega.common.tracing.RequestTracker;
import io.pravega.common.tracing.TagLogger;
import io.pravega.common.util.RetriesExhaustedException;
import io.pravega.controller.metrics.StreamMetrics;
import io.pravega.controller.metrics.TransactionMetrics;
import io.pravega.controller.store.SegmentRecord;
import io.pravega.controller.store.kvtable.KVTableMetadataStore;
import io.pravega.controller.store.stream.BucketStore;
import io.pravega.controller.store.stream.OperationContext;
import io.pravega.controller.store.stream.ScaleMetadata;
import io.pravega.controller.store.stream.State;
import io.pravega.controller.store.stream.StoreException;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.VersionedTransactionData;
import io.pravega.controller.store.stream.records.StreamSegmentRecord;
import io.pravega.controller.stream.api.grpc.v1.Controller;
import io.pravega.controller.stream.api.grpc.v1.Controller.CreateKeyValueTableStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.CreateReaderGroupResponse;
import io.pravega.controller.stream.api.grpc.v1.Controller.CreateScopeStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.CreateStreamStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.DeleteKVTableStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.DeleteReaderGroupStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.DeleteScopeStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.DeleteStreamStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.KeyValueTableConfigResponse;
import io.pravega.controller.stream.api.grpc.v1.Controller.NodeUri;
import io.pravega.controller.stream.api.grpc.v1.Controller.PingTxnStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.ReaderGroupConfigResponse;
import io.pravega.controller.stream.api.grpc.v1.Controller.ScaleResponse;
import io.pravega.controller.stream.api.grpc.v1.Controller.ScaleStatusResponse;
import io.pravega.controller.stream.api.grpc.v1.Controller.SegmentId;
import io.pravega.controller.stream.api.grpc.v1.Controller.SegmentRange;
import io.pravega.controller.stream.api.grpc.v1.Controller.SubscribersResponse;
import io.pravega.controller.stream.api.grpc.v1.Controller.TxnState;
import io.pravega.controller.stream.api.grpc.v1.Controller.TxnStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.UpdateReaderGroupResponse;
import io.pravega.controller.stream.api.grpc.v1.Controller.UpdateStreamStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.UpdateSubscriberStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.ControllerToBucketMappingRequest.BucketType;
import io.pravega.controller.task.KeyValueTable.TableMetadataTasks;
import io.pravega.controller.task.Stream.StreamMetadataTasks;
import io.pravega.controller.task.Stream.StreamTransactionMetadataTasks;
import io.pravega.shared.NameUtils;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.LoggerFactory;

import java.security.SecureRandom;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

/**
 * Stream controller RPC server implementation.
 */
@Getter
@AllArgsConstructor
public class ControllerService {
    private static final TagLogger log = new TagLogger(LoggerFactory.getLogger(ControllerService.class));
    // Generator for new request identifiers
    private static final SecureRandom REQUEST_ID_GENERATOR = RandomFactory.createSecure();

    private final KVTableMetadataStore kvtMetadataStore;
    private final TableMetadataTasks kvtMetadataTasks;
    private final StreamMetadataStore streamStore;
    private final BucketStore bucketStore;
    private final StreamMetadataTasks streamMetadataTasks;
    private final StreamTransactionMetadataTasks streamTransactionMetadataTasks;
    private final SegmentHelper segmentHelper;
    private final Executor executor;
    private final Cluster cluster;
    private final RequestTracker requestTracker;

    public static long nextRequestId() {
        return REQUEST_ID_GENERATOR.nextLong();
    }

    public CompletableFuture<List<NodeUri>> getControllerServerList() {
        if (cluster == null) {
            return Futures.failedFuture(new IllegalStateException("Controller cluster not initialized"));
        }

        return CompletableFuture.supplyAsync(() -> {
            try {
                return cluster.getClusterMembers().stream()
                        .map(host -> NodeUri.newBuilder().setEndpoint(host.getIpAddr()).setPort(host.getPort()).build())
                        .collect(Collectors.toList());
            } catch (ClusterException e) {
                // cluster implementation throws checked exceptions which cannot be thrown inside completable futures.
                throw Exceptions.sneakyThrow(e);
            }
        }, executor);
    }

    public CompletableFuture<CreateKeyValueTableStatus> createKeyValueTable(String scope, String kvtName,
                                                                            final KeyValueTableConfiguration kvtConfig,
                                                                            final long createTimestamp, 
                                                                            final long requestId) {
        Preconditions.checkNotNull(kvtConfig, "kvTableConfig");
        Preconditions.checkArgument(createTimestamp >= 0);
        Preconditions.checkArgument(kvtConfig.getPartitionCount() > 0);
        Preconditions.checkArgument(kvtConfig.getPrimaryKeyLength() > 0);
        Preconditions.checkArgument(kvtConfig.getSecondaryKeyLength() >= 0);
        Preconditions.checkArgument(kvtConfig.getRolloverSizeBytes() >= 0);
        Timer timer = new Timer();
        try {
            NameUtils.validateUserKeyValueTableName(kvtName);
        } catch (IllegalArgumentException | NullPointerException e) {
            log.warn(requestId, "Create KeyValueTable failed due to invalid name {}", kvtName);
            return CompletableFuture.completedFuture(
                    CreateKeyValueTableStatus.newBuilder().setStatus(CreateKeyValueTableStatus.Status.INVALID_TABLE_NAME).build());
        }
        return kvtMetadataTasks.createKeyValueTable(scope, kvtName, kvtConfig, createTimestamp, requestId)
                .thenApplyAsync(status -> {
                    reportCreateKVTableMetrics(scope, kvtName, kvtConfig.getPartitionCount(), status, timer.getElapsed());
                    return CreateKeyValueTableStatus.newBuilder().setStatus(status).build();
                }, executor);
    }

    public CompletableFuture<List<SegmentRange>> getCurrentSegmentsKeyValueTable(final String scope, final String kvtName, 
                                                                                 final long requestId) {
        Exceptions.checkNotNullOrEmpty(scope, "scope");
        Exceptions.checkNotNullOrEmpty(kvtName, "KeyValueTable");

        // Fetch active segments from segment store.
        return kvtMetadataStore.getActiveSegments(scope, kvtName, null, executor)
                .thenApplyAsync(activeSegments -> getSegmentRanges(activeSegments, scope, kvtName), executor);
    }

    /**
     * List existing KeyValueTables in specified scope.
     *
     * @param scope Name of the scope.
     * @param token continuation token
     * @param limit limit for number of KeyValueTables to return.
     * @param requestId  request id
     * @return List of KeyValueTables in scope.
     */
    public CompletableFuture<Pair<List<String>, String>> listKeyValueTables(final String scope, final String token, final int limit,
                                                                            final long requestId) {
        Exceptions.checkNotNullOrEmpty(scope, "scope");
        OperationContext context = streamStore.createScopeContext(scope, requestId);
        return kvtMetadataStore.listKeyValueTables(scope, token, limit, context, executor);
    }

    public CompletableFuture<KeyValueTableConfigResponse> getKeyValueTableConfiguration(final String scope, final String kvtName,
                                                                                                   final long requestId) {
        Exceptions.checkNotNullOrEmpty(scope, "Scope name");
        Exceptions.checkNotNullOrEmpty(kvtName, "KeyValueTable name.");
        OperationContext context = kvtMetadataStore.createContext(scope, kvtName, requestId);
        return kvtMetadataStore.getConfiguration(scope, kvtName, context, executor).handleAsync((r, ex) -> {
            if (ex == null) {
                return KeyValueTableConfigResponse.newBuilder().setConfig(ModelHelper.decode(scope, kvtName, r))
                        .setStatus(KeyValueTableConfigResponse.Status.SUCCESS).build();
            } else if (Exceptions.unwrap(ex) instanceof StoreException.DataNotFoundException) {
                return KeyValueTableConfigResponse.newBuilder().setStatus(KeyValueTableConfigResponse.Status.TABLE_NOT_FOUND).build();
            }
            return KeyValueTableConfigResponse.newBuilder().setStatus(KeyValueTableConfigResponse.Status.FAILURE).build();
        });
    }

    /**
     * Deletes key value table.
     * 
     * @param scope scope
     * @param kvtName key value table name.
     * @param requestId requestId
     * @return deletion status future. 
     */
    public CompletableFuture<DeleteKVTableStatus> deleteKeyValueTable(final String scope, final String kvtName,
                                                                      final long requestId) {
        Exceptions.checkNotNullOrEmpty(scope, "Scope Name");
        Exceptions.checkNotNullOrEmpty(kvtName, "KeyValueTable Name");
        Timer timer = new Timer();
        return kvtMetadataTasks.deleteKeyValueTable(scope, kvtName, requestId)
                .thenApplyAsync(status -> {
                    reportDeleteKVTableMetrics(scope, kvtName, status, timer.getElapsed());
                    return DeleteKVTableStatus.newBuilder().setStatus(status).build();
                }, executor);
    }

    /**
     * Creates reader group metadata. 
     * @param scope scope
     * @param rgName reader group name
     * @param rgConfig reader group config
     * @param createTimestamp creation time
     * @param requestId request id
     * @return Create Readergroup status future. 
     */
    public CompletableFuture<CreateReaderGroupResponse> createReaderGroup(String scope, String rgName,
                                                                          final ReaderGroupConfig rgConfig,
                                                                          final long createTimestamp,
                                                                          final long requestId) {
        Preconditions.checkNotNull(scope, "ReaderGroup scope is null");
        Preconditions.checkNotNull(rgName, "ReaderGroup name is null");
        Preconditions.checkNotNull(rgConfig, "ReaderGroup config is null");
        Preconditions.checkArgument(createTimestamp >= 0);
        Timer timer = new Timer();
        try {
            NameUtils.validateReaderGroupName(rgName);
        } catch (IllegalArgumentException | NullPointerException e) {
            log.error(requestId, "Create ReaderGroup failed due to invalid name {}", rgName);
            return CompletableFuture.completedFuture(
                    CreateReaderGroupResponse.newBuilder().setStatus(CreateReaderGroupResponse.Status.INVALID_RG_NAME).build());
        }
        return streamMetadataTasks.createReaderGroup(scope, rgName, rgConfig, createTimestamp, requestId)
                .thenApplyAsync(response -> {
                    reportCreateReaderGroupMetrics(scope, rgName, response.getStatus(), timer.getElapsed());
                    return response;
                }, executor);
    }

    /**
     * Updates reader group metadata. 
     * @param scope scope 
     * @param rgName reader group name
     * @param rgConfig reader group config.
     * @param requestId request id
     * @return Update reader group response future. 
     */
    public CompletableFuture<UpdateReaderGroupResponse> updateReaderGroup(String scope, String rgName,
                                                                          final ReaderGroupConfig rgConfig,
                                                                          final long requestId) {
        Preconditions.checkNotNull(scope, "ReaderGroup scope is null");
        Preconditions.checkNotNull(rgName, "ReaderGroup name is null");
        Preconditions.checkNotNull(rgConfig, "ReaderGroup config is null");
        Timer timer = new Timer();
        return streamMetadataTasks.updateReaderGroup(scope, rgName, rgConfig, requestId)
                .thenApplyAsync(response -> {
                            reportUpdateReaderGroupMetrics(scope, rgName, response.getStatus(), timer.getElapsed());
                            return response;
                }, executor);
    }

    /**
     * Updates reader group metadata. 
     * @param scope scope 
     * @param rgName reader group name
     * @param requestId request id
     * @return reader group config response future. 
     */
    public CompletableFuture<ReaderGroupConfigResponse> getReaderGroupConfig(String scope, String rgName,
                                                                             final long requestId) {
        Preconditions.checkNotNull(scope, "ReaderGroup scope is null");
        Preconditions.checkNotNull(rgName, "ReaderGroup name is null");
        return streamMetadataTasks.getReaderGroupConfig(scope, rgName, requestId);
    }

    public CompletableFuture<DeleteReaderGroupStatus> deleteReaderGroup(String scope, String rgName, String readerGroupId,
                                                                        long requestId) {
        Preconditions.checkNotNull(scope, "ReaderGroup scope is null");
        Preconditions.checkNotNull(rgName, "ReaderGroup name is null");
        Preconditions.checkNotNull(readerGroupId, "ReaderGroup Id is null");
        Timer timer = new Timer();
        return streamMetadataTasks.deleteReaderGroup(scope, rgName, readerGroupId, requestId)
                .thenApplyAsync(status -> {
                    reportDeleteReaderGroupMetrics(scope, rgName, status, timer.getElapsed());
                    return DeleteReaderGroupStatus.newBuilder().setStatus(status).build();
                }, executor);
    }

    /**
     * list subscribers of a stream. 
     * @param scope scope 
     * @param stream stream name
     * @param requestId request id
     * @return reader group config response future. 
     */
    public CompletableFuture<SubscribersResponse> listSubscribers(final String scope, final String stream,
                                                                  final long requestId) {
        Preconditions.checkNotNull(scope, "scopeName is null");
        Preconditions.checkNotNull(stream, "streamName is null");
        return streamMetadataTasks.listSubscribers(scope, stream, requestId);

    }

    /**
     * Update subscribers streamcut. 
     * @param scope scope
     * @param stream stream 
     * @param subscriber subscriber id
     * @param readerGroupId reader group identifier
     * @param generation reader group metadata generation
     * @param truncationStreamCut stream cut position of subscriber.
     * @param requestId request id.
     * @return update subscriber status future
     */
    public CompletableFuture<UpdateSubscriberStatus> updateSubscriberStreamCut(String scope, String stream,
                                                                               final String subscriber,
                                                                               final String readerGroupId,
                                                                               final long generation,
                                                                               final ImmutableMap<Long, Long> truncationStreamCut,
                                                                               final long requestId) {
        Preconditions.checkNotNull(scope, "scopeName is null");
        Preconditions.checkNotNull(stream, "streamName is null");
        Preconditions.checkNotNull(subscriber, "subscriber is null");
        Preconditions.checkNotNull(readerGroupId, "readerGroupId is null");
        Preconditions.checkNotNull(truncationStreamCut, "Truncation StreamCut is null");
        Timer timer = new Timer();
        return streamMetadataTasks.updateSubscriberStreamCut(scope, stream, subscriber, readerGroupId, generation, 
                truncationStreamCut, requestId)
                            .thenApplyAsync(status -> {
                                reportUpdateTruncationSCMetrics(scope, stream, status, timer.getElapsed());
                                return UpdateSubscriberStatus.newBuilder().setStatus(status).build();
                            }, executor);
    }

    /**
     * Creates stream with specified configuration.
     * @param scope scope
     * @param stream stream
     * @param streamConfig stream configuration
     * @param createTimestamp creation time
     * @param requestId request id
     * @return Create stream status future.
     */
    public CompletableFuture<CreateStreamStatus> createInternalStream(String scope, String stream, final StreamConfiguration streamConfig,
                                                              final long createTimestamp, long requestId) {
        validate(streamConfig, createTimestamp);
        try {
            NameUtils.validateStreamName(stream);
        } catch (IllegalArgumentException | NullPointerException e) {
            log.error(requestId, "Create stream failed due to invalid stream name {}", stream);
            return CompletableFuture.completedFuture(
                    CreateStreamStatus.newBuilder().setStatus(CreateStreamStatus.Status.INVALID_STREAM_NAME).build());
        }
        return callCreateStream(scope, stream, streamConfig, createTimestamp, requestId);
    }

    /**
     * Creates stream with specified configuration. 
     * @param scope scope
     * @param stream stream name
     * @param streamConfig stream configuration
     * @param createTimestamp creation time
     * @param requestId request id
     * @return Create stream status future. 
     */
    public CompletableFuture<CreateStreamStatus> createStream(String scope, String stream, final StreamConfiguration streamConfig,
            final long createTimestamp, long requestId) {
        validate(streamConfig, createTimestamp);
        try {
            NameUtils.validateStreamName(stream);
        } catch (IllegalArgumentException | NullPointerException e) {
            log.error(requestId, "Create stream failed due to invalid stream name {}", stream);
            return CompletableFuture.completedFuture(
                    CreateStreamStatus.newBuilder().setStatus(CreateStreamStatus.Status.INVALID_STREAM_NAME).build());
        }
        return callCreateStream(scope, stream, streamConfig, createTimestamp, requestId);
    }

    private CompletableFuture<CreateStreamStatus> callCreateStream(final String scope, final String stream,
                                                                   final StreamConfiguration streamConfig,
                                                                   final long createTimestamp, long requestId) {
        Timer timer = new Timer();
        return Futures.exceptionallyExpecting(streamStore.getState(scope, stream, true, null, executor),
                        e -> Exceptions.unwrap(e) instanceof StoreException.DataNotFoundException, State.UNKNOWN)
                .thenCompose(state -> {
                    if (state.equals(State.UNKNOWN) || state.equals(State.CREATING)) {
                        return streamMetadataTasks.createStreamRetryOnLockFailure(scope,
                                stream,
                                streamConfig,
                                createTimestamp, 10,
                                requestId).thenApplyAsync(status -> {
                            reportCreateStreamMetrics(scope, stream, streamConfig.getScalingPolicy().getMinNumSegments(),
                                    status, timer.getElapsed());
                            return CreateStreamStatus.newBuilder().setStatus(status).build();
                        }, executor);
                    } else {
                        log.info(requestId, "Stream {} already exists ", NameUtils.getScopedStreamName(scope, stream));
                        return CompletableFuture.completedFuture(
                                CreateStreamStatus.newBuilder().setStatus(CreateStreamStatus.Status.STREAM_EXISTS).build());
                    }
                });
    }

    private void validate(final StreamConfiguration streamConfig, final long createTimestamp) {
        Preconditions.checkNotNull(streamConfig, "streamConfig");
        Preconditions.checkArgument(createTimestamp >= 0);
        Preconditions.checkArgument(streamConfig.getRolloverSizeBytes() >= 0,
                String.format("Segment rollover size bytes cannot be less than 0, actual is %s", streamConfig.getRolloverSizeBytes()));
    }

    /**
     * Updates stream with specified configuration. 
     * @param scope scope
     * @param stream stream
     * @param streamConfig stream configuration
     * @param requestId request id
     * @return Update stream status future.
     */
    public CompletableFuture<UpdateStreamStatus> updateStream(String scope, String stream, final StreamConfiguration streamConfig,
                                                              long requestId) {
        Preconditions.checkNotNull(streamConfig, "streamConfig");
        Timer timer = new Timer();
        return streamMetadataTasks.updateStream(scope, stream, streamConfig, requestId)
                .thenApplyAsync(status -> {
                    reportUpdateStreamMetrics(scope, stream, status, timer.getElapsed());
                    return UpdateStreamStatus.newBuilder().setStatus(status).build();
                }, executor);
    }

    /**
     * Truncates stream at specified streamcut. 
     * @param scope scope
     * @param stream stream
     * @param streamCut streamcut to truncate at. 
     * @param requestId request id
     * @return Create stream status future. 
     */
    public CompletableFuture<UpdateStreamStatus> truncateStream(final String scope, final String stream,
                                                                final Map<Long, Long> streamCut, long requestId) {
        Preconditions.checkNotNull(scope, "scope");
        Preconditions.checkNotNull(stream, "stream");
        Preconditions.checkNotNull(streamCut, "streamCut");
        Timer timer = new Timer();
        return streamMetadataTasks.truncateStream(scope, stream, streamCut, requestId)
                .thenApplyAsync(status -> {
                    reportTruncateStreamMetrics(scope, stream, status, timer.getElapsed());
                    return UpdateStreamStatus.newBuilder().setStatus(status).build();
                }, executor);
    }

    public CompletableFuture<StreamConfiguration> getStream(final String scopeName, final String streamName, long requestId) {
        OperationContext context = streamStore.createStreamContext(scopeName, streamName, requestId);
        return streamStore.getConfiguration(scopeName, streamName, context, executor);
    }

    public CompletableFuture<UpdateStreamStatus> sealStream(final String scope, final String stream, long requestId) {
        Exceptions.checkNotNullOrEmpty(scope, "scope");
        Exceptions.checkNotNullOrEmpty(stream, "stream");
        Timer timer = new Timer();
        return streamMetadataTasks.sealStream(scope, stream, requestId)
                .thenApplyAsync(status -> {
                    reportSealStreamMetrics(scope, stream, status, timer.getElapsed());
                    return UpdateStreamStatus.newBuilder().setStatus(status).build();
                }, executor);
    }

    public CompletableFuture<DeleteStreamStatus> deleteStream(final String scope, final String stream, long requestId) {
        Exceptions.checkNotNullOrEmpty(scope, "scope");
        Exceptions.checkNotNullOrEmpty(stream, "stream");
        Timer timer = new Timer();
        return streamMetadataTasks.deleteStream(scope, stream, requestId)
                .thenApplyAsync(status -> {
                    reportDeleteStreamMetrics(scope, stream, status, timer.getElapsed());
                    return DeleteStreamStatus.newBuilder().setStatus(status).build();
                }, executor);
    }

    public CompletableFuture<List<SegmentRange>> getCurrentSegments(final String scope, final String stream, long requestId) {
        Exceptions.checkNotNullOrEmpty(scope, "scope");
        Exceptions.checkNotNullOrEmpty(stream, "stream");

        // Fetch active segments from segment store.
        OperationContext context = streamStore.createStreamContext(scope, stream, requestId);

        return streamStore.getActiveSegments(scope, stream, context, executor)
                .thenApplyAsync(activeSegments -> getSegmentRanges(activeSegments, scope, stream), executor);
    }

    public CompletableFuture<List<SegmentRange>> getEpochSegments(final String scope, final String stream, int epoch, long requestId) {
        Exceptions.checkNotNullOrEmpty(scope, "scope");
        Exceptions.checkNotNullOrEmpty(stream, "stream");
        Exceptions.checkArgument(epoch >= 0, "epoch", "Epoch cannot be less than 0");
        OperationContext context = streamStore.createStreamContext(scope, stream, requestId);

        return streamStore.getEpoch(scope, stream, epoch, context, executor)
                          .thenApplyAsync(epochRecord -> getSegmentRanges(epochRecord.getSegments(), scope, stream), executor);
    }

    public CompletableFuture<Map<SegmentId, Long>> getSegmentsAtHead(final String scope, final String stream, long requestId) {
        Exceptions.checkNotNullOrEmpty(scope, "scope");
        Exceptions.checkNotNullOrEmpty(stream, "stream");

        // First fetch segments active at specified timestamp from the specified stream.
        // Divide current segments in segmentFutures into at most count positions.
        OperationContext context = streamStore.createStreamContext(scope, stream, requestId);

        return streamStore.getSegmentsAtHead(scope, stream, context, executor).thenApply(segments -> {
            return segments.entrySet().stream()
                           .collect(Collectors.toMap(entry -> ModelHelper.createSegmentId(scope, stream, entry.getKey().segmentId()),
                                   Map.Entry::getValue));
        });
    }

    public CompletableFuture<Map<SegmentRange, List<Long>>> getSegmentsImmediatelyFollowing(SegmentId segment, long requestId) {
        Preconditions.checkNotNull(segment, "segment");
        String scope = segment.getStreamInfo().getScope();
        String stream = segment.getStreamInfo().getStream();
        OperationContext context = streamStore.createStreamContext(scope, stream, requestId);
        return streamStore.getSuccessors(scope,
                stream,
                segment.getSegmentId(),
                context,
                executor)
                .thenApply(successors -> successors.entrySet().stream()
                        .collect(Collectors.toMap(
                                entry -> ModelHelper.createSegmentRange(scope,
                                        stream, entry.getKey().segmentId(),
                                                entry.getKey().getKeyStart(),
                                                entry.getKey().getKeyEnd()),
                                Map.Entry::getValue)));
    }

    public CompletableFuture<List<StreamSegmentRecord>> getSegmentsBetweenStreamCuts(Controller.StreamCutRange range, 
                                                                                     long requestId) {
        Preconditions.checkNotNull(range, "segment");
        Preconditions.checkArgument(!(range.getFromMap().isEmpty() && range.getToMap().isEmpty()));

        String scope = range.getStreamInfo().getScope();
        String stream = range.getStreamInfo().getStream();
        OperationContext context = streamStore.createStreamContext(scope, stream, requestId);
        return streamStore.getSegmentsBetweenStreamCuts(scope,
                stream,
                range.getFromMap(),
                range.getToMap(),
                context,
                executor);
    }

    public CompletableFuture<ScaleResponse> scale(final String scope,
                                                  final String stream,
                                                  final List<Long> segmentsToSeal,
                                                  final Map<Double, Double> newKeyRanges,
                                                  final long scaleTimestamp, final long requestId) {
        Exceptions.checkNotNullOrEmpty(scope, "scope");
        Exceptions.checkNotNullOrEmpty(stream, "stream");
        Preconditions.checkNotNull(segmentsToSeal, "sealedSegments");
        Preconditions.checkNotNull(newKeyRanges, "newKeyRanges");

        return streamMetadataTasks.manualScale(scope,
                                         stream,
                                         segmentsToSeal,
                                         new ArrayList<>(ModelHelper.encode(newKeyRanges)),
                                         scaleTimestamp,
                                         requestId);
    }

    public CompletableFuture<ScaleStatusResponse> checkScale(final String scope, final String stream, final int epoch, 
                                                             final long requestId) {
        Exceptions.checkNotNullOrEmpty(scope, "scope");
        Exceptions.checkNotNullOrEmpty(stream, "stream");
        Exceptions.checkArgument(epoch >= 0, "epoch", "Epoch cannot be less than 0");

        return streamMetadataTasks.checkScale(scope, stream, epoch, requestId);
    }

    public CompletableFuture<List<ScaleMetadata>> getScaleRecords(final String scope, final String stream, final long from, 
                                                                  final long to, final long requestId) {
        Exceptions.checkNotNullOrEmpty(scope, "scope");
        Exceptions.checkNotNullOrEmpty(stream, "stream");
        OperationContext context = streamStore.createStreamContext(scope, stream, requestId);
        return streamStore.getScaleMetadata(scope, stream, from, to, context, executor);
    }

    /**
     * Gets the uri of segment store which owns this segment. 
     *
     * @param segment segment to validate
     * @return future that will contain the uri of the segment store node. 
     */
    public CompletableFuture<NodeUri> getURI(final SegmentId segment) {
        Preconditions.checkNotNull(segment, "segment");

        return CompletableFuture.completedFuture(
                segmentHelper.getSegmentUri(segment.getStreamInfo().getScope(), segment.getStreamInfo().getStream(),
                        segment.getSegmentId()));
    }

    private SegmentRange convert(final String scope,
                                 final String stream,
                                 final SegmentRecord segment) {
        Exceptions.checkNotNullOrEmpty(scope, "scope");
        Exceptions.checkNotNullOrEmpty(stream, "stream");
        Preconditions.checkNotNull(segment, "segment");
        return ModelHelper.createSegmentRange(
                scope, stream, segment.segmentId(), segment.getKeyStart(), segment.getKeyEnd());
    }

    /**
     * Checks if the segment is still open. 
     *
     * @param scope scope 
     * @param stream stream name
     * @param segmentId segment to validate
     * @param requestId request id
     * @return future that when completed will indicate if the segment is open or not. 
     */
    public CompletableFuture<Boolean> isSegmentValid(final String scope,
                                                     final String stream,
                                                     final long segmentId,
                                                     final long requestId) {
        Exceptions.checkNotNullOrEmpty(scope, "scope");
        Exceptions.checkNotNullOrEmpty(stream, "stream");
        OperationContext context = streamStore.createStreamContext(scope, stream, requestId);

        return streamStore.getActiveSegments(scope, stream, context, executor)
                .thenApplyAsync(x -> x.stream().anyMatch(z -> z.segmentId() == segmentId), executor);
    }

    /**
     * Checks if its a well formed streamcut. Well formed stream cuts are those that can unambiguously split the stream into two parts, 
     * before and after. Any event position can be compared against a streamcut to determine unambiguously if it lies
     * before, On, or after the streamcut. 
     *
     * @param scope scope 
     * @param stream stream name
     * @param streamCut stream cut to validate
     * @param requestId request id
     * @return future that when completed will indicate if the stream cut is well formed or not. 
     */
    public CompletableFuture<Boolean> isStreamCutValid(final String scope,
                                                       final String stream,
                                                       final Map<Long, Long> streamCut, final long requestId) {
        Exceptions.checkNotNullOrEmpty(scope, "scope");
        Exceptions.checkNotNullOrEmpty(stream, "stream");
        OperationContext context = streamStore.createStreamContext(scope, stream, requestId);

        return streamStore.isStreamCutValid(scope, stream, streamCut, context, executor);
    }

    /**
     * Creates transaction where a new txn id is generated and the txn segments and metadata is created.
     * 
     * @param scope scope 
     * @param stream stream name
     * @param lease lease for transaction.
     * @param requestId request id
     * @return Transaction state future
     */
    @SuppressWarnings("ReturnCount")
    public CompletableFuture<Pair<UUID, List<SegmentRange>>> createTransaction(final String scope, final String stream,
                                                                               final long lease, final long requestId) {
        Exceptions.checkNotNullOrEmpty(scope, "scope");
        Exceptions.checkNotNullOrEmpty(stream, "stream");
        Timer timer = new Timer();
        OperationContext context = streamStore.createStreamContext(scope, stream, requestId);
        return streamStore.getConfiguration(scope, stream, context, executor).thenCompose(streamConfig ->
                streamTransactionMetadataTasks.createTxn(scope, stream, lease, requestId, streamConfig.getRolloverSizeBytes()))
                .thenApply(pair -> {
                    VersionedTransactionData data = pair.getKey();
                    List<StreamSegmentRecord> segments = pair.getValue();
                    return new ImmutablePair<>(data.getId(), getSegmentRanges(segments, scope, stream));
                }).handle((result, ex) -> {
                    if (ex != null) {
                        TransactionMetrics.getInstance().createTransactionFailed(scope, stream);
                        throw new CompletionException(ex);
                    }
                    TransactionMetrics.getInstance().createTransaction(scope, stream, timer.getElapsed());
                    return result;
                });
    }

    private List<SegmentRange> getSegmentRanges(List<? extends SegmentRecord> activeSegments, String scope, String stream) {
        List<SegmentRange> listOfSegment = activeSegments
                .stream()
                .map(segment -> convert(scope, stream, segment))
                .collect(Collectors.toList());
        listOfSegment.sort(Comparator.comparingDouble(SegmentRange::getMinKey));
        return listOfSegment;
    }

    /**
     * Commits transaction. Updates txn metadata with committing status and then posts an event for asynchronous processing. 
     * 
     * @param scope scope 
     * @param stream stream name
     * @param txId transaction id
     * @param writerId writer id
     * @param timestamp timestamp
     * @param requestId request id
     * @return Transaction state future
     */
    public CompletableFuture<TxnStatus> commitTransaction(final String scope, final String stream, final UUID txId,
                                                          final String writerId, final long timestamp, final long requestId) {
        Exceptions.checkNotNullOrEmpty(scope, "scope");
        Exceptions.checkNotNullOrEmpty(stream, "stream");
        Preconditions.checkNotNull(txId, "txnId");
        Timer timer = new Timer();
        return streamTransactionMetadataTasks.commitTxn(scope, stream, txId, writerId, timestamp, requestId)
                .handle((ok, ex) -> {
                    if (ex != null) {
                        log.error(requestId, "Transaction commit failed", ex);
                        log.error("Transaction commit failed for txn {} on stream {}. Cause: {}", txId.toString(),
                                NameUtils.getScopedStreamName(scope, stream), ex);
                        Throwable unwrap = getRealException(ex);
                        if (unwrap instanceof StoreException.DataNotFoundException || unwrap instanceof StoreException.IllegalStateException) {
                            TransactionMetrics.getInstance().commitTransactionFailed(scope, stream);
                            return TxnStatus.newBuilder().setStatus(TxnStatus.Status.FAILURE).build();
                        }
                        throw new CompletionException(unwrap);
                    } else {
                        TransactionMetrics.getInstance().committingTransaction(timer.getElapsed());
                        return TxnStatus.newBuilder().setStatus(TxnStatus.Status.SUCCESS).build();
                    }
                });
    }

    private Throwable getRealException(Throwable ex) {
        Throwable unwrap = Exceptions.unwrap(ex);
        if (unwrap instanceof RetriesExhaustedException) {
            unwrap = Exceptions.unwrap(unwrap.getCause());
        }
        return unwrap;
    }

    /**
     * Aborts transaction. Updates txn metadata with Aborting status and then posts an event for asynchronous processing. 
     * 
     * @param scope scope 
     * @param stream stream name
     * @param txId transaction id
     * @param requestId request id
     * @return Transaction state future
     */
    public CompletableFuture<TxnStatus> abortTransaction(final String scope, final String stream, final UUID txId, long requestId) {
        Exceptions.checkNotNullOrEmpty(scope, "scope");
        Exceptions.checkNotNullOrEmpty(stream, "stream");
        Preconditions.checkNotNull(txId, "txnId");
        Timer timer = new Timer();
        return streamTransactionMetadataTasks.abortTxn(scope, stream, txId, null, requestId)
                .handle((ok, ex) -> {
                    if (ex != null) {
                        log.error(requestId, "Transaction abort failed for txn {} on Stream {}", txId.toString(),
                                NameUtils.getScopedStreamName(scope, stream), ex);
                        Throwable unwrap = getRealException(ex);
                        if (unwrap instanceof StoreException.DataNotFoundException || unwrap instanceof StoreException.IllegalStateException) {
                            TransactionMetrics.getInstance().abortTransactionFailed(scope, stream);
                            return TxnStatus.newBuilder().setStatus(TxnStatus.Status.FAILURE).build();
                        }
                        throw new CompletionException(unwrap);
                    } else {
                        TransactionMetrics.getInstance().abortingTransaction(timer.getElapsed());
                        return TxnStatus.newBuilder().setStatus(TxnStatus.Status.SUCCESS).build();
                    }
                });
    }

    /**
     * Pings transaction by updating its metadata.
     * @param scope scope 
     * @param stream stream name
     * @param txId transaction id
     * @param lease lease
     * @param requestId request id
     * @return Ping Transaction status future
     */
    public CompletableFuture<PingTxnStatus> pingTransaction(final String scope,
                                                            final String stream,
                                                            final UUID txId,
                                                            final long lease, 
                                                            final long requestId) {
        Exceptions.checkNotNullOrEmpty(scope, "scope");
        Exceptions.checkNotNullOrEmpty(stream, "stream");
        Preconditions.checkNotNull(txId, "txnId");

        return streamTransactionMetadataTasks.pingTxn(scope, stream, txId, lease, requestId);
    }

    /**
     * Checks transaction status.
     * @param scope scope 
     * @param stream stream name
     * @param txnId transaction id
     * @param requestId request id
     * @return Transaction state future
     */
    public CompletableFuture<TxnState> checkTransactionStatus(final String scope, final String stream,
            final UUID txnId, final long requestId) {
        Exceptions.checkNotNullOrEmpty(scope, "scope");
        Exceptions.checkNotNullOrEmpty(stream, "stream");
        Preconditions.checkNotNull(txnId, "txnId");
        OperationContext context = streamStore.createStreamContext(scope, stream, requestId);

        return streamStore.transactionStatus(scope, stream, txnId, context, executor)
                .thenApplyAsync(res -> TxnState.newBuilder().setState(TxnState.State.valueOf(res.name())).build(), executor);
    }

    /**
     * List transaction in completed(COMMITTED/ABORTED) state.
     * @param scope scope name
     * @param stream stream name
     * @param requestId request id
     * @return map having transactionId and transaction status
     */
    public CompletableFuture<Map<UUID, io.pravega.controller.store.stream.TxnStatus>> listCompletedTxns(final String scope, final String stream,
                                                                                                        final long requestId) {
        Exceptions.checkNotNullOrEmpty(scope, "scope");
        Exceptions.checkNotNullOrEmpty(stream, "stream");
        OperationContext context = streamStore.createStreamContext(scope, stream, requestId);
        return streamStore.listCompletedTxns(scope, stream, context, executor);
    }


    /**
     * Controller Service API to create scope.
     *
     * @param scope Name of scope to be created.
     * @param requestId request id
     * @return Status of create scope.
     */
    public CompletableFuture<CreateScopeStatus> createScope(final String scope, final long requestId) {
        Exceptions.checkNotNullOrEmpty(scope, "scope");
        Timer timer = new Timer();
        try {
            NameUtils.validateScopeName(scope);
        } catch (IllegalArgumentException | NullPointerException e) {
            log.error(requestId, "Create scope failed due to invalid scope name {}", scope);
            return CompletableFuture.completedFuture(CreateScopeStatus.newBuilder().setStatus(
                    CreateScopeStatus.Status.INVALID_SCOPE_NAME).build());
        }
        OperationContext context = streamStore.createScopeContext(scope, requestId);

        return streamStore.createScope(scope, context, executor)
                          .thenApply(r -> reportCreateScopeMetrics(scope, r, timer.getElapsed()));
    }

    /**
     * Controller Service API to delete scope.
     *
     * @param scope Name of scope to be deleted.
     * @param requestId request id
     * @return Status of delete scope.
     */
    public CompletableFuture<DeleteScopeStatus> deleteScope(final String scope, final long requestId) {
        Exceptions.checkNotNullOrEmpty(scope, "scope");
        Timer timer = new Timer();
        OperationContext context = streamStore.createScopeContext(scope, requestId);

        return streamStore.deleteScope(scope, context, executor).thenApply(r -> reportDeleteScopeMetrics(scope, r,
                timer.getElapsed()));
    }

    /**
     * Controller Service API to delete scope recursively.
     *
     * @param scope Name of scope to be deleted.
     * @param requestId request id
     * @return Status of delete scope.
     */
    public CompletableFuture<DeleteScopeStatus> deleteScopeRecursive(final String scope, final long requestId) {
        Exceptions.checkNotNullOrEmpty(scope, "scope");
        Timer timer = new Timer();

        return streamMetadataTasks.deleteScopeRecursive(scope, requestId)
                .thenApplyAsync(status -> {
                    reportDeleteScopeRecursiveMetrics(scope, status, timer.getElapsed());
                    return DeleteScopeStatus.newBuilder().setStatus(status).build();
                }, executor);
    }

    /**
     * List existing streams in scopes.
     *
     * @param scope Name of the scope.
     * @param requestId request id
     * @return List of streams in scope.
     */
    public CompletableFuture<Map<String, StreamConfiguration>> listStreamsInScope(final String scope, final long requestId) {
        Exceptions.checkNotNullOrEmpty(scope, "scope");
        OperationContext context = streamStore.createScopeContext(scope, requestId);

        return streamStore.listStreamsInScope(scope, context, executor);
    }

    /**
     * List existing streams in scopes.
     *
     * @param scope Name of the scope.
     * @param token continuation token
     * @param limit limit for number of streams to return.
     * @param requestId request id
     * @return List of streams in scope.
     */
    public CompletableFuture<Pair<List<String>, String>> listStreams(final String scope, final String token, final int limit,
                                                                     final long requestId) {
        Exceptions.checkNotNullOrEmpty(scope, "scope");
        OperationContext context = streamStore.createScopeContext(scope, requestId);

        return streamStore.listStream(scope, token, limit, executor, context);
    }

    /**
     * List streams matching the provided tag in a scope.
     *
     * @param scope Name of the scope.
     * @param tag Tag name.
     * @param token continuation token
     * @param requestId request id
     * @return List of streams in scope.
     */
    public CompletableFuture<Pair<List<String>, String>> listStreamsForTag(final String scope, final String tag,
                                                                           final String token, final long requestId) {
        Exceptions.checkNotNullOrEmpty(scope, "scope");
        Exceptions.checkNotNullOrEmpty(tag, "tag");
        OperationContext context = streamStore.createScopeContext(scope, requestId);

        return streamStore.listStreamsForTag(scope, tag, token, executor, context);
    }

    /**
     * List Scopes in cluster.
     *
     * @param requestId request id
     * @return List of scopes.
     */
    public CompletableFuture<List<String>> listScopes(long requestId) {
        return streamStore.listScopes(executor, requestId);
    }

    /**
     * List Scopes in cluster from continuation token and limit it to number of elements specified by limit parameter.
     *
     * @param token continuation token
     * @param limit number of elements to return.
     * @param requestId request id
     * @return List of scopes.
     */
    public CompletableFuture<Pair<List<String>, String>> listScopes(final String token, final int limit, long requestId) {
        return streamStore.listScopes(token, limit, executor, requestId);
    }

    /**
     * Retrieve a scope.
     *
     * @param scopeName Name of Scope.
     * @param requestId request id
     * @return Scope if it exists.
     */
    public CompletableFuture<String> getScope(final String scopeName, long requestId) {
        Preconditions.checkNotNull(scopeName);
        OperationContext context = streamStore.createScopeContext(scopeName, requestId);

        return streamStore.getScopeConfiguration(scopeName, context, executor);
    }

    // Metrics reporting region
    private void reportCreateKVTableMetrics(String scope, String kvtName, int initialSegments, 
                                            CreateKeyValueTableStatus.Status status,
                                            Duration latency) {
        if (status.equals(CreateKeyValueTableStatus.Status.SUCCESS)) {
            StreamMetrics.getInstance().createKeyValueTable(scope, kvtName, initialSegments, latency);
        } else if (status.equals(CreateKeyValueTableStatus.Status.FAILURE)) {
            StreamMetrics.getInstance().createKeyValueTableFailed(scope, kvtName);
        }
    }

    public static void reportDeleteKVTableMetrics(String scope, String kvtName, DeleteKVTableStatus.Status status, Duration latency) {
        if (status.equals(DeleteKVTableStatus.Status.SUCCESS)) {
            StreamMetrics.getInstance().deleteKeyValueTable(scope, kvtName, latency);
        } else if (status.equals(DeleteKVTableStatus.Status.FAILURE)) {
            StreamMetrics.getInstance().deleteKeyValueTableFailed(scope, kvtName);
        }
    }

    private void reportCreateStreamMetrics(String scope, String streamName, int initialSegments, CreateStreamStatus.Status status,
                                           Duration latency) {
        if (status.equals(CreateStreamStatus.Status.SUCCESS)) {
            StreamMetrics.getInstance().createStream(scope, streamName, initialSegments, latency);
        } else if (status.equals(CreateStreamStatus.Status.FAILURE)) {
            StreamMetrics.getInstance().createStreamFailed(scope, streamName);
        }
    }

    private CreateScopeStatus reportCreateScopeMetrics(String scope, CreateScopeStatus status, Duration latency) {
        if (status.getStatus().equals(CreateScopeStatus.Status.SUCCESS)) {
            StreamMetrics.getInstance().createScope(latency);
        } else if (status.getStatus().equals(CreateScopeStatus.Status.FAILURE)) {
            StreamMetrics.getInstance().createScopeFailed(scope);
        }
        return status;
    }

    private void reportUpdateStreamMetrics(String scope, String streamName, UpdateStreamStatus.Status status, Duration latency) {
        if (status.equals(UpdateStreamStatus.Status.SUCCESS)) {
            StreamMetrics.getInstance().updateStream(scope, streamName, latency);
        } else if (status.equals(UpdateStreamStatus.Status.FAILURE)) {
            StreamMetrics.getInstance().updateStreamFailed(scope, streamName);
        }
    }

    private void reportCreateReaderGroupMetrics(String scope, String rgName, CreateReaderGroupResponse.Status status, 
                                                Duration latency) {
        if (status.equals(CreateReaderGroupResponse.Status.SUCCESS)) {
            StreamMetrics.getInstance().createReaderGroup(scope, rgName, latency);
        } else if (status.equals(CreateReaderGroupResponse.Status.FAILURE)) {
            StreamMetrics.getInstance().createReaderGroupFailed(scope, rgName);
        }
    }

    private void reportUpdateReaderGroupMetrics(String scope, String streamName, UpdateReaderGroupResponse.Status status, 
                                                Duration latency) {
        if (status.equals(UpdateReaderGroupResponse.Status.SUCCESS)) {
            StreamMetrics.getInstance().updateReaderGroup(scope, streamName, latency);
        } else if (status.equals(UpdateReaderGroupResponse.Status.FAILURE)) {
            StreamMetrics.getInstance().updateReaderGroupFailed(scope, streamName);
        }
    }

    public static void reportDeleteReaderGroupMetrics(String scope, String streamName, DeleteReaderGroupStatus.Status status,
                                                Duration latency) {
        if (status.equals(DeleteReaderGroupStatus.Status.SUCCESS)) {
            StreamMetrics.getInstance().deleteReaderGroup(scope, streamName, latency);
        } else if (status.equals(DeleteReaderGroupStatus.Status.FAILURE)) {
            StreamMetrics.getInstance().deleteReaderGroupFailed(scope, streamName);
        }
    }

    private void reportUpdateTruncationSCMetrics(String scope, String streamName, UpdateSubscriberStatus.Status status,
                                                 Duration latency) {
        if (status.equals(UpdateSubscriberStatus.Status.SUCCESS)) {
            StreamMetrics.getInstance().updateTruncationSC(scope, streamName, latency);
        } else if (status.equals(UpdateSubscriberStatus.Status.FAILURE)) {
            StreamMetrics.getInstance().updateTruncationSCFailed(scope, streamName);
        }
    }

    private void reportTruncateStreamMetrics(String scope, String streamName, UpdateStreamStatus.Status status, 
                                             Duration latency) {
        if (status.equals(UpdateStreamStatus.Status.SUCCESS)) {
            StreamMetrics.getInstance().truncateStream(scope, streamName, latency);
        } else if (status.equals(UpdateStreamStatus.Status.FAILURE)) {
            StreamMetrics.getInstance().truncateStreamFailed(scope, streamName);
        }
    }

    public static void reportSealStreamMetrics(String scope, String streamName, UpdateStreamStatus.Status status, Duration latency) {
        if (status.equals(UpdateStreamStatus.Status.SUCCESS)) {
            StreamMetrics.getInstance().sealStream(scope, streamName, latency);
        } else if (status.equals(UpdateStreamStatus.Status.FAILURE)) {
            StreamMetrics.getInstance().sealStreamFailed(scope, streamName);
        }
    }

    public static void reportDeleteStreamMetrics(String scope, String streamName, DeleteStreamStatus.Status status, Duration latency) {
        if (status.equals(DeleteStreamStatus.Status.SUCCESS)) {
            StreamMetrics.getInstance().deleteStream(scope, streamName, latency);
        } else if (status.equals(DeleteStreamStatus.Status.FAILURE)) {
            StreamMetrics.getInstance().deleteStreamFailed(scope, streamName);
        }
    }

    private DeleteScopeStatus reportDeleteScopeMetrics(String scope, DeleteScopeStatus status, Duration latency) {
        if (status.getStatus().equals(DeleteScopeStatus.Status.SUCCESS)) {
            StreamMetrics.getInstance().deleteScope(latency);
        } else if (status.getStatus().equals(DeleteScopeStatus.Status.FAILURE)) {
            StreamMetrics.getInstance().deleteScopeFailed(scope);
        }
        return status;
    }

    private void reportDeleteScopeRecursiveMetrics(String scope, DeleteScopeStatus.Status status, Duration latency) {
        if (status.equals(DeleteScopeStatus.Status.SUCCESS)) {
            StreamMetrics.getInstance().deleteScope(latency);
        } else if (status.equals(DeleteScopeStatus.Status.FAILURE)) {
            StreamMetrics.getInstance().deleteScopeRecursiveFailed(scope);
        }
    }

    public CompletableFuture<Controller.TimestampResponse> noteTimestampFromWriter(String scope, String stream, String writerId, 
                                                                                   long timestamp, Map<Long, Long> streamCut,
                                                                                   long requestId) {
        OperationContext context = streamStore.createStreamContext(scope, stream, requestId);

        return bucketStore.addStreamToBucketStore(BucketStore.ServiceType.WatermarkingService, scope, stream, executor)
                          .thenCompose(v -> streamStore.noteWriterMark(scope, stream, writerId, timestamp, streamCut, 
                                  context, executor))
                .thenApply(r -> {
                        Controller.TimestampResponse.Builder response = Controller.TimestampResponse.newBuilder();
                        switch (r) {
                            case SUCCESS:
                                response.setResult(Controller.TimestampResponse.Status.SUCCESS);
                                break;
                            case INVALID_TIME:
                                response.setResult(Controller.TimestampResponse.Status.INVALID_TIME);
                                break;
                            case INVALID_POSITION:
                                response.setResult(Controller.TimestampResponse.Status.INVALID_POSITION);
                                break;
                            default:
                                response.setResult(Controller.TimestampResponse.Status.INTERNAL_ERROR);
                                break;
                        }
                        return response.build();
                });
    }

    /**
     * Controller Service API to get controller to bucket mapping.
     *
     * @param serviceType Name of scope to be deleted.
     * @param requestId   request id
     * @return Controller to bucket mapping.
     */
    public CompletableFuture<Map<String, Set<Integer>>> getControllerToBucketMapping(final BucketType serviceType,
                                                                                     final long requestId) {
        BucketStore.ServiceType type = BucketStore.ServiceType.valueOf(serviceType.toString());
        return bucketStore.getBucketControllerMap(type).thenApply(mapping -> {
            log.info(requestId, "Successfully fetched controllers to bucket mapping for service type {}",
                    serviceType.toString());
            return mapping;
        });
    }

    public CompletableFuture<Controller.RemoveWriterResponse> removeWriter(String scope, String stream, String writer, 
                                                                           long requestId) {
        OperationContext context = streamStore.createStreamContext(scope, stream, requestId);

        return streamStore.shutdownWriter(scope, stream, writer, context, executor)
                .handle((r, e) -> {
                    Controller.RemoveWriterResponse.Builder response = Controller.RemoveWriterResponse.newBuilder();
                    if (e != null) {
                        if (Exceptions.unwrap(e) instanceof StoreException.DataNotFoundException) {
                            response.setResult(Controller.RemoveWriterResponse.Status.UNKNOWN_WRITER);
                        } else {
                            response.setResult(Controller.RemoveWriterResponse.Status.INTERNAL_ERROR);
                        }
                    } else {
                        response.setResult(Controller.RemoveWriterResponse.Status.SUCCESS);
                    }
                    return response.build();
                });
    }
    // End metrics reporting region
}
