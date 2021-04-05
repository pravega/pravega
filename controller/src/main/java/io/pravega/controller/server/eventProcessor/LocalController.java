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
package io.pravega.controller.server.eventProcessor;

import com.google.common.base.Preconditions;
import io.pravega.client.admin.KeyValueTableInfo;
import io.pravega.client.control.impl.ReaderGroupConfigRejectedException;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.PingFailedException;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.StreamCut;
import io.pravega.client.stream.Transaction;
import io.pravega.client.control.impl.CancellableRequest;
import io.pravega.client.control.impl.Controller;
import io.pravega.client.control.impl.ControllerFailureException;
import io.pravega.client.control.impl.ModelHelper;
import io.pravega.client.stream.impl.SegmentWithRange;
import io.pravega.client.stream.impl.StreamImpl;
import io.pravega.client.stream.impl.StreamSegmentSuccessors;
import io.pravega.client.stream.impl.StreamSegments;
import io.pravega.client.stream.impl.StreamSegmentsWithPredecessors;
import io.pravega.client.stream.impl.TxnSegments;
import io.pravega.client.stream.impl.WriterPosition;
import io.pravega.client.stream.impl.ReaderGroupNotFoundException;
import io.pravega.client.tables.KeyValueTableConfiguration;
import io.pravega.client.tables.impl.KeyValueTableSegments;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.controller.task.Stream.StreamMetadataTasks;
import io.pravega.shared.NameUtils;
import io.pravega.shared.security.auth.AccessOperation;
import io.pravega.common.util.AsyncIterator;
import io.pravega.common.util.ContinuationTokenAsyncIterator;
import io.pravega.controller.server.ControllerService;
import io.pravega.controller.server.security.auth.GrpcAuthHelper;
import io.pravega.controller.store.stream.StoreException;
import io.pravega.controller.stream.api.grpc.v1.Controller.ScaleResponse;
import io.pravega.controller.stream.api.grpc.v1.Controller.SegmentRange;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

@Slf4j
public class LocalController implements Controller {

    private static final int PAGE_LIMIT = 1000;
    private ControllerService controller;
    private final String tokenSigningKey;
    private final boolean authorizationEnabled;

    public LocalController(ControllerService controller, boolean authorizationEnabled, String tokenSigningKey) {
        this.controller = controller;
        this.tokenSigningKey = tokenSigningKey;
        this.authorizationEnabled = authorizationEnabled;
    }

    @Override
    public CompletableFuture<Boolean> checkScopeExists(String scopeName) {
        return Futures.exceptionallyExpecting(this.controller.getScope(scopeName).thenApply(v -> true),
                e -> Exceptions.unwrap(e) instanceof StoreException.DataNotFoundException, false);
    }

    @Override
    public AsyncIterator<String> listScopes() {
        final Function<String, CompletableFuture<Map.Entry<String, Collection<String>>>> function = token ->
                controller.listScopes(token, PAGE_LIMIT)
                          .thenApply(result -> new AbstractMap.SimpleEntry<>(result.getValue(), result.getKey()));

        return new ContinuationTokenAsyncIterator<>(function, "");
    }

    @Override
    public CompletableFuture<Boolean> checkStreamExists(String scopeName, String streamName) {
        return Futures.exceptionallyExpecting(this.controller.getStream(scopeName, streamName).thenApply(v -> true),
                e -> Exceptions.unwrap(e) instanceof StoreException.DataNotFoundException, false);
    }

    @Override
    public CompletableFuture<Boolean> createScope(final String scopeName) {
        return this.controller.createScope(scopeName).thenApply(x -> {
            switch (x.getStatus()) {
            case FAILURE:
                throw new ControllerFailureException("Failed to create scope: " + scopeName);
            case INVALID_SCOPE_NAME:
                throw new IllegalArgumentException("Illegal scope name: " + scopeName);
            case SCOPE_EXISTS:
                return false;
            case SUCCESS:
                return true;
            default:
                throw new ControllerFailureException("Unknown return status creating scope " + scopeName + " "
                        + x.getStatus());
            }
        });
    }

    @Override
    public AsyncIterator<Stream> listStreams(String scopeName) {
        final Function<String, CompletableFuture<Map.Entry<String, Collection<Stream>>>> function = token ->
                controller.listStreams(scopeName, token, PAGE_LIMIT)
                          .thenApply(result -> {
                              List<Stream> asStreamList = result.getKey().stream().map(m -> new StreamImpl(scopeName, m)).collect(Collectors.toList());
                              return new AbstractMap.SimpleEntry<>(result.getValue(), asStreamList);
                          });

        return new ContinuationTokenAsyncIterator<>(function, "");
    }

    @Override
    public CompletableFuture<Boolean> deleteScope(String scopeName) {
        return this.controller.deleteScope(scopeName).thenApply(x -> {
            switch (x.getStatus()) {
            case FAILURE:
                throw new ControllerFailureException("Failed to delete scope: " + scopeName);
            case SCOPE_NOT_EMPTY:
                throw new IllegalStateException("Scope " + scopeName + " is not empty.");
            case SCOPE_NOT_FOUND:
                return false;
            case SUCCESS:
                return true;
            default:
                throw new ControllerFailureException("Unknown return status deleting scope " + scopeName
                                                     + " " + x.getStatus());
            }
        });
    }

    @Override
    public CompletableFuture<Boolean> createStream(String scope, String streamName, final StreamConfiguration streamConfig) {
        return this.controller.createStream(scope, streamName, streamConfig, System.currentTimeMillis()).thenApply(x -> {
            switch (x.getStatus()) {
            case FAILURE:
                throw new ControllerFailureException("Failed to createing stream: " + streamConfig);
            case INVALID_STREAM_NAME:
                throw new IllegalArgumentException("Illegal stream name: " + streamConfig);
            case SCOPE_NOT_FOUND:
                throw new IllegalArgumentException("Scope does not exist: " + streamConfig);
            case STREAM_EXISTS:
                return false;
            case SUCCESS:
                return true;
            default:
                throw new ControllerFailureException("Unknown return status creating stream " + streamConfig
                                                     + " " + x.getStatus());
            }
        });
    }

    @Override
    public CompletableFuture<Boolean> updateStream(String scope, String streamName, final StreamConfiguration streamConfig) {
        return this.controller.updateStream(scope, streamName, streamConfig).thenApply(x -> {
            switch (x.getStatus()) {
            case FAILURE:
                throw new ControllerFailureException("Failed to update stream: " + streamConfig);
            case SCOPE_NOT_FOUND:
                throw new IllegalArgumentException("Scope does not exist: " + streamConfig);
            case STREAM_NOT_FOUND:
                throw new IllegalArgumentException("Stream does not exist: " + streamConfig);
            case SUCCESS:
                return true;
            default:
                throw new ControllerFailureException("Unknown return status updating stream " + streamConfig
                                                     + " " + x.getStatus());
            }
        });
    }

    @Override
    public CompletableFuture<ReaderGroupConfig> createReaderGroup(String scopeName, String rgName, ReaderGroupConfig config) {
        StreamMetadataTasks streamMetadataTasks = controller.getStreamMetadataTasks();
        return streamMetadataTasks.createReaderGroupInternal(scopeName, rgName, config, System.currentTimeMillis())
                .thenApply(x -> {
            final String scopedRGName = NameUtils.getScopedReaderGroupName(scopeName, rgName);
            switch (x.getStatus()) {
                case FAILURE:
                    throw new ControllerFailureException("Failed to create ReaderGroup: " + scopedRGName);
                case INVALID_RG_NAME:
                    throw new IllegalArgumentException("Illegal ReaderGroup name: " + rgName);
                case SCOPE_NOT_FOUND:
                    throw new IllegalArgumentException("Scope does not exist: " + scopeName);
                case SUCCESS:
                    return ModelHelper.encode(x.getConfig());
                default:
                    throw new ControllerFailureException("Unknown return status creating ReaderGroup " + scopedRGName
                            + " " + x);
            }
        });
    }

    @Override
    public CompletableFuture<Long> updateReaderGroup(String scopeName, String rgName, ReaderGroupConfig config) {
        return this.controller.updateReaderGroup(scopeName, rgName, config).thenApply(x -> {
            final String scopedRGName = NameUtils.getScopedReaderGroupName(scopeName, rgName);
            switch (x.getStatus()) {
                case FAILURE:
                    throw new ControllerFailureException("Failed to create ReaderGroup: " + scopedRGName);
                case INVALID_CONFIG:
                    throw new ReaderGroupConfigRejectedException("Invalid Reader Group Config: " + config.toString());
                case RG_NOT_FOUND:
                    throw new ReaderGroupNotFoundException("Scope does not exist: " + scopedRGName);
                case SUCCESS:
                    return x.getGeneration();
                default:
                    throw new ControllerFailureException("Unknown return status creating ReaderGroup " + scopedRGName
                            + " " + x.getStatus());
            }
        });
    }

    @Override
    public CompletableFuture<ReaderGroupConfig> getReaderGroupConfig(String scopeName, String rgName) {
        return this.controller.getReaderGroupConfig(scopeName, rgName).thenApply(x -> {
           final String scopedRGName = NameUtils.getScopedReaderGroupName(scopeName, rgName);
           switch (x.getStatus()) {
                case FAILURE:
                    throw new ControllerFailureException("Failed to get Config for ReaderGroup: " + scopedRGName);
                case RG_NOT_FOUND:
                    throw new ReaderGroupNotFoundException("Could not find Reader Group: " + scopedRGName);
                case SUCCESS:
                    return ModelHelper.encode(x.getConfig());
                default:
                    throw new ControllerFailureException("Unknown return status getting config for ReaderGroup " + scopedRGName
                            + " " + x.getStatus());
            }
        });
    }

    @Override
    public CompletableFuture<Boolean> deleteReaderGroup(final String scopeName, final String rgName,
                                                        final UUID readerGroupId) {
        return this.controller.deleteReaderGroup(scopeName, rgName, readerGroupId.toString()).thenApply(x -> {
            final String scopedRGName = NameUtils.getScopedReaderGroupName(scopeName, rgName);
            switch (x.getStatus()) {
                case FAILURE:
                    throw new ControllerFailureException("Failed to create ReaderGroup: " + scopedRGName);
                case RG_NOT_FOUND:
                    throw new ReaderGroupNotFoundException("Reader group not found: " + scopedRGName);
                case SUCCESS:
                    return true;
                default:
                    throw new ControllerFailureException("Unknown return status creating ReaderGroup " + scopedRGName
                            + " " + x.getStatus());
            }
        });
    }

    @Override
    public CompletableFuture<List<String>> listSubscribers(final String scope, final String streamName) {
        return this.controller.listSubscribers(scope, streamName).thenApply(x -> {
            switch (x.getStatus()) {
                case FAILURE:
                    throw new ControllerFailureException("Failed to listSubscribers for stream: " + scope + "/" + streamName);
                case STREAM_NOT_FOUND:
                    throw new IllegalArgumentException("Stream does not exist: " + streamName);
                case SUCCESS:
                    return x.getSubscribersList().stream().collect(Collectors.toList());
                default:
                    throw new ControllerFailureException("Unknown return status for listSubscribers on stream " + scope + "/" + streamName + " " + x.getStatus());
            }
        });
    }

    @Override
    public CompletableFuture<Boolean> updateSubscriberStreamCut(final String scope, final String streamName, final String subscriber,
                                                                final UUID readerGroupId, final long generation, final StreamCut streamCut) {
        return this.controller.updateSubscriberStreamCut(scope, streamName, subscriber, readerGroupId.toString(), generation,
                ModelHelper.getStreamCutMap(streamCut)).thenApply(x -> {
            switch (x.getStatus()) {
                case FAILURE:
                    throw new ControllerFailureException("Failed to update streamcut: " + scope + "/" + streamName);
                case STREAM_NOT_FOUND:
                    throw new IllegalArgumentException("Stream does not exist: " + streamName);
                case SUBSCRIBER_NOT_FOUND:
                    throw new IllegalArgumentException("Subscriber does not exist: " + subscriber);
                case GENERATION_MISMATCH:
                    throw new IllegalArgumentException("Subscriber generation does not match: " + subscriber);
                case SUCCESS:
                    return true;
                default:
                    throw new ControllerFailureException("Unknown return status updating truncation streamcut for subscriber "
                                                 + subscriber + ", on stream " + scope + "/" + streamName + " " + x.getStatus());
            }
        });
    }

        @Override
    public CompletableFuture<Boolean> truncateStream(final String scope, final String stream, final StreamCut streamCut) {
        final Map<Long, Long> segmentToOffsetMap = streamCut.asImpl().getPositions().entrySet().stream()
                                                               .collect(Collectors.toMap(e -> e.getKey().getSegmentId(),
                                                                       Map.Entry::getValue));
        return truncateStream(scope, stream, segmentToOffsetMap);
    }

    public CompletableFuture<Boolean> truncateStream(final String scope, final String stream, final Map<Long, Long> streamCut) {
        return this.controller.truncateStream(scope, stream, streamCut).thenApply(x -> {
            switch (x.getStatus()) {
            case FAILURE:
                throw new ControllerFailureException("Failed to truncate stream: " + stream);
            case SCOPE_NOT_FOUND:
                throw new IllegalArgumentException("Scope does not exist: " + scope);
            case STREAM_NOT_FOUND:
                throw new IllegalArgumentException("Stream does not exist: " + stream);
            case SUCCESS:
                return true;
            default:
                throw new ControllerFailureException("Unknown return status truncating stream " + stream
                                                     + " " + x.getStatus());
            }
        });
    }

    @Override
    public CompletableFuture<Boolean> sealStream(String scope, String streamName) {
        return this.controller.sealStream(scope, streamName).thenApply(x -> {
            switch (x.getStatus()) {
            case FAILURE:
                throw new ControllerFailureException("Failed to seal stream: " + streamName);
            case SCOPE_NOT_FOUND:
                throw new IllegalArgumentException("Scope does not exist: " + scope);
            case STREAM_NOT_FOUND:
                throw new IllegalArgumentException("Stream does not exist: " + streamName);
            case SUCCESS:
                return true;
            default:
                throw new ControllerFailureException("Unknown return status scealing stream " + streamName
                                                     + " " + x.getStatus());
            }
        });
    }

    @Override
    public CompletableFuture<Boolean> deleteStream(final String scope, final String streamName) {
        return this.controller.deleteStream(scope, streamName).thenApply(x -> {
            switch (x.getStatus()) {
            case FAILURE:
                throw new ControllerFailureException("Failed to delete stream: " + streamName);
            case STREAM_NOT_FOUND:
                return false;
            case STREAM_NOT_SEALED:
                throw new IllegalArgumentException("Stream is not sealed: " + streamName);
            case SUCCESS:
                return true;
            default:
                throw new ControllerFailureException("Unknown return status deleting stream " + streamName + " "
                        + x.getStatus());
            }
        });
    }

    @Override
    public CancellableRequest<Boolean> scaleStream(final Stream stream, final List<Long> sealedSegments,
                                                   final Map<Double, Double> newKeyRanges,
                                                   final ScheduledExecutorService executor) {
        CancellableRequest<Boolean> cancellableRequest = new CancellableRequest<>();

        startScaleInternal(stream, sealedSegments, newKeyRanges)
                .whenComplete((startScaleResponse, e) -> {
                    if (e != null) {
                        cancellableRequest.start(() -> Futures.failedFuture(e), any -> true, executor);
                    } else {
                        final boolean started = startScaleResponse.getStatus().equals(ScaleResponse.ScaleStreamStatus.STARTED);

                        cancellableRequest.start(() -> {
                            if (started) {
                                return checkScaleStatus(stream, startScaleResponse.getEpoch());
                            } else {
                                return CompletableFuture.completedFuture(false);
                            }
                        }, isDone -> !started || isDone, executor);
                    }
                });

        return cancellableRequest;
    }

    @Override
    public CompletableFuture<Boolean> startScale(final Stream stream,
                                                  final List<Long> sealedSegments,
                                                  final Map<Double, Double> newKeyRanges) {
        return startScaleInternal(stream, sealedSegments, newKeyRanges)
                .thenApply(x -> {
                    switch (x.getStatus()) {
                    case FAILURE:
                        throw new ControllerFailureException("Failed to scale stream: " + stream);
                    case PRECONDITION_FAILED:
                        return false;
                    case STARTED:
                        return true;
                    default:
                        throw new ControllerFailureException("Unknown return status scaling stream "
                                + stream + " " + x.getStatus());
                    }
                });
    }

    @Override
    public CompletableFuture<Boolean> checkScaleStatus(final Stream stream, final int epoch) {
        return this.controller.checkScale(stream.getScope(), stream.getStreamName(), epoch)
                .thenApply(response -> {
                    switch (response.getStatus()) {
                        case IN_PROGRESS:
                            return false;
                        case SUCCESS:
                            return true;
                        case INVALID_INPUT:
                            throw new ControllerFailureException("invalid input");
                        case INTERNAL_ERROR:
                        default:
                            throw new ControllerFailureException("unknown error");
                    }
                });
    }

    private CompletableFuture<ScaleResponse> startScaleInternal(final Stream stream, final List<Long> sealedSegments,
                                                                final Map<Double, Double> newKeyRanges) {
        Preconditions.checkNotNull(stream, "stream");
        Preconditions.checkNotNull(sealedSegments, "sealedSegments");
        Preconditions.checkNotNull(newKeyRanges, "newKeyRanges");

        return this.controller.scale(stream.getScope(),
                stream.getStreamName(),
                sealedSegments,
                newKeyRanges,
                System.currentTimeMillis());
    }

    @Override
    public CompletableFuture<StreamSegments> getCurrentSegments(final String scope, final String streamName) {
        return controller.getCurrentSegments(scope, streamName)
                .thenApply(this::getStreamSegments);
    }

    @Override
    public CompletableFuture<StreamSegments> getEpochSegments(String scope, String streamName, int epoch) {
        return controller.getEpochSegments(scope, streamName, epoch)
                         .thenApply(this::getStreamSegments);
    }

    private StreamSegments getStreamSegments(List<SegmentRange> ranges) {
        return new StreamSegments(getRangeMap(ranges));
    }

    @Override
    public CompletableFuture<TxnSegments> createTransaction(Stream stream, long lease) {
        return controller
                .createTransaction(stream.getScope(), stream.getStreamName(), lease)
                .thenApply(pair -> new TxnSegments(getStreamSegments(pair.getRight()), pair.getKey()));
    }

    @Override
    public CompletableFuture<Transaction.PingStatus> pingTransaction(Stream stream, UUID txId, long lease) {
        return controller.pingTransaction(stream.getScope(), stream.getStreamName(), txId, lease)
                         .thenApply(status -> {
                             try {
                                 return ModelHelper.encode(status.getStatus(), stream + " " + txId);
                             } catch (PingFailedException ex) {
                                 throw new CompletionException(ex);
                             }
                         });
    }

    @Override
    public CompletableFuture<Void> commitTransaction(Stream stream, final String writerId, final Long timestamp, UUID txnId) {
        long time = Optional.ofNullable(timestamp).orElse(Long.MIN_VALUE);
        return controller
                .commitTransaction(stream.getScope(), stream.getStreamName(), txnId, writerId, time)
                .thenApply(x -> null);
    }

    @Override
    public CompletableFuture<Void> abortTransaction(Stream stream, UUID txnId) {
        return controller
                .abortTransaction(stream.getScope(), stream.getStreamName(), txnId)
                .thenApply(x -> null);
    }

    @Override
    public CompletableFuture<Transaction.Status> checkTransactionStatus(Stream stream, UUID txnId) {
        return controller.checkTransactionStatus(stream.getScope(), stream.getStreamName(), txnId)
                .thenApply(status -> ModelHelper.encode(status.getState(), stream + " " + txnId));
    }

    @Override
    public CompletableFuture<Map<Segment, Long>> getSegmentsAtTime(Stream stream, long timestamp) {
        return controller.getSegmentsAtHead(stream.getScope(), stream.getStreamName()).thenApply(segments -> {
            return segments.entrySet()
                           .stream()
                           .collect(Collectors.toMap(entry -> ModelHelper.encode(entry.getKey()),
                                                     entry -> entry.getValue()));
        });
    }

    @Override
    public CompletableFuture<StreamSegmentsWithPredecessors> getSuccessors(Segment segment) {
        return controller.getSegmentsImmediatelyFollowing(ModelHelper.decode(segment))
                .thenApply(x -> {
                    Map<SegmentWithRange, List<Long>> map = new HashMap<>();
                    x.forEach((segmentId, list) -> map.put(ModelHelper.encode(segmentId), list));
                    return new StreamSegmentsWithPredecessors(map, retrieveDelegationToken());
                });
    }

    @Override
    public CompletableFuture<StreamSegmentSuccessors> getSuccessors(StreamCut from) {
        return getSegments(from, StreamCut.UNBOUNDED);
    }

    @Override
    public CompletableFuture<StreamSegmentSuccessors> getSegments(StreamCut fromStreamCut, StreamCut toStreamCut) {
        Stream stream = fromStreamCut.asImpl().getStream();
        return controller.getSegmentsBetweenStreamCuts(ModelHelper.decode(stream.getScope(), stream.getStreamName(),
                getStreamCutMap(fromStreamCut), getStreamCutMap(toStreamCut)))
                .thenApply(segments -> ModelHelper.createStreamCutRangeResponse(stream.getScope(), stream.getStreamName(),
                        segments.stream().map(x -> ModelHelper.createSegmentId(stream.getScope(), stream.getStreamName(), x.segmentId()))
                                .collect(Collectors.toList()), retrieveDelegationToken()))
                .thenApply(response -> new StreamSegmentSuccessors(response.getSegmentsList().stream().map(ModelHelper::encode).collect(Collectors.toSet()),
                response.getDelegationToken()));
    }

    @Override
    public CompletableFuture<PravegaNodeUri> getEndpointForSegment(String qualifiedSegmentName) {
        Segment segment = Segment.fromScopedName(qualifiedSegmentName);
            return controller.getURI(ModelHelper.createSegmentId(segment.getScope(), segment.getStreamName(),
                    segment.getSegmentId())).thenApply(ModelHelper::encode);
    }

    @Override
    public CompletableFuture<Boolean> isSegmentOpen(Segment segment) {
        return controller.isSegmentValid(segment.getScope(), segment.getStreamName(), segment.getSegmentId());
    }

    @Override
    public void close() {
    }

    public String retrieveDelegationToken() {
        if (authorizationEnabled) {
            return GrpcAuthHelper.retrieveMasterToken(tokenSigningKey);
        } else {
            return StringUtils.EMPTY;
        }
    }

    @Override
    public CompletableFuture<String> getOrRefreshDelegationTokenFor(String scope, String streamName, AccessOperation accessOperation) {
        String retVal = "";
        if (authorizationEnabled) {
            retVal = GrpcAuthHelper.retrieveMasterToken(tokenSigningKey);
        }
        return CompletableFuture.completedFuture(retVal);
    }

    private Map<Long, Long> getStreamCutMap(StreamCut streamCut) {
        if (streamCut.equals(StreamCut.UNBOUNDED)) {
            return Collections.emptyMap();
        }
        return streamCut.asImpl().getPositions().entrySet()
                .stream().collect(Collectors.toMap(x -> x.getKey().getSegmentId(), Map.Entry::getValue));
    }

    @Override
    public CompletableFuture<Void> noteTimestampFromWriter(String writer, Stream stream, long timestamp, WriterPosition lastWrittenPosition) {
        Map<Long, Long> map = ModelHelper.createStreamCut(stream, lastWrittenPosition).getCutMap();
        return Futures.toVoid(controller.noteTimestampFromWriter(stream.getScope(), stream.getStreamName(), writer, timestamp, map));
    }

    @Override
    public CompletableFuture<Void> removeWriter(String writerId, Stream stream) {
        return Futures.toVoid(controller.removeWriter(stream.getScope(), stream.getStreamName(), writerId));
    }

    //region KeyValueTables

    @Override
    public CompletableFuture<Boolean> createKeyValueTable(String scope, String kvtName, KeyValueTableConfiguration kvtConfig) {
        return this.controller.createKeyValueTable(scope, kvtName, kvtConfig, System.currentTimeMillis()).thenApply(x -> {
            switch (x.getStatus()) {
                case FAILURE:
                    throw new ControllerFailureException("Failed to create KeyValueTable: " + kvtName);
                case INVALID_TABLE_NAME:
                    throw new IllegalArgumentException("Illegal KeyValueTable name: " + kvtName);
                case SCOPE_NOT_FOUND:
                    throw new IllegalArgumentException("Scope does not exist: " + scope);
                case TABLE_EXISTS:
                    return false;
                case SUCCESS:
                    return true;
                default:
                    throw new ControllerFailureException("Unknown return status creating kvtable " + kvtName
                            + " " + x.getStatus());
            }
        });
    }
    
    @Override
    public AsyncIterator<KeyValueTableInfo> listKeyValueTables(String scopeName) {
        final Function<String, CompletableFuture<Map.Entry<String, Collection<KeyValueTableInfo>>>> function = token ->
                controller.listKeyValueTables(scopeName, token, PAGE_LIMIT)
                        .thenApply(result -> {
                            List<KeyValueTableInfo> kvTablesList = result.getLeft().stream().map(kvt -> new KeyValueTableInfo(scopeName, kvt))
                                    .collect(Collectors.toList());

                            return new AbstractMap.SimpleEntry<>(result.getValue(), kvTablesList);
                        });

        return new ContinuationTokenAsyncIterator<>(function, "");
    }

    @Override
    public CompletableFuture<Boolean> deleteKeyValueTable(String scope, String kvtName) {
        return this.controller.deleteKeyValueTable(scope, kvtName).thenApply(x -> {
            switch (x.getStatus()) {
                case FAILURE:
                    throw new ControllerFailureException("Failed to delete KeyValueTable: " + kvtName);
                case TABLE_NOT_FOUND:
                    return false;
                case SUCCESS:
                    return true;
                default:
                    throw new ControllerFailureException("Unknown return status deleting KeyValueTable " + kvtName + " "
                            + x.getStatus());
            }
        });
    }

    @Override
    public CompletableFuture<KeyValueTableSegments> getCurrentSegmentsForKeyValueTable(String scope, String kvtName) {
        return controller.getCurrentSegmentsKeyValueTable(scope, kvtName)
                .thenApply(this::getKeyValueTableSegments);
    }

    private KeyValueTableSegments getKeyValueTableSegments(List<SegmentRange> ranges) {
        return new KeyValueTableSegments(getRangeMap(ranges));
    }

    private NavigableMap<Double, SegmentWithRange> getRangeMap(List<SegmentRange> ranges) {
        NavigableMap<Double, SegmentWithRange> rangeMap = new TreeMap<>();
        for (SegmentRange r : ranges) {
            rangeMap.put(r.getMaxKey(), new SegmentWithRange(ModelHelper.encode(r.getSegmentId()), r.getMinKey(), r.getMaxKey()));
        }
        return rangeMap;
    }
    //endregion
}
