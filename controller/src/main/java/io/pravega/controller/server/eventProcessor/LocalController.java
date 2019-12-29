/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.server.eventProcessor;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import io.pravega.client.byteStream.impl.BufferedByteStreamWriterImpl;
import io.pravega.client.byteStream.impl.ByteStreamReaderImpl;
import io.pravega.client.byteStream.impl.ByteStreamWriterImpl;
import io.pravega.client.security.auth.DelegationTokenProvider;
import io.pravega.client.security.auth.DelegationTokenProviderFactory;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.state.Revision;
import io.pravega.client.state.RevisionedStreamClient;
import io.pravega.client.state.SynchronizerConfig;
// import io.pravega.client.state.impl.RevisionImpl;
import io.pravega.client.stream.*;
import io.pravega.client.stream.impl.*;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.tracing.TagLogger;
import io.pravega.common.util.AsyncIterator;
import io.pravega.common.util.ContinuationTokenAsyncIterator;
import io.pravega.controller.server.ControllerService;
import io.pravega.controller.server.rpc.auth.GrpcAuthHelper;
import io.pravega.controller.stream.api.grpc.v1.Controller.ScaleResponse;
import io.pravega.controller.stream.api.grpc.v1.Controller.SegmentRange;
import io.pravega.shared.protocol.netty.PravegaNodeUri;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import io.pravega.shared.watermarks.Watermark;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.LoggerFactory;

public class LocalController implements Controller {
    private static final TagLogger log = new TagLogger(LoggerFactory.getLogger(LocalController.class));

    private static final int LIST_STREAM_IN_SCOPE_LIMIT = 1000;
    private ControllerService controller;
    private final String tokenSigningKey;
    private final boolean authorizationEnabled;

    public LocalController(ControllerService controller, boolean authorizationEnabled, String tokenSigningKey) {
        this.controller = controller;
        this.tokenSigningKey = tokenSigningKey;
        this.authorizationEnabled = authorizationEnabled;
    }

    @Override
    public CompletableFuture<String> getEvent(String routingKey, String scopeName, String streamName, Long segmentNumber) {
        log.error("-----------------getEvent--------------------------");
        ClientFactoryImpl clientFactory = new ClientFactoryImpl(scopeName, this);
        final Serializer<String> serializer = new JavaSerializer<>();
        final Random random = new Random();
        final Supplier<String> keyGenerator = () -> String.valueOf(random.nextInt());
        EventStreamReader<String> reader = clientFactory.createReader("readerId", "group", serializer,
                ReaderConfig.builder().build());
        CompletableFuture<String> ackFuture = new CompletableFuture<String>();
        String data = reader.readNextEvent(-1).getEvent();
        ackFuture.complete(data);
        return ackFuture;
    }

    @Override
    public CompletableFuture<Void> createEvent(String routingKey, String scopeName, String streamName, String message) {
        CompletableFuture<Void> ack = null;
        try {
            ClientFactoryImpl clientFactory = new ClientFactoryImpl(scopeName, this);
            final Serializer<String> serializer = new JavaSerializer<>();
            final Random random = new Random();
            final Supplier<String> keyGenerator = () -> String.valueOf(random.nextInt());
            EventStreamWriter<String> writer = clientFactory.createEventWriter(streamName, serializer,
                    EventWriterConfig.builder().build());
            ack = writer.writeEvent(keyGenerator.get(), message);
            // ack.get();
            log.info("event created for scope:{} stream:{}", scopeName, streamName);
        } catch (Exception e) {
            log.error("Exception:", e);
            e.printStackTrace();
        }
        return ack;
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
                controller.listStreams(scopeName, token, LIST_STREAM_IN_SCOPE_LIMIT)
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

    private StreamSegments getStreamSegments(List<SegmentRange> ranges) {
        NavigableMap<Double, SegmentWithRange> rangeMap = new TreeMap<>();
        for (SegmentRange r : ranges) {
            rangeMap.put(r.getMaxKey(), new SegmentWithRange(ModelHelper.encode(r.getSegmentId()), r.getMinKey(), r.getMaxKey()));
        }
        return new StreamSegments(rangeMap, retrieveDelegationToken());
    }

    @Override
    public CompletableFuture<TxnSegments> createTransaction(Stream stream, long lease) {
        return controller
                .createTransaction(stream.getScope(), stream.getStreamName(), lease)
                .thenApply(pair -> new TxnSegments(getStreamSegments(pair.getRight()), pair.getKey()));
    }

    @Override
    public CompletableFuture<Transaction.PingStatus> pingTransaction(Stream stream, UUID txId, long lease) {
        return controller.pingTransaction(stream.getScope(), stream.getStreamName(), ModelHelper.decode(txId), lease)
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
                .commitTransaction(stream.getScope(), stream.getStreamName(), ModelHelper.decode(txnId), writerId, time)
                .thenApply(x -> null);
    }

    @Override
    public CompletableFuture<Void> abortTransaction(Stream stream, UUID txId) {
        return controller
                .abortTransaction(stream.getScope(), stream.getStreamName(), ModelHelper.decode(txId))
                .thenApply(x -> null);
    }

    @Override
    public CompletableFuture<Transaction.Status> checkTransactionStatus(Stream stream, UUID txnId) {
        return controller.checkTransactionStatus(stream.getScope(), stream.getStreamName(), ModelHelper.decode(txnId))
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
    public CompletableFuture<String> getOrRefreshDelegationTokenFor(String scope, String streamName) {
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
}
