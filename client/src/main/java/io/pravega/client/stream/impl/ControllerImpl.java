/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.stream.impl;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.util.RoundRobinLoadBalancerFactory;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.InvalidStreamException;
import io.pravega.client.stream.PingFailedException;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.Transaction;
import io.pravega.client.stream.TxnFailedException;
import io.pravega.common.Exceptions;
import io.pravega.common.LoggerHelpers;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.Retry;
import io.pravega.controller.stream.api.grpc.v1.Controller.CreateScopeStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.CreateStreamStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.CreateTxnRequest;
import io.pravega.controller.stream.api.grpc.v1.Controller.CreateTxnResponse;
import io.pravega.controller.stream.api.grpc.v1.Controller.DeleteScopeStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.DeleteStreamStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.GetSegmentsRequest;
import io.pravega.controller.stream.api.grpc.v1.Controller.NodeUri;
import io.pravega.controller.stream.api.grpc.v1.Controller.PingTxnRequest;
import io.pravega.controller.stream.api.grpc.v1.Controller.PingTxnStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.ScaleRequest;
import io.pravega.controller.stream.api.grpc.v1.Controller.ScaleResponse;
import io.pravega.controller.stream.api.grpc.v1.Controller.ScaleStatusRequest;
import io.pravega.controller.stream.api.grpc.v1.Controller.ScaleStatusResponse;
import io.pravega.controller.stream.api.grpc.v1.Controller.ScopeInfo;
import io.pravega.controller.stream.api.grpc.v1.Controller.SegmentRange;
import io.pravega.controller.stream.api.grpc.v1.Controller.SegmentRanges;
import io.pravega.controller.stream.api.grpc.v1.Controller.SegmentValidityResponse;
import io.pravega.controller.stream.api.grpc.v1.Controller.SegmentsAtTime;
import io.pravega.controller.stream.api.grpc.v1.Controller.StreamInfo;
import io.pravega.controller.stream.api.grpc.v1.Controller.SuccessorResponse;
import io.pravega.controller.stream.api.grpc.v1.Controller.TxnRequest;
import io.pravega.controller.stream.api.grpc.v1.Controller.TxnState;
import io.pravega.controller.stream.api.grpc.v1.Controller.TxnStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.UpdateStreamStatus;
import io.pravega.controller.stream.api.grpc.v1.ControllerServiceGrpc;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import java.util.Collection;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.net.URI;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static io.pravega.common.concurrent.Futures.getAndHandleExceptions;

/**
 * RPC based client implementation of Stream Controller V1 API.
 */
@Slf4j
public class ControllerImpl implements Controller {

    // The default keepalive interval for the gRPC transport to ensure long running RPCs are tested for connectivity.
    // This value should be greater than the permissible value configured at the server which is by default 5 minutes.
    private static final long DEFAULT_KEEPALIVE_TIME_MINUTES = 6;

    // The internal retry object to handle RPC failures.
    private final Retry.RetryAndThrowExceptionally<StatusRuntimeException, Exception> retryConfig;

    // The executor supplied by the appication to handle internal retries.
    private final ScheduledExecutorService executor;

    // Flag to indicate if the client is closed.
    private final AtomicBoolean closed = new AtomicBoolean(false);

    // The gRPC client for the Controller Service.
    private final ControllerServiceGrpc.ControllerServiceStub client;

    // io.grpc.Channel used by the grpc client for Controller Service.
    private final ManagedChannel channel;

    /**
     * Creates a new instance of the Controller client class.
     *
     * @param controllerURI The controller rpc URI. This can be of 2 types
     *                      1. tcp://ip1:port1,ip2:port2,...
     *                          This is used if the controller endpoints are static and can be directly accessed.
     *                      2. pravega://ip1:port1,ip2:port2,...
     *                          This is used to autodiscovery the controller endpoints from an initial controller list.
     * @param config        The configuration for this client implementation.
     * @param executor      The executor service to be used for handling retries.
     */
    public ControllerImpl(final URI controllerURI, final ControllerImplConfig config,
                          final ScheduledExecutorService executor) {
        this(NettyChannelBuilder.forTarget(controllerURI.toString())
                .nameResolverFactory(new ControllerResolverFactory())
                .loadBalancerFactory(RoundRobinLoadBalancerFactory.getInstance())
                .keepAliveTime(DEFAULT_KEEPALIVE_TIME_MINUTES, TimeUnit.MINUTES)
                .usePlaintext(true), config, executor);
        log.info("Controller client connecting to server at {}", controllerURI.getAuthority());
    }

    /**
     * Creates a new instance of the Controller client class.
     *
     * @param channelBuilder The channel builder to connect to the service instance.
     * @param config         The configuration for this client implementation.
     * @param executor       The executor service to be used internally.
     */
    @VisibleForTesting
    public ControllerImpl(ManagedChannelBuilder<?> channelBuilder, final ControllerImplConfig config,
                          final ScheduledExecutorService executor) {
        Preconditions.checkNotNull(channelBuilder, "channelBuilder");
        this.executor = executor;
        this.retryConfig = Retry.withExpBackoff(config.getInitialBackoffMillis(), config.getBackoffMultiple(),
                config.getRetryAttempts(), config.getMaxBackoffMillis())
                .retryingOn(StatusRuntimeException.class)
                .throwingOn(Exception.class);

        // Create Async RPC client.
        this.channel = channelBuilder.build();
        this.client = ControllerServiceGrpc.newStub(this.channel);
    }

    @Override
    public CompletableFuture<Boolean> createScope(final String scopeName) {
        Exceptions.checkNotClosed(closed.get(), this);
        long traceId = LoggerHelpers.traceEnter(log, "createScope", scopeName);

        final CompletableFuture<CreateScopeStatus> result = this.retryConfig.runAsync(() -> {
                RPCAsyncCallback<CreateScopeStatus> callback = new RPCAsyncCallback<>();
                client.createScope(ScopeInfo.newBuilder().setScope(scopeName).build(), callback);
                return callback.getFuture();
        }, this.executor);
        return result.thenApply(x -> {
                switch (x.getStatus()) {
                case FAILURE:
                    log.warn("Failed to create scope: {}", scopeName);
                    throw new ControllerFailureException("Failed to create scope: " + scopeName);
                case INVALID_SCOPE_NAME:
                    log.warn("Illegal scope name: {}", scopeName);
                    throw new IllegalArgumentException("Illegal scope name: " + scopeName);
                case SCOPE_EXISTS:
                    log.warn("Scope already exists: {}", scopeName);
                    return false;
                case SUCCESS:
                    log.info("Scope created successfully: {}", scopeName);
                    return true;
                case UNRECOGNIZED:
                default:
                    throw new ControllerFailureException("Unknown return status creating scope " + scopeName
                                                         + " " + x.getStatus());
                }
            }).whenComplete((x, e) -> {
                if (e != null) {
                    log.warn("createScope failed: ", e);
                }
                LoggerHelpers.traceLeave(log, "createScope", traceId);
            });
    }

    @Override
    public CompletableFuture<Boolean> deleteScope(String scopeName) {
        Exceptions.checkNotClosed(closed.get(), this);
        long traceId = LoggerHelpers.traceEnter(log, "deleteScope", scopeName);

        final CompletableFuture<DeleteScopeStatus> result = this.retryConfig.runAsync(() -> {
            RPCAsyncCallback<DeleteScopeStatus> callback = new RPCAsyncCallback<>();
            client.deleteScope(ScopeInfo.newBuilder().setScope(scopeName).build(), callback);
            return callback.getFuture();
        }, this.executor);
        return result.thenApply(x -> {
            switch (x.getStatus()) {
                case FAILURE:
                    log.warn("Failed to delete scope: {}", scopeName);
                    throw new ControllerFailureException("Failed to delete scope: " + scopeName);
                case SCOPE_NOT_EMPTY:
                    log.warn("Cannot delete non empty scope: {}", scopeName);
                    throw new IllegalStateException("Scope "+ scopeName+ " is not empty.");
                case SCOPE_NOT_FOUND:
                    log.warn("Scope not found: {}", scopeName);
                    return false;
                case SUCCESS:
                    log.info("Scope deleted successfully: {}", scopeName);
                    return true;
                case UNRECOGNIZED:
                default:
                    throw new ControllerFailureException("Unknown return status deleting scope " + scopeName
                                                         + " " + x.getStatus());
                }
            }).whenComplete((x, e) -> {
                if (e != null) {
                    log.warn("deleteScope failed: ", e);
                }
                LoggerHelpers.traceLeave(log, "deleteScope", traceId);
            });
    }

    @Override
    public CompletableFuture<Boolean> createStream(final StreamConfiguration streamConfig) {
        Exceptions.checkNotClosed(closed.get(), this);
        Preconditions.checkNotNull(streamConfig, "streamConfig");
        long traceId = LoggerHelpers.traceEnter(log, "createStream", streamConfig);

        final CompletableFuture<CreateStreamStatus> result = this.retryConfig.runAsync(() -> {
            RPCAsyncCallback<CreateStreamStatus> callback = new RPCAsyncCallback<>();
            client.createStream(ModelHelper.decode(streamConfig), callback);
            return callback.getFuture();
        }, this.executor);
        return result.thenApply(x -> {
            switch (x.getStatus()) {
            case FAILURE:
                log.warn("Failed to create stream: {}", streamConfig.getStreamName());
                throw new ControllerFailureException("Failed to create stream: " + streamConfig);
            case INVALID_STREAM_NAME:
                log.warn("Illegal stream name: {}", streamConfig.getStreamName());
                throw new IllegalArgumentException("Illegal stream name: " + streamConfig);
            case SCOPE_NOT_FOUND:
                log.warn("Scope not found: {}", streamConfig.getScope());
                throw new IllegalArgumentException("Scope does not exist: " + streamConfig);
            case STREAM_EXISTS:
                log.warn("Stream already exists: {}", streamConfig.getStreamName());
                return false;
            case SUCCESS:
                log.info("Stream created successfully: {}", streamConfig.getStreamName());
                return true;
            case UNRECOGNIZED:
            default:
                throw new ControllerFailureException("Unknown return status creating stream " + streamConfig
                                                     + " " + x.getStatus());
            }
        }).whenComplete((x, e) -> {
            if (e != null) {
                log.warn("createStream failed: ", e);
            }
            LoggerHelpers.traceLeave(log, "createStream", traceId);
        });
    }

    @Override
    public CompletableFuture<Boolean> updateStream(final StreamConfiguration streamConfig) {
        Exceptions.checkNotClosed(closed.get(), this);
        Preconditions.checkNotNull(streamConfig, "streamConfig");
        long traceId = LoggerHelpers.traceEnter(log, "updateStream", streamConfig);

        final CompletableFuture<UpdateStreamStatus> result = this.retryConfig.runAsync(() -> {
            RPCAsyncCallback<UpdateStreamStatus> callback = new RPCAsyncCallback<>();
            client.updateStream(ModelHelper.decode(streamConfig), callback);
            return callback.getFuture();
        }, this.executor);
        return result.thenApply(x -> {
            switch (x.getStatus()) {
            case FAILURE:
                log.warn("Failed to update stream: {}", streamConfig.getStreamName());
                throw new ControllerFailureException("Failed to update stream: " + streamConfig);
            case SCOPE_NOT_FOUND:
                log.warn("Scope not found: {}", streamConfig.getScope());
                throw new IllegalArgumentException("Scope does not exist: " + streamConfig);
            case STREAM_NOT_FOUND:
                log.warn("Stream does not exist: {}", streamConfig.getStreamName());
                throw new IllegalArgumentException("Stream does not exist: " + streamConfig);
            case SUCCESS:
                log.info("Successfully updated stream: {}", streamConfig.getStreamName());
                return true;
            case UNRECOGNIZED:
            default:
                throw new ControllerFailureException("Unknown return status updating stream " + streamConfig
                                                     + " " + x.getStatus());
            }
        }).whenComplete((x, e) -> {
            if (e != null) {
                log.warn("updateStream failed: ", e);
            }
            LoggerHelpers.traceLeave(log, "updateStream", traceId);
        });
    }

    @Override
    public CompletableFuture<Boolean> truncateStream(final String scope, final String stream, final StreamCut streamCut) {
        return truncateStream(scope, stream, streamCut.getPositions().entrySet()
                .stream().collect(Collectors.toMap(x -> x.getKey().getSegmentNumber(), Map.Entry::getValue)));
    }

    private CompletableFuture<Boolean> truncateStream(final String scope, final String stream, final Map<Integer, Long> streamCut) {
        Exceptions.checkNotClosed(closed.get(), this);
        Preconditions.checkNotNull(streamCut, "streamCut");
        long traceId = LoggerHelpers.traceEnter(log, "truncateStream", streamCut);

        final CompletableFuture<UpdateStreamStatus> result = this.retryConfig.runAsync(() -> {
            RPCAsyncCallback<UpdateStreamStatus> callback = new RPCAsyncCallback<>();
            client.truncateStream(ModelHelper.decode(scope, stream, streamCut), callback);
            return callback.getFuture();
        }, this.executor);
        return result.thenApply(x -> {
            switch (x.getStatus()) {
                case FAILURE:
                    log.warn("Failed to truncate stream: {}/{}", scope, stream);
                    throw new ControllerFailureException("Failed to truncate stream: " + scope + "/" + stream);
                case SCOPE_NOT_FOUND:
                    log.warn("Scope not found: {}", scope);
                    throw new IllegalArgumentException("Scope does not exist: " + scope);
                case STREAM_NOT_FOUND:
                    log.warn("Stream does not exist: {}/{}", scope, stream);
                    throw new IllegalArgumentException("Stream does not exist: " + stream);
                case SUCCESS:
                    log.info("Successfully updated stream: {}/{}", scope, stream);
                    return true;
                case UNRECOGNIZED:
                default:
                    throw new ControllerFailureException("Unknown return status updating stream " + scope + "/" + stream
                            + " " + x.getStatus());
            }
        }).whenComplete((x, e) -> {
            if (e != null) {
                log.warn("updateStream failed: ", e);
            }
            LoggerHelpers.traceLeave(log, "updateStream", traceId);
        });
    }

    @Override
    public CancellableRequest<Boolean> scaleStream(final Stream stream, final List<Integer> sealedSegments,
                                                  final Map<Double, Double> newKeyRanges,
                                                  final ScheduledExecutorService executor) {
        Exceptions.checkNotClosed(closed.get(), this);
        CancellableRequest<Boolean> cancellableRequest = new CancellableRequest<>();

        startScaleInternal(stream, sealedSegments, newKeyRanges)
                .whenComplete((startScaleResponse, e) -> {
                    if (e != null) {
                        log.error("failed to start scale {}", e);
                        cancellableRequest.start(() -> Futures.failedFuture(e), any -> true, executor);
                    } else {
                        try {
                            final boolean started = handleScaleResponse(stream, startScaleResponse);
                            cancellableRequest.start(() -> {
                                if (started) {
                                    return checkScaleStatus(stream, startScaleResponse.getEpoch());
                                } else {
                                    return CompletableFuture.completedFuture(false);
                                }
                            }, isDone -> !started || isDone, executor);
                        } catch (Exception ex) {
                            cancellableRequest.start(() -> Futures.failedFuture(ex), any -> true, executor);
                        }
                    }
                });

        return cancellableRequest;
    }

    @Override
    public CompletableFuture<Boolean> startScale(final Stream stream, final List<Integer> sealedSegments,
                                                  final Map<Double, Double> newKeyRanges) {
        Exceptions.checkNotClosed(closed.get(), this);
        long traceId = LoggerHelpers.traceEnter(log, "scaleStream", stream);
        return startScaleInternal(stream, sealedSegments, newKeyRanges)
                .thenApply(response -> handleScaleResponse(stream, response))
                .whenComplete((x, e) -> {
                    if (e != null) {
                        log.warn("scaleStream failed: ", e);
                    }
                    LoggerHelpers.traceLeave(log, "scaleStream", traceId);
                });
    }

    private Boolean handleScaleResponse(Stream stream, ScaleResponse response) {
        switch (response.getStatus()) {
        case FAILURE:
            log.warn("Failed to scale stream: {}", stream.getStreamName());
            throw new ControllerFailureException("Failed to scale stream: " + stream);
        case PRECONDITION_FAILED:
            log.warn("Precondition failed for scale stream: {}", stream.getStreamName());
            return false;
        case STARTED:
            log.info("Successfully started scale stream: {}", stream.getStreamName());
            return true;
        case UNRECOGNIZED:
        default:
            throw new ControllerFailureException("Unknown return status scaling stream " + stream
                                                 + " " + response.getStatus());
        }
    }

    @Override
    public CompletableFuture<Boolean> checkScaleStatus(final Stream stream, final int scaleEpoch) {
        Exceptions.checkNotClosed(closed.get(), this);
        Preconditions.checkNotNull(stream, "stream");
        Preconditions.checkArgument(scaleEpoch >= 0);

        long traceId = LoggerHelpers.traceEnter(log, "checkScale", stream);
        final CompletableFuture<ScaleStatusResponse> result = this.retryConfig.runAsync(() -> {
            RPCAsyncCallback<ScaleStatusResponse> callback = new RPCAsyncCallback<>();
            client.checkScale(ScaleStatusRequest.newBuilder()
                            .setStreamInfo(ModelHelper.createStreamInfo(stream.getScope(), stream.getStreamName()))
                            .setEpoch(scaleEpoch)
                            .build(),
                    callback);
            return callback.getFuture();
        }, this.executor);
        return result.thenApply(response -> {
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
        }).whenComplete((x, e) -> {
            if (e != null) {
                log.warn("checking status failed: ", e);
            }
            LoggerHelpers.traceLeave(log, "checkScale", traceId);
        });
    }

    private CompletableFuture<ScaleResponse> startScaleInternal(final Stream stream, final List<Integer> sealedSegments,
                                                                final Map<Double, Double> newKeyRanges) {
        Preconditions.checkNotNull(stream, "stream");
        Preconditions.checkNotNull(sealedSegments, "sealedSegments");
        Preconditions.checkNotNull(newKeyRanges, "newKeyRanges");

        final CompletableFuture<ScaleResponse> result = this.retryConfig.runAsync(() -> {
            RPCAsyncCallback<ScaleResponse> callback = new RPCAsyncCallback<>();
            client.scale(ScaleRequest.newBuilder()
                            .setStreamInfo(ModelHelper.createStreamInfo(stream.getScope(), stream.getStreamName()))
                            .addAllSealedSegments(sealedSegments)
                            .addAllNewKeyRanges(newKeyRanges.entrySet().stream()
                                    .map(x -> ScaleRequest.KeyRangeEntry.newBuilder()
                                            .setStart(x.getKey()).setEnd(x.getValue()).build())
                                    .collect(Collectors.toList()))
                            .setScaleTimestamp(System.currentTimeMillis())
                            .build(),
                    callback);
            return callback.getFuture();
        }, this.executor);
        return result;
    }

    @Override
    public CompletableFuture<Boolean> sealStream(final String scope, final String streamName) {
        Exceptions.checkNotClosed(closed.get(), this);
        Exceptions.checkNotNullOrEmpty(scope, "scope");
        Exceptions.checkNotNullOrEmpty(streamName, "streamName");
        long traceId = LoggerHelpers.traceEnter(log, "sealStream", scope, streamName);

        final CompletableFuture<UpdateStreamStatus> result = this.retryConfig.runAsync(() -> {
            RPCAsyncCallback<UpdateStreamStatus> callback = new RPCAsyncCallback<>();
            client.sealStream(ModelHelper.createStreamInfo(scope, streamName), callback);
            return callback.getFuture();
        }, this.executor);
        return result.thenApply(x -> {
            switch (x.getStatus()) {
            case FAILURE:
                log.warn("Failed to seal stream: {}", streamName);
                throw new ControllerFailureException("Failed to seal stream: " + streamName);
            case SCOPE_NOT_FOUND:
                log.warn("Scope not found: {}", scope);
                throw new InvalidStreamException("Scope does not exist: " + scope);
            case STREAM_NOT_FOUND:
                log.warn("Stream does not exist: {}", streamName);
                throw new InvalidStreamException("Stream does not exist: " + streamName);
            case SUCCESS:
                log.info("Successfully sealed stream: {}", streamName);
                return true;
            case UNRECOGNIZED:
            default:
                throw new ControllerFailureException("Unknown return status scealing stream " + streamName
                                                     + " " + x.getStatus());
            }
        }).whenComplete((x, e) -> {
            if (e != null) {
                log.warn("sealStream failed: ", e);
            }
            LoggerHelpers.traceLeave(log, "sealStream", traceId);
        });
    }

    @Override
    public CompletableFuture<Boolean> deleteStream(final String scope, final String streamName) {
        Exceptions.checkNotClosed(closed.get(), this);
        Exceptions.checkNotNullOrEmpty(scope, "scope");
        Exceptions.checkNotNullOrEmpty(streamName, "streamName");
        long traceId = LoggerHelpers.traceEnter(log, "deleteStream", scope, streamName);

        final CompletableFuture<DeleteStreamStatus> result = this.retryConfig.runAsync(() -> {
            RPCAsyncCallback<DeleteStreamStatus> callback = new RPCAsyncCallback<>();
            client.deleteStream(ModelHelper.createStreamInfo(scope, streamName), callback);
            return callback.getFuture();
        }, this.executor);
        return result.thenApply(x -> {
            switch (x.getStatus()) {
            case FAILURE:
                log.warn("Failed to delete stream: {}", streamName);
                throw new ControllerFailureException("Failed to delete stream: " + streamName);
            case STREAM_NOT_FOUND:
                log.warn("Stream does not exist: {}", streamName);
                return false;
            case STREAM_NOT_SEALED:
                log.warn("Stream is not sealed: {}", streamName);
                throw new IllegalArgumentException("Stream is not sealed: " + streamName);
            case SUCCESS:
                log.info("Successfully deleted stream: {}", streamName);
                return true;
            case UNRECOGNIZED:
            default:
                throw new ControllerFailureException("Unknown return status deleting stream " + streamName
                                                     + " " + x.getStatus());
            }
        }).whenComplete((x, e) -> {
            if (e != null) {
                log.warn("deleteStream failed: ", e);
            }
            LoggerHelpers.traceLeave(log, "deleteStream", traceId);
        });
    }

    @Override
    public CompletableFuture<Map<Segment, Long>> getSegmentsAtTime(final Stream stream, final long timestamp) {
        Exceptions.checkNotClosed(closed.get(), this);
        Preconditions.checkNotNull(stream, "stream");
        long traceId = LoggerHelpers.traceEnter(log, "getSegmentsAtTime", stream, timestamp);

        final CompletableFuture<SegmentsAtTime> result = this.retryConfig.runAsync(() -> {
            RPCAsyncCallback<SegmentsAtTime> callback = new RPCAsyncCallback<>();
            StreamInfo streamInfo = ModelHelper.createStreamInfo(stream.getScope(), stream.getStreamName());
            GetSegmentsRequest request = GetSegmentsRequest.newBuilder()
                    .setStreamInfo(streamInfo)
                    .setTimestamp(timestamp)
                    .build();
            client.getSegments(request, callback);
            return callback.getFuture();
        }, this.executor);
        return result.thenApply(segments -> {
            log.debug("Received the following data from the controller {}", segments.getSegmentsList());
            return segments.getSegmentsList()
                           .stream()
                           .collect(Collectors.toMap(location -> ModelHelper.encode(location.getSegmentId()),
                                                     location -> location.getOffset()));
        }).whenComplete((x, e) -> {
            if (e != null) {
                log.warn("getSegmentsAtTime failed: ", e);
            }
            LoggerHelpers.traceLeave(log, "getSegmentsAtTime", traceId);
        });
    }

    @Override
    public CompletableFuture<StreamSegmentsWithPredecessors> getSuccessors(Segment segment) {
        Exceptions.checkNotClosed(closed.get(), this);
        long traceId = LoggerHelpers.traceEnter(log, "getSuccessors", segment);

        final CompletableFuture<SuccessorResponse> resultFuture = this.retryConfig.runAsync(() -> {
            RPCAsyncCallback<SuccessorResponse> callback = new RPCAsyncCallback<>();
            client.getSegmentsImmediatlyFollowing(ModelHelper.decode(segment), callback);
            return callback.getFuture();
        }, this.executor);
        return resultFuture.thenApply(successors -> {
            log.debug("Received the following data from the controller {}", successors.getSegmentsList());
            Map<SegmentWithRange, List<Integer>> result = new HashMap<>();
            for (SuccessorResponse.SegmentEntry entry : successors.getSegmentsList()) {
                result.put(ModelHelper.encode(entry.getSegment()), entry.getValueList());
            }
            return new StreamSegmentsWithPredecessors(result);
        }).whenComplete((x, e) -> {
            if (e != null) {
                log.warn("getSuccessors failed: ", e);
            }
            LoggerHelpers.traceLeave(log, "getSuccessors", traceId);
        });
    }
    
    @Override
    public CompletableFuture<Set<Segment>> getSuccessors(final StreamCut from) {
        Exceptions.checkNotClosed(closed.get(), this);
        Stream stream = from.getStream();
        long traceId = LoggerHelpers.traceEnter(log, "getSuccessorsFromCut", stream);
        val currentSegments = getAndHandleExceptions(getCurrentSegments(stream.getScope(), stream.getStreamName()),
                                                     RuntimeException::new).getSegments();
        final Set<Segment> unread = getSegmentsInclusive(from, currentSegments);
        LoggerHelpers.traceLeave(log, "getSuccessorsFromCut", traceId);
        return CompletableFuture.completedFuture(unread);
    }

    @Override
    public CompletableFuture<Set<Segment>> getSegments(final StreamCut fromStreamCut, final StreamCut toStreamCut) {
        Exceptions.checkNotClosed(closed.get(), this);
        Preconditions.checkNotNull(fromStreamCut, "fromStreamCut");
        Preconditions.checkNotNull(toStreamCut, "toStreamCut");
        Preconditions.checkArgument(fromStreamCut.getStream().equals(toStreamCut.getStream()),
                "Ensure streamCuts for the same stream is passed");

        final Stream stream = fromStreamCut.getStream();
        long traceId = LoggerHelpers.traceEnter(log, "getSuccessorsFromCut", stream);
        final Set<Segment> segments = getSegmentsInclusive(fromStreamCut, toStreamCut.getPositions().keySet());
        LoggerHelpers.traceLeave(log, "getSuccessorsFromCut", traceId);
        return CompletableFuture.completedFuture(segments);
    }

    private Set<Segment> getSegmentsInclusive(StreamCut fromStreamCut, Collection<Segment> currentSegments) {
        final HashSet<Segment> unread = new HashSet<>(fromStreamCut.getPositions().keySet());
        unread.addAll(currentSegments);
        unread.addAll(computeKnownUnreadSegments(currentSegments, fromStreamCut));
        ArrayDeque<Segment> toFetchSuccessors = new ArrayDeque<>();
        for (Segment toFetch : fromStreamCut.getPositions().keySet()) {
            if (!unread.contains(toFetch)) {
                toFetchSuccessors.add(toFetch);
            }
        }
        while (!toFetchSuccessors.isEmpty()) {
            Segment segment = toFetchSuccessors.remove();
            Set<Segment> successors = getAndHandleExceptions(getSuccessors(segment), RuntimeException::new).getSegmentToPredecessor()
                                                                                                           .keySet();
            for (Segment successor : successors) {
                if (!unread.contains(successor)) {
                    unread.add(successor);
                    toFetchSuccessors.add(successor);
                }
            }
        }
        return unread;
    }

    private List<Segment> computeKnownUnreadSegments(final Collection<Segment> currentSegments, final StreamCut from) {
        int highestCut = from.getPositions().keySet().stream().mapToInt(s -> s.getSegmentNumber()).max().getAsInt();
        int lowestCurrent = currentSegments.stream().mapToInt(s -> s.getSegmentNumber()).min().getAsInt();
        if (highestCut >= lowestCurrent) {
            return Collections.emptyList();
        }
        List<Segment> result = new ArrayList<>(lowestCurrent - highestCut);
        for (int num = highestCut + 1; num < lowestCurrent; num++) {
            result.add(new Segment(from.getStream().getScope(), from.getStream().getStreamName(), num));
        }
        return result;
    }

    @Override
    public CompletableFuture<StreamSegments> getCurrentSegments(final String scope, final String stream) {
        Exceptions.checkNotClosed(closed.get(), this);
        Exceptions.checkNotNullOrEmpty(scope, "scope");
        Exceptions.checkNotNullOrEmpty(stream, "stream");
        long traceId = LoggerHelpers.traceEnter(log, "getCurrentSegments", scope, stream);

        final CompletableFuture<SegmentRanges> result = this.retryConfig.runAsync(() -> {
            RPCAsyncCallback<SegmentRanges> callback = new RPCAsyncCallback<>();
            client.getCurrentSegments(ModelHelper.createStreamInfo(scope, stream), callback);
            return callback.getFuture();
        }, this.executor);
        return result.thenApply(ranges -> {
                    log.debug("Received the following data from the controller {}", ranges.getSegmentRangesList());
                    NavigableMap<Double, Segment> rangeMap = new TreeMap<>();
                    for (SegmentRange r : ranges.getSegmentRangesList()) {
                        rangeMap.put(r.getMaxKey(), ModelHelper.encode(r.getSegmentId()));
                    }
                    return rangeMap;
                }).thenApply(StreamSegments::new)
                .whenComplete((x, e) -> {
                    if (e != null) {
                        log.warn("getCurrentSegments failed: ", e);
                    }
                    LoggerHelpers.traceLeave(log, "getCurrentSegments", traceId);
                });
    }

    @Override
    public CompletableFuture<PravegaNodeUri> getEndpointForSegment(final String qualifiedSegmentName) {
        Exceptions.checkNotClosed(closed.get(), this);
        Exceptions.checkNotNullOrEmpty(qualifiedSegmentName, "qualifiedSegmentName");
        long traceId = LoggerHelpers.traceEnter(log, "getEndpointForSegment", qualifiedSegmentName);

        final CompletableFuture<NodeUri> result = this.retryConfig.runAsync(() -> {
            RPCAsyncCallback<NodeUri> callback = new RPCAsyncCallback<>();
            Segment segment = Segment.fromScopedName(qualifiedSegmentName);
            client.getURI(ModelHelper.createSegmentId(segment.getScope(),
                    segment.getStreamName(),
                    segment.getSegmentNumber()),
                    callback);
            return callback.getFuture();
        }, this.executor);
        return result.thenApply(ModelHelper::encode)
                .whenComplete((x, e) -> {
                    if (e != null) {
                        log.warn("getEndpointForSegment failed: ", e);
                    }
                    LoggerHelpers.traceLeave(log, "getEndpointForSegment", traceId);
                });
    }

    @Override
    public CompletableFuture<Boolean> isSegmentOpen(final Segment segment) {
        Exceptions.checkNotClosed(closed.get(), this);
        long traceId = LoggerHelpers.traceEnter(log, "isSegmentOpen", segment);

        final CompletableFuture<SegmentValidityResponse> result = this.retryConfig.runAsync(() -> {
            RPCAsyncCallback<SegmentValidityResponse> callback = new RPCAsyncCallback<>();
            client.isSegmentValid(ModelHelper.createSegmentId(segment.getScope(),
                    segment.getStreamName(),
                    segment.getSegmentNumber()),
                    callback);
            return callback.getFuture();
        }, this.executor);
        return result.thenApply(SegmentValidityResponse::getResponse)
                .whenComplete((x, e) -> {
                    if (e != null) {
                        log.warn("isSegmentOpen failed: ", e);
                    }
                    LoggerHelpers.traceLeave(log, "isSegmentOpen", traceId);
                });
    }

    @Override
    public CompletableFuture<TxnSegments> createTransaction(final Stream stream, final long lease, final long scaleGracePeriod) {
        Exceptions.checkNotClosed(closed.get(), this);
        Preconditions.checkNotNull(stream, "stream");
        long traceId = LoggerHelpers.traceEnter(log, "createTransaction", stream, lease, scaleGracePeriod);

        final CompletableFuture<CreateTxnResponse> result = this.retryConfig.runAsync(() -> {
            RPCAsyncCallback<CreateTxnResponse> callback = new RPCAsyncCallback<>();
            client.createTransaction(
                    CreateTxnRequest.newBuilder()
                            .setStreamInfo(ModelHelper.createStreamInfo(stream.getScope(), stream.getStreamName()))
                            .setLease(lease)
                            .setScaleGracePeriod(scaleGracePeriod)
                            .build(),
                    callback);
            return callback.getFuture();
        }, this.executor);
        return result.thenApply(this::convert)
                .whenComplete((x, e) -> {
                    if (e != null) {
                        log.warn("createTransaction failed: ", e);
                    }
                    LoggerHelpers.traceLeave(log, "createTransaction", traceId);
                });
    }

    private TxnSegments convert(CreateTxnResponse response) {
        NavigableMap<Double, Segment> rangeMap = new TreeMap<>();

        for (SegmentRange r : response.getActiveSegmentsList()) {
            rangeMap.put(r.getMaxKey(), ModelHelper.encode(r.getSegmentId()));
        }
        StreamSegments segments = new StreamSegments(rangeMap);
        return new TxnSegments(segments, ModelHelper.encode(response.getTxnId()));
    }

    @Override
    public CompletableFuture<Void> pingTransaction(Stream stream, UUID txId, long lease) {
        Exceptions.checkNotClosed(closed.get(), this);
        long traceId = LoggerHelpers.traceEnter(log, "pingTransaction", stream, txId, lease);

        final CompletableFuture<PingTxnStatus> result = this.retryConfig.runAsync(() -> {
            RPCAsyncCallback<PingTxnStatus> callback = new RPCAsyncCallback<>();
            client.pingTransaction(PingTxnRequest.newBuilder().setStreamInfo(
                    ModelHelper.createStreamInfo(stream.getScope(), stream.getStreamName()))
                            .setTxnId(ModelHelper.decode(txId))
                            .setLease(lease).build(),
                    callback);
            return callback.getFuture();
        }, this.executor);
        return Futures.toVoidExpecting(result,
                                             PingTxnStatus.newBuilder().setStatus(PingTxnStatus.Status.OK).build(),
                                             PingFailedException::new)
                      .whenComplete((x, e) -> {
                    if (e != null) {
                        log.warn("pingTransaction failed: ", e);
                    }
                    LoggerHelpers.traceLeave(log, "pingTransaction", traceId);
                });
    }

    @Override
    public CompletableFuture<Void> commitTransaction(final Stream stream, final UUID txId) {
        Exceptions.checkNotClosed(closed.get(), this);
        Preconditions.checkNotNull(stream, "stream");
        Preconditions.checkNotNull(txId, "txId");
        long traceId = LoggerHelpers.traceEnter(log, "commitTransaction", stream, txId);

        final CompletableFuture<TxnStatus> result = this.retryConfig.runAsync(() -> {
            RPCAsyncCallback<TxnStatus> callback = new RPCAsyncCallback<>();
            client.commitTransaction(TxnRequest.newBuilder()
                            .setStreamInfo(ModelHelper.createStreamInfo(stream.getScope(),
                                    stream.getStreamName()))
                            .setTxnId(ModelHelper.decode(txId))
                            .build(),
                    callback);
            return callback.getFuture();
        }, this.executor);
        return Futures.toVoidExpecting(result,
                TxnStatus.newBuilder().setStatus(TxnStatus.Status.SUCCESS).build(), TxnFailedException::new)
                      .whenComplete((x, e) -> {
                    if (e != null) {
                        log.warn("commitTransaction failed: ", e);
                    }
                    LoggerHelpers.traceLeave(log, "commitTransaction", traceId);
                });
    }

    @Override
    public CompletableFuture<Void> abortTransaction(final Stream stream, final UUID txId) {
        Exceptions.checkNotClosed(closed.get(), this);
        Preconditions.checkNotNull(stream, "stream");
        Preconditions.checkNotNull(txId, "txId");
        long traceId = LoggerHelpers.traceEnter(log, "abortTransaction", stream, txId);

        final CompletableFuture<TxnStatus> result = this.retryConfig.runAsync(() -> {
            RPCAsyncCallback<TxnStatus> callback = new RPCAsyncCallback<>();
            client.abortTransaction(TxnRequest.newBuilder()
                            .setStreamInfo(ModelHelper.createStreamInfo(stream.getScope(),
                                    stream.getStreamName()))
                            .setTxnId(ModelHelper.decode(txId))
                            .build(),
                    callback);
            return callback.getFuture();
        }, this.executor);
        return Futures.toVoidExpecting(result,
                TxnStatus.newBuilder().setStatus(TxnStatus.Status.SUCCESS).build(), TxnFailedException::new)
                      .whenComplete((x, e) -> {
                    if (e != null) {
                        log.warn("abortTransaction failed: ", e);
                    }
                    LoggerHelpers.traceLeave(log, "abortTransaction", traceId);
                });
    }

    @Override
    public CompletableFuture<Transaction.Status> checkTransactionStatus(final Stream stream, final UUID txId) {
        Exceptions.checkNotClosed(closed.get(), this);
        Preconditions.checkNotNull(stream, "stream");
        Preconditions.checkNotNull(txId, "txId");
        long traceId = LoggerHelpers.traceEnter(log, "checkTransactionStatus", stream, txId);

        final CompletableFuture<TxnState> result = this.retryConfig.runAsync(() -> {
            RPCAsyncCallback<TxnState> callback = new RPCAsyncCallback<>();
            client.checkTransactionState(TxnRequest.newBuilder()
                            .setStreamInfo(ModelHelper.createStreamInfo(stream.getScope(),
                                    stream.getStreamName()))
                            .setTxnId(ModelHelper.decode(txId))
                            .build(),
                    callback);
            return callback.getFuture();
        }, this.executor);
        return result.thenApply(status -> ModelHelper.encode(status.getState(), stream + " " + txId))
                .whenComplete((x, e) -> {
                    if (e != null) {
                        log.warn("checkTransactionStatus failed: ", e);
                    }
                    LoggerHelpers.traceLeave(log, "checkTransactionStatus", traceId);
                });
    }

    @Override
    public void close() {
        if (!closed.getAndSet(true)) {
            this.channel.shutdownNow(); // Initiates a shutdown of channel.
        }
    }

    // Local callback definition to wrap gRPC responses in CompletableFutures used by the rest of our code.
    private static final class RPCAsyncCallback<T> implements StreamObserver<T> {
        private T result = null;
        private final CompletableFuture<T> future = new CompletableFuture<>();

        @Override
        public void onNext(T value) {
            result = value;
        }

        @Override
        public void onError(Throwable t) {
            log.warn("gRPC call failed with server error: {}", t.getMessage());
            future.completeExceptionally(t);
        }

        @Override
        public void onCompleted() {

            // gRPC interface guarantees that onNext() with the success result would have been called before this.
            future.complete(result);
        }

        public CompletableFuture<T> getFuture() {
            return future;
        }
    }
}
