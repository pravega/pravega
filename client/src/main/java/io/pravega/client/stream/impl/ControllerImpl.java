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
import com.google.common.base.Strings;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import io.grpc.auth.MoreCallCredentials;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.util.RoundRobinLoadBalancerFactory;
import io.netty.handler.ssl.SslContextBuilder;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.InvalidStreamException;
import io.pravega.client.stream.PingFailedException;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.StreamCut;
import io.pravega.client.stream.Transaction;
import io.pravega.client.stream.TxnFailedException;
import io.pravega.common.Exceptions;
import io.pravega.common.LoggerHelpers;
import io.pravega.common.hash.RandomFactory;
import io.pravega.common.tracing.RequestTracker;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.tracing.TagLogger;
import io.pravega.common.util.Retry;
import io.pravega.controller.stream.api.grpc.v1.Controller.CreateScopeStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.CreateStreamStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.CreateTxnRequest;
import io.pravega.controller.stream.api.grpc.v1.Controller.CreateTxnResponse;
import io.pravega.controller.stream.api.grpc.v1.Controller.DelegationToken;
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
import io.pravega.controller.stream.api.grpc.v1.Controller.StreamConfig;
import io.pravega.controller.stream.api.grpc.v1.Controller.StreamInfo;
import io.pravega.controller.stream.api.grpc.v1.Controller.SuccessorResponse;
import io.pravega.controller.stream.api.grpc.v1.Controller.TxnRequest;
import io.pravega.controller.stream.api.grpc.v1.Controller.TxnState;
import io.pravega.controller.stream.api.grpc.v1.Controller.TxnStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.UpdateStreamStatus;
import io.pravega.controller.stream.api.grpc.v1.ControllerServiceGrpc;
import io.pravega.controller.stream.api.grpc.v1.ControllerServiceGrpc.ControllerServiceStub;
import io.pravega.controller.stream.api.grpc.v1.Controller.StreamCutRangeResponse;
import io.pravega.shared.controller.tracing.RPCTracingHelpers;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.net.ssl.SSLException;
import org.slf4j.LoggerFactory;

/**
 * RPC based client implementation of Stream Controller V1 API.
 */
public class ControllerImpl implements Controller {

    private static final TagLogger log = new TagLogger(LoggerFactory.getLogger(ControllerImpl.class));

    // The default keepalive interval for the gRPC transport to ensure long running RPCs are tested for connectivity.
    // This value should be greater than the permissible value configured at the server which is by default 5 minutes.
    private static final long DEFAULT_KEEPALIVE_TIME_MINUTES = 6;

    // The internal retry object to handle RPC failures.
    private final Retry.RetryAndThrowExceptionally<StatusRuntimeException, Exception> retryConfig;

    // The executor supplied by the appication to handle internal retries.
    private final ScheduledExecutorService executor;

    // Flag to indicate if the client is closed.
    private final AtomicBoolean closed = new AtomicBoolean(false);

    // io.grpc.Channel used by the grpc client for Controller Service.
    private final ManagedChannel channel;

    // The gRPC client for the Controller Service.
    private final ControllerServiceStub client;

    // Generate random numbers for request ids.
    private final Supplier<Long> requestIdGenerator = RandomFactory.create()::nextLong;

    /**
     * Creates a new instance of the Controller client class.
     *
     * @param config        The configuration for this client implementation.
     * @param executor      The executor service to be used for handling retries.
     */
    public ControllerImpl(final ControllerImplConfig config,
                          final ScheduledExecutorService executor) {
        this(NettyChannelBuilder.forTarget(config.getClientConfig().getControllerURI().toString())
                                .nameResolverFactory(new ControllerResolverFactory())
                                .loadBalancerFactory(RoundRobinLoadBalancerFactory.getInstance())
                                .keepAliveTime(DEFAULT_KEEPALIVE_TIME_MINUTES, TimeUnit.MINUTES),
                config, executor);
        log.info("Controller client connecting to server at {}", config.getClientConfig().getControllerURI().getAuthority());
    }

    /**
     * Creates a new instance of the Controller client class.
     *  @param channelBuilder The channel builder to connect to the service instance.
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

        if (config.getClientConfig().isEnableTls()) {
            SslContextBuilder sslContextBuilder;
            String trustStore = config.getClientConfig().getTrustStore();
            sslContextBuilder = GrpcSslContexts.forClient();
            if (!Strings.isNullOrEmpty(trustStore)) {
                sslContextBuilder = sslContextBuilder.trustManager(new File(trustStore));
            }
            try {
                channelBuilder = ((NettyChannelBuilder) channelBuilder).sslContext(sslContextBuilder.build())
                                                                       .negotiationType(NegotiationType.TLS);
            } catch (SSLException e) {
                throw new CompletionException(e);
            }
        } else {
            channelBuilder = ((NettyChannelBuilder) channelBuilder).negotiationType(NegotiationType.PLAINTEXT);
        }

        // Trace channel.
        channelBuilder = channelBuilder.intercept(RPCTracingHelpers.getClientInterceptor());

        // Create Async RPC client.
        this.channel = channelBuilder.build();
        ControllerServiceStub client = ControllerServiceGrpc.newStub(this.channel);
        Credentials credentials = config.getClientConfig().getCredentials();
        if (credentials != null) {
            PravegaCredentialsWrapper wrapper = new PravegaCredentialsWrapper(credentials);
            client = client.withCallCredentials(MoreCallCredentials.from(wrapper));
        }
        this.client = client;
    }

    @Override
    public CompletableFuture<Boolean> createScope(final String scopeName) {
        Exceptions.checkNotClosed(closed.get(), this);
        final long requestId = requestIdGenerator.get();
        long traceId = LoggerHelpers.traceEnter(log, "createScope", scopeName, requestId);

        final CompletableFuture<CreateScopeStatus> result = this.retryConfig.runAsync(() -> {
            RPCAsyncCallback<CreateScopeStatus> callback = new RPCAsyncCallback<>(requestId, "createScope");
            new ControllerClientTagger(client).withTag(requestId, "createScope", scopeName)
                                              .createScope(ScopeInfo.newBuilder().setScope(scopeName).build(), callback);
            return callback.getFuture();
        }, this.executor);
        return result.thenApply(x -> {
                switch (x.getStatus()) {
                case FAILURE:
                    log.warn(requestId, "Failed to create scope: {}", scopeName);
                    throw new ControllerFailureException("Failed to create scope: " + scopeName);
                case INVALID_SCOPE_NAME:
                    log.warn(requestId, "Illegal scope name: {}", scopeName);
                    throw new IllegalArgumentException("Illegal scope name: " + scopeName);
                case SCOPE_EXISTS:
                    log.warn(requestId, "Scope already exists: {}", scopeName);
                    return false;
                case SUCCESS:
                    log.info(requestId, "Scope created successfully: {}", scopeName);
                    return true;
                case UNRECOGNIZED:
                default:
                    throw new ControllerFailureException("Unknown return status creating scope " + scopeName
                                                         + " " + x.getStatus());
                }
            }).whenComplete((x, e) -> {
                if (e != null) {
                    log.warn(requestId, "createScope failed: ", e);
                }
                LoggerHelpers.traceLeave(log, "createScope", traceId, scopeName, requestId);
            });
    }

    @Override
    public CompletableFuture<Boolean> deleteScope(String scopeName) {
        Exceptions.checkNotClosed(closed.get(), this);
        final long requestId = requestIdGenerator.get();
        long traceId = LoggerHelpers.traceEnter(log, "deleteScope", scopeName, requestId);

        final CompletableFuture<DeleteScopeStatus> result = this.retryConfig.runAsync(() -> {
            RPCAsyncCallback<DeleteScopeStatus> callback = new RPCAsyncCallback<>(requestId, "deleteScope");
            new ControllerClientTagger(client).withTag(requestId, "deleteScope", scopeName)
                                              .deleteScope(ScopeInfo.newBuilder().setScope(scopeName).build(), callback);
            return callback.getFuture();
        }, this.executor);
        return result.thenApply(x -> {
            switch (x.getStatus()) {
                case FAILURE:
                    log.warn(requestId, "Failed to delete scope: {}", scopeName);
                    throw new ControllerFailureException("Failed to delete scope: " + scopeName);
                case SCOPE_NOT_EMPTY:
                    log.warn(requestId, "Cannot delete non empty scope: {}", scopeName);
                    throw new IllegalStateException("Scope " + scopeName + " is not empty.");
                case SCOPE_NOT_FOUND:
                    log.warn(requestId, "Scope not found: {}", scopeName);
                    return false;
                case SUCCESS:
                    log.info(requestId, "Scope deleted successfully: {}", scopeName);
                    return true;
                case UNRECOGNIZED:
                default:
                    throw new ControllerFailureException("Unknown return status deleting scope " + scopeName
                                                         + " " + x.getStatus());
                }
            }).whenComplete((x, e) -> {
                if (e != null) {
                    log.warn(requestId, "deleteScope failed: ", e);
                }
                LoggerHelpers.traceLeave(log, "deleteScope", traceId, scopeName, requestId);
            });
    }

    @Override
    public CompletableFuture<Boolean> createStream(String scope, String streamName, final StreamConfiguration streamConfig) {
        Exceptions.checkNotNullOrEmpty(scope, "scope");
        Exceptions.checkNotClosed(closed.get(), this);
        Preconditions.checkNotNull(streamConfig, "streamConfig");
        final long requestId = requestIdGenerator.get();
        long traceId = LoggerHelpers.traceEnter(log, "createStream", streamConfig, requestId);

        final CompletableFuture<CreateStreamStatus> result = this.retryConfig.runAsync(() -> {
            RPCAsyncCallback<CreateStreamStatus> callback = new RPCAsyncCallback<>(requestId, "createStream");
            new ControllerClientTagger(client).withTag(requestId, "createStream", scope, streamName)
                                              .createStream(ModelHelper.decode(scope, streamName, streamConfig), callback);
            return callback.getFuture();
        }, this.executor);
        return result.thenApply(x -> {
            switch (x.getStatus()) {
            case FAILURE:
                log.warn(requestId, "Failed to create stream: {}", streamName);
                throw new ControllerFailureException("Failed to create stream: " + streamConfig);
            case INVALID_STREAM_NAME:
                log.warn(requestId, "Illegal stream name: {}", streamName);
                throw new IllegalArgumentException("Illegal stream name: " + streamConfig);
            case SCOPE_NOT_FOUND:
                log.warn(requestId, "Scope not found: {}", scope);
                throw new IllegalArgumentException("Scope does not exist: " + streamConfig);
            case STREAM_EXISTS:
                log.warn(requestId, "Stream already exists: {}", streamName);
                return false;
            case SUCCESS:
                log.info(requestId, "Stream created successfully: {}", streamName);
                return true;
            case UNRECOGNIZED:
            default:
                throw new ControllerFailureException("Unknown return status creating stream " + streamConfig
                                                     + " " + x.getStatus());
            }
        }).whenComplete((x, e) -> {
            if (e != null) {
                log.warn(requestId, "createStream failed: ", e);
            }
            LoggerHelpers.traceLeave(log, "createStream", traceId, streamConfig, requestId);
        });
    }

    @Override
    public CompletableFuture<Boolean> updateStream(String scope, String streamName, final StreamConfiguration streamConfig) {
        Exceptions.checkNotClosed(closed.get(), this);
        Preconditions.checkNotNull(streamConfig, "streamConfig");
        final long requestId = requestIdGenerator.get();
        long traceId = LoggerHelpers.traceEnter(log, "updateStream", streamConfig, requestId);

        final CompletableFuture<UpdateStreamStatus> result = this.retryConfig.runAsync(() -> {
            RPCAsyncCallback<UpdateStreamStatus> callback = new RPCAsyncCallback<>(requestId, "updateStream");
            new ControllerClientTagger(client).withTag(requestId, "updateStream", scope, streamName)
                                              .updateStream(ModelHelper.decode(scope, streamName, streamConfig), callback);
            return callback.getFuture();
        }, this.executor);
        return result.thenApply(x -> {
            switch (x.getStatus()) {
            case FAILURE:
                log.warn(requestId, "Failed to update stream: {}", streamName);
                throw new ControllerFailureException("Failed to update stream: " + streamConfig);
            case SCOPE_NOT_FOUND:
                log.warn(requestId, "Scope not found: {}", scope);
                throw new IllegalArgumentException("Scope does not exist: " + streamConfig);
            case STREAM_NOT_FOUND:
                log.warn(requestId, "Stream does not exist: {}", streamName);
                throw new IllegalArgumentException("Stream does not exist: " + streamConfig);
            case SUCCESS:
                log.info(requestId, "Successfully updated stream: {}", streamName);
                return true;
            case UNRECOGNIZED:
            default:
                throw new ControllerFailureException("Unknown return status updating stream " + streamConfig
                                                     + " " + x.getStatus());
            }
        }).whenComplete((x, e) -> {
            if (e != null) {
                log.warn(requestId, "updateStream failed: ", e);
            }
            LoggerHelpers.traceLeave(log, "updateStream", traceId, streamConfig, requestId);
        });
    }

    @Override
    public CompletableFuture<Boolean> truncateStream(final String scope, final String stream, final StreamCut streamCut) {
        return truncateStream(scope, stream, getStreamCutMap(streamCut));
    }

    private CompletableFuture<Boolean> truncateStream(final String scope, final String stream, final Map<Long, Long> streamCut) {
        Exceptions.checkNotClosed(closed.get(), this);
        Preconditions.checkNotNull(streamCut, "streamCut");
        final long requestId = requestIdGenerator.get();
        long traceId = LoggerHelpers.traceEnter(log, "truncateStream", streamCut, requestId);

        final CompletableFuture<UpdateStreamStatus> result = this.retryConfig.runAsync(() -> {
            RPCAsyncCallback<UpdateStreamStatus> callback = new RPCAsyncCallback<>(requestId, "truncateStream");
            new ControllerClientTagger(client).withTag(requestId, "truncateStream", scope, stream)
                                              .truncateStream(ModelHelper.decode(scope, stream, streamCut), callback);
            return callback.getFuture();
        }, this.executor);
        return result.thenApply(x -> {
            switch (x.getStatus()) {
                case FAILURE:
                    log.warn(requestId, "Failed to truncate stream: {}/{}", scope, stream);
                    throw new ControllerFailureException("Failed to truncate stream: " + scope + "/" + stream);
                case SCOPE_NOT_FOUND:
                    log.warn(requestId, "Scope not found: {}", scope);
                    throw new IllegalArgumentException("Scope does not exist: " + scope);
                case STREAM_NOT_FOUND:
                    log.warn(requestId, "Stream does not exist: {}/{}", scope, stream);
                    throw new IllegalArgumentException("Stream does not exist: " + stream);
                case SUCCESS:
                    log.info(requestId, "Successfully updated stream: {}/{}", scope, stream);
                    return true;
                case UNRECOGNIZED:
                default:
                    throw new ControllerFailureException("Unknown return status truncating stream " + scope + "/" + stream
                            + " " + x.getStatus());
            }
        }).whenComplete((x, e) -> {
            if (e != null) {
                log.warn(requestId, "truncateStream failed: ", e);
            }
            LoggerHelpers.traceLeave(log, "truncateStream", traceId, streamCut, requestId);
        });
    }

    @Override
    public CancellableRequest<Boolean> scaleStream(final Stream stream, final List<Long> sealedSegments,
                                                  final Map<Double, Double> newKeyRanges,
                                                  final ScheduledExecutorService executor) {
        Exceptions.checkNotClosed(closed.get(), this);
        CancellableRequest<Boolean> cancellableRequest = new CancellableRequest<>();
        final long requestId = requestIdGenerator.get();
        long traceId = LoggerHelpers.traceEnter(log, "scaleStream", stream, requestId);
        startScaleInternal(stream, sealedSegments, newKeyRanges, "scaleStream", requestId)
                .whenComplete((startScaleResponse, e) -> {
                    if (e != null) {
                        log.error(requestId, "failed to start scale {}", e);
                        cancellableRequest.start(() -> Futures.failedFuture(e), any -> true, executor);
                    } else {
                        try {
                            final boolean started = handleScaleResponse(stream, startScaleResponse, requestId);
                            cancellableRequest.start(() -> {
                                if (started) {
                                    return checkScaleStatus(stream, startScaleResponse.getEpoch());
                                } else {
                                    return CompletableFuture.completedFuture(false);
                                }
                            }, isDone -> !started || isDone, executor);
                            LoggerHelpers.traceLeave(log, "scaleStream", traceId, stream, requestId);
                        } catch (Exception ex) {
                            cancellableRequest.start(() -> Futures.failedFuture(ex), any -> true, executor);
                        }
                    }
                });

        return cancellableRequest;
    }

    @Override
    public CompletableFuture<Boolean> startScale(final Stream stream, final List<Long> sealedSegments,
                                                  final Map<Double, Double> newKeyRanges) {
        Exceptions.checkNotClosed(closed.get(), this);
        final long requestId = requestIdGenerator.get();
        long traceId = LoggerHelpers.traceEnter(log, "scaleStream", stream, requestId);
        return startScaleInternal(stream, sealedSegments, newKeyRanges, "scaleStream", requestId)
                .thenApply(response -> handleScaleResponse(stream, response, traceId))
                .whenComplete((x, e) -> {
                    if (e != null) {
                        log.warn(requestId, "scaleStream failed: ", e);
                    }
                    LoggerHelpers.traceLeave(log, "scaleStream", traceId, stream, requestId);
                });
    }

    private Boolean handleScaleResponse(Stream stream, ScaleResponse response, long requestId) {
        switch (response.getStatus()) {
        case FAILURE:
            log.warn(requestId, "Failed to scale stream: {}", stream.getStreamName());
            throw new ControllerFailureException("Failed to scale stream: " + stream);
        case PRECONDITION_FAILED:
            log.warn(requestId, "Precondition failed for scale stream: {}", stream.getStreamName());
            return false;
        case STARTED:
            log.info(requestId, "Successfully started scale stream: {}", stream.getStreamName());
            return true;
        case UNRECOGNIZED:
        default:
            throw new ControllerFailureException("Unknown return status scaling stream " + stream + " " + response.getStatus());
        }
    }

    @Override
    public CompletableFuture<Boolean> checkScaleStatus(final Stream stream, final int scaleEpoch) {
        Exceptions.checkNotClosed(closed.get(), this);
        Preconditions.checkNotNull(stream, "stream");
        Preconditions.checkArgument(scaleEpoch >= 0);

        long traceId = LoggerHelpers.traceEnter(log, "checkScale", stream);
        final CompletableFuture<ScaleStatusResponse> result = this.retryConfig.runAsync(() -> {
            RPCAsyncCallback<ScaleStatusResponse> callback = new RPCAsyncCallback<>(traceId, "checkScale");
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

    private CompletableFuture<ScaleResponse> startScaleInternal(final Stream stream, final List<Long> sealedSegments,
                                                                final Map<Double, Double> newKeyRanges, String method,
                                                                long requestId) {
        Preconditions.checkNotNull(stream, "stream");
        Preconditions.checkNotNull(sealedSegments, "sealedSegments");
        Preconditions.checkNotNull(newKeyRanges, "newKeyRanges");

        final CompletableFuture<ScaleResponse> result = this.retryConfig.runAsync(() -> {
            RPCAsyncCallback<ScaleResponse> callback = new RPCAsyncCallback<>(requestId, method);
            long scaleTimestamp = System.currentTimeMillis();
            new ControllerClientTagger(client)
                    .withTag(requestId, method, stream.getScope(), stream.getStreamName(), String.valueOf(scaleTimestamp))
                    .scale(ScaleRequest.newBuilder()
                                       .setStreamInfo(ModelHelper.createStreamInfo(stream.getScope(), stream.getStreamName()))
                                       .addAllSealedSegments(sealedSegments)
                                       .addAllNewKeyRanges(newKeyRanges.entrySet().stream()
                                                                       .map(x -> ScaleRequest.KeyRangeEntry.newBuilder()
                                                                                                           .setStart(x.getKey()).setEnd(x.getValue()).build())
                                                                       .collect(Collectors.toList()))
                                       .setScaleTimestamp(scaleTimestamp)
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
        final long requestId = requestIdGenerator.get();
        long traceId = LoggerHelpers.traceEnter(log, "sealStream", scope, streamName, requestId);

        final CompletableFuture<UpdateStreamStatus> result = this.retryConfig.runAsync(() -> {
            RPCAsyncCallback<UpdateStreamStatus> callback = new RPCAsyncCallback<>(requestId, "sealStream");
            new ControllerClientTagger(client).withTag(requestId, "sealStream", scope, streamName)
                                              .sealStream(ModelHelper.createStreamInfo(scope, streamName), callback);
            return callback.getFuture();
        }, this.executor);
        return result.thenApply(x -> {
            switch (x.getStatus()) {
            case FAILURE:
                log.warn(requestId, "Failed to seal stream: {}", streamName);
                throw new ControllerFailureException("Failed to seal stream: " + streamName);
            case SCOPE_NOT_FOUND:
                log.warn(requestId, "Scope not found: {}", scope);
                throw new InvalidStreamException("Scope does not exist: " + scope);
            case STREAM_NOT_FOUND:
                log.warn(requestId, "Stream does not exist: {}", streamName);
                throw new InvalidStreamException("Stream does not exist: " + streamName);
            case SUCCESS:
                log.info(requestId, "Successfully sealed stream: {}", streamName);
                return true;
            case UNRECOGNIZED:
            default:
                throw new ControllerFailureException("Unknown return status sealing stream " + streamName + " " + x.getStatus());
            }
        }).whenComplete((x, e) -> {
            if (e != null) {
                log.warn(requestId, "sealStream failed: ", e);
            }
            LoggerHelpers.traceLeave(log, "sealStream", traceId, scope, streamName, requestId);
        });
    }

    @Override
    public CompletableFuture<Boolean> deleteStream(final String scope, final String streamName) {
        Exceptions.checkNotClosed(closed.get(), this);
        Exceptions.checkNotNullOrEmpty(scope, "scope");
        Exceptions.checkNotNullOrEmpty(streamName, "streamName");
        final long requestId = requestIdGenerator.get();
        long traceId = LoggerHelpers.traceEnter(log, "deleteStream", scope, streamName, requestId);

        final CompletableFuture<DeleteStreamStatus> result = this.retryConfig.runAsync(() -> {
            RPCAsyncCallback<DeleteStreamStatus> callback = new RPCAsyncCallback<>(requestId, "deleteStream");
            new ControllerClientTagger(client).withTag(requestId, "deleteStream", scope, streamName)
                                              .deleteStream(ModelHelper.createStreamInfo(scope, streamName), callback);
            return callback.getFuture();
        }, this.executor);
        return result.thenApply(x -> {
            switch (x.getStatus()) {
            case FAILURE:
                log.warn(requestId, "Failed to delete stream: {}", streamName);
                throw new ControllerFailureException("Failed to delete stream: " + streamName);
            case STREAM_NOT_FOUND:
                log.warn(requestId, "Stream does not exist: {}", streamName);
                return false;
            case STREAM_NOT_SEALED:
                log.warn(requestId, "Stream is not sealed: {}", streamName);
                throw new IllegalArgumentException("Stream is not sealed: " + streamName);
            case SUCCESS:
                log.info(requestId, "Successfully deleted stream: {}", streamName);
                return true;
            case UNRECOGNIZED:
            default:
                throw new ControllerFailureException("Unknown return status deleting stream " + streamName + " " + x.getStatus());
            }
        }).whenComplete((x, e) -> {
            if (e != null) {
                log.warn(requestId, "deleteStream failed: ", e);
            }
            LoggerHelpers.traceLeave(log, "deleteStream", traceId, scope, streamName, requestId);
        });
    }

    @Override
    public CompletableFuture<Map<Segment, Long>> getSegmentsAtTime(final Stream stream, final long timestamp) {
        Exceptions.checkNotClosed(closed.get(), this);
        Preconditions.checkNotNull(stream, "stream");
        long traceId = LoggerHelpers.traceEnter(log, "getSegmentsAtTime", stream, timestamp);

        final CompletableFuture<SegmentsAtTime> result = this.retryConfig.runAsync(() -> {
            RPCAsyncCallback<SegmentsAtTime> callback = new RPCAsyncCallback<>(traceId, "getSegmentsAtTime");
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
            RPCAsyncCallback<SuccessorResponse> callback = new RPCAsyncCallback<>(traceId, "getSuccessors");
            client.getSegmentsImmediatlyFollowing(ModelHelper.decode(segment), callback);
            return callback.getFuture();
        }, this.executor);
        return resultFuture.thenApply(successors -> {
            log.debug("Received the following data from the controller {}", successors.getSegmentsList());
            Map<SegmentWithRange, List<Long>> result = new HashMap<>();
            for (SuccessorResponse.SegmentEntry entry : successors.getSegmentsList()) {
                result.put(ModelHelper.encode(entry.getSegment()), entry.getValueList());
            }
            return new StreamSegmentsWithPredecessors(result, successors.getDelegationToken());
        }).whenComplete((x, e) -> {
            if (e != null) {
                log.warn("getSuccessors failed: ", e);
            }
            LoggerHelpers.traceLeave(log, "getSuccessors", traceId);
        });
    }

    @Override
    public CompletableFuture<StreamSegmentSuccessors> getSuccessors(final StreamCut from) {
        Exceptions.checkNotClosed(closed.get(), this);
        Stream stream = from.asImpl().getStream();
        long traceId = LoggerHelpers.traceEnter(log, "getSuccessorsFromCut", stream);

        return getSegmentsBetweenStreamCuts(from, StreamCut.UNBOUNDED).whenComplete((x, e) -> {
            if (e != null) {
                log.warn("getSuccessors failed: ", e);
            }
            LoggerHelpers.traceLeave(log, "getSuccessors", traceId);
        });
    }

    @Override
    public CompletableFuture<StreamSegmentSuccessors> getSegments(final StreamCut fromStreamCut, final StreamCut toStreamCut) {
        Exceptions.checkNotClosed(closed.get(), this);
        Preconditions.checkNotNull(fromStreamCut, "fromStreamCut");
        Preconditions.checkNotNull(toStreamCut, "toStreamCut");
        Preconditions.checkArgument(fromStreamCut.asImpl().getStream().equals(toStreamCut.asImpl().getStream()),
                "Ensure streamCuts for the same stream is passed");

        final Stream stream = fromStreamCut.asImpl().getStream();
        long traceId = LoggerHelpers.traceEnter(log, "getSegments", stream);

        return getSegmentsBetweenStreamCuts(fromStreamCut, toStreamCut).whenComplete((x, e) -> {
            if (e != null) {
                log.warn("getSuccessors failed: ", e);
            }
            LoggerHelpers.traceLeave(log, "getSuccessors", traceId);
        });
    }

    private CompletableFuture<StreamSegmentSuccessors> getSegmentsBetweenStreamCuts(final StreamCut fromStreamCut, final StreamCut toStreamCut) {
        Exceptions.checkNotClosed(closed.get(), this);

        final Stream stream = fromStreamCut.asImpl().getStream();
        long traceId = LoggerHelpers.traceEnter(log, "getSegments", stream);
        CompletableFuture<String> token = getOrRefreshDelegationTokenFor(stream.getScope(), stream.getStreamName());
        final CompletableFuture<StreamCutRangeResponse> resultFuture = this.retryConfig.runAsync(() -> {
            RPCAsyncCallback<StreamCutRangeResponse> callback = new RPCAsyncCallback<>(traceId, "getSuccessorsFromCut");
            client.getSegmentsBetween(ModelHelper.decode(stream.getScope(), stream.getStreamName(),
                    getStreamCutMap(fromStreamCut), getStreamCutMap(toStreamCut)), callback);
            return callback.getFuture();
        }, this.executor);
        return resultFuture.thenApply(response -> {
            log.debug("Received the following data from the controller {}", response.getSegmentsList());

            return new StreamSegmentSuccessors(response.getSegmentsList().stream().map(ModelHelper::encode).collect(Collectors.toSet()),
                    response.getDelegationToken());
        });
    }

    private Map<Long, Long> getStreamCutMap(StreamCut streamCut) {
        if (streamCut.equals(StreamCut.UNBOUNDED)) {
            return Collections.emptyMap();
        }
        return streamCut.asImpl().getPositions().entrySet()
                .stream().collect(Collectors.toMap(x -> x.getKey().getSegmentId(), Map.Entry::getValue));
    }

    @Override
    public CompletableFuture<StreamSegments> getCurrentSegments(final String scope, final String stream) {
        Exceptions.checkNotClosed(closed.get(), this);
        Exceptions.checkNotNullOrEmpty(scope, "scope");
        Exceptions.checkNotNullOrEmpty(stream, "stream");
        long traceId = LoggerHelpers.traceEnter(log, "getCurrentSegments", scope, stream);

        final CompletableFuture<SegmentRanges> result = this.retryConfig.runAsync(() -> {
            RPCAsyncCallback<SegmentRanges> callback = new RPCAsyncCallback<>(traceId, "getCurrentSegments");
            client.getCurrentSegments(ModelHelper.createStreamInfo(scope, stream), callback);
            return callback.getFuture();
        }, this.executor);
        return result.thenApply(ranges -> {
            log.debug("Received the following data from the controller {}", ranges.getSegmentRangesList());
            NavigableMap<Double, Segment> rangeMap = new TreeMap<>();
            for (SegmentRange r : ranges.getSegmentRangesList()) {
                Preconditions.checkState(r.getMinKey() <= r.getMaxKey(),
                                         "Min keyrange %s was not less than maximum keyRange %s for segment %s",
                                         r.getMinKey(), r.getMaxKey(), r.getSegmentId());
                rangeMap.put(r.getMaxKey(), ModelHelper.encode(r.getSegmentId()));
            }
            return new StreamSegments(rangeMap, ranges.getDelegationToken());
        }).whenComplete((x, e) -> {
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
            RPCAsyncCallback<NodeUri> callback = new RPCAsyncCallback<>(traceId, "getEndpointForSegment");
            Segment segment = Segment.fromScopedName(qualifiedSegmentName);
            client.getURI(ModelHelper.createSegmentId(segment.getScope(),
                    segment.getStreamName(),
                    segment.getSegmentId()),
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
            RPCAsyncCallback<SegmentValidityResponse> callback = new RPCAsyncCallback<>(traceId, "isSegmentOpen");
            client.isSegmentValid(ModelHelper.createSegmentId(segment.getScope(),
                    segment.getStreamName(),
                    segment.getSegmentId()),
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
    public CompletableFuture<TxnSegments> createTransaction(final Stream stream, final long lease) {
        Exceptions.checkNotClosed(closed.get(), this);
        Preconditions.checkNotNull(stream, "stream");
        long traceId = LoggerHelpers.traceEnter(log, "createTransaction", stream, lease);

        final CompletableFuture<CreateTxnResponse> result = this.retryConfig.runAsync(() -> {
            RPCAsyncCallback<CreateTxnResponse> callback = new RPCAsyncCallback<>(traceId, "createTransaction");
            client.createTransaction(
                    CreateTxnRequest.newBuilder()
                            .setStreamInfo(ModelHelper.createStreamInfo(stream.getScope(), stream.getStreamName()))
                            .setLease(lease)
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
            Preconditions.checkState(r.getMinKey() <= r.getMaxKey());
            rangeMap.put(r.getMaxKey(), ModelHelper.encode(r.getSegmentId()));
        }
        StreamSegments segments = new StreamSegments(rangeMap, response.getDelegationToken());
        return new TxnSegments(segments, ModelHelper.encode(response.getTxnId()));
    }

    @Override
    public CompletableFuture<Void> pingTransaction(Stream stream, UUID txId, long lease) {
        Exceptions.checkNotClosed(closed.get(), this);
        long traceId = LoggerHelpers.traceEnter(log, "pingTransaction", stream, txId, lease);

        final CompletableFuture<PingTxnStatus> result = this.retryConfig.runAsync(() -> {
            RPCAsyncCallback<PingTxnStatus> callback = new RPCAsyncCallback<>(traceId, "pingTransaction");
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
            RPCAsyncCallback<TxnStatus> callback = new RPCAsyncCallback<>(traceId, "commitTransaction");
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
            RPCAsyncCallback<TxnStatus> callback = new RPCAsyncCallback<>(traceId, "abortTransaction");
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
            RPCAsyncCallback<TxnState> callback = new RPCAsyncCallback<>(traceId, "checkTransactionStatus");
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

    @Override
    public CompletableFuture<String> getOrRefreshDelegationTokenFor(String scope, String streamName) {
        Exceptions.checkNotClosed(closed.get(), this);
        Exceptions.checkNotNullOrEmpty(scope, "scope");
        Exceptions.checkNotNullOrEmpty(streamName, "stream");
        long traceId = LoggerHelpers.traceEnter(log, "getOrRefreshDelegationTokenFor", scope, streamName);

        final CompletableFuture<DelegationToken> result = this.retryConfig.runAsync(() -> {
            RPCAsyncCallback<DelegationToken> callback = new RPCAsyncCallback<>(traceId, "getOrRefreshDelegationTokenFor");
            client.getDelegationToken(ModelHelper.createStreamInfo(scope, streamName), callback);
            return callback.getFuture();
        }, this.executor);

        return result.thenApply( token -> token.getDelegationToken())
        .whenComplete((x, e) -> {
            if (e != null) {
                log.warn("getCurrentSegments failed: ", e);
            }
            LoggerHelpers.traceLeave(log, "getCurrentSegments", traceId);
        });
    }

    // Local callback definition to wrap gRPC responses in CompletableFutures used by the rest of our code.
    private static final class RPCAsyncCallback<T> implements StreamObserver<T> {
        private final long traceId;
        private final String method;
        private T result = null;
        private final CompletableFuture<T> future = new CompletableFuture<>();

        RPCAsyncCallback(long traceId, String method) {
            this.traceId = traceId;
            this.method = method;
        }

        @Override
        public void onNext(T value) {
            result = value;
        }

        @Override
        public void onError(Throwable t) {
            log.warn("gRPC call for {} with trace id {} failed with server error.", method, traceId, t);
            if (t instanceof RuntimeException) {
                future.completeExceptionally(t);
            } else {
                future.completeExceptionally(new RuntimeException(t));
            }
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

    /**
     * Wrapper class for a ControllerServiceStub object that i) abstracts the logic for attaching tags to client RPC
     * requests, and ii) exposes only the operations that are currently supported by the tracing mechanism.
     */
    private static class ControllerClientTagger {

        private ControllerServiceStub clientStub;

        ControllerClientTagger(ControllerServiceStub clientStub) {
            this.clientStub = clientStub;
        }

        /**
         * We attach custom tags to requests by adding them as call options upon a controller request. This guarantees that,
         * irrespective of whether the different threads issue and intercept the client request, the tags will be available
         * for being attached to the RPC request.
         *
         * @param requestId Client-side trace id for this request.
         * @param requestInfo Information that will be used as a descriptor for this request.
         * @return Client stub wrapper with parameters as call options for the internal client stub.
         */
        ControllerClientTagger withTag(long requestId, String...requestInfo) {
            String requestDescriptor = RequestTracker.buildRequestDescriptor(requestInfo);
            log.info(requestId, "Tagging client request ({}).", requestDescriptor);
            clientStub = clientStub.withOption(RPCTracingHelpers.REQUEST_DESCRIPTOR_CALL_OPTION, requestDescriptor)
                                   .withOption(RPCTracingHelpers.REQUEST_ID_CALL_OPTION, String.valueOf(requestId));
            return this;
        }

        // Region of ControllerService operations with end to end tracing supported.

        public void createScope(ScopeInfo scopeInfo, RPCAsyncCallback<CreateScopeStatus> callback) {
            clientStub.createScope(scopeInfo, callback);
        }

        public void deleteScope(ScopeInfo scopeInfo, RPCAsyncCallback<DeleteScopeStatus> callback) {
            clientStub.deleteScope(scopeInfo, callback);
        }

        public void createStream(StreamConfig streamConfig, RPCAsyncCallback<CreateStreamStatus> callback) {
            clientStub.createStream(streamConfig, callback);
        }

        public void scale(ScaleRequest scaleRequest, RPCAsyncCallback<ScaleResponse> callback) {
            clientStub.scale(scaleRequest, callback);
        }

        public void updateStream(StreamConfig streamConfig, RPCAsyncCallback<UpdateStreamStatus> callback) {
            clientStub.updateStream(streamConfig, callback);
        }

        public void truncateStream(io.pravega.controller.stream.api.grpc.v1.Controller.StreamCut streamCut,
                                   RPCAsyncCallback<UpdateStreamStatus> callback) {
            clientStub.truncateStream(streamCut, callback);
        }

        public void sealStream(StreamInfo streamInfo, RPCAsyncCallback<UpdateStreamStatus> callback) {
            clientStub.sealStream(streamInfo, callback);
        }

        public void deleteStream(StreamInfo streamInfo, RPCAsyncCallback<DeleteStreamStatus> callback) {
            clientStub.deleteStream(streamInfo, callback);
        }
    }
}
