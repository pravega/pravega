/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.control.impl;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;
import io.grpc.auth.MoreCallCredentials;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.StreamObserver;
import io.netty.handler.ssl.SslContextBuilder;
import io.pravega.client.admin.KeyValueTableInfo;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.InvalidStreamException;
import io.pravega.client.stream.NoSuchScopeException;
import io.pravega.client.stream.PingFailedException;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.StreamCut;
import io.pravega.client.stream.Transaction;
import io.pravega.client.stream.TxnFailedException;
import io.pravega.client.stream.impl.Credentials;
import io.pravega.client.stream.impl.SegmentWithRange;
import io.pravega.client.stream.impl.StreamImpl;
import io.pravega.client.stream.impl.StreamSegmentSuccessors;
import io.pravega.client.stream.impl.StreamSegments;
import io.pravega.client.stream.impl.StreamSegmentsWithPredecessors;
import io.pravega.client.stream.impl.TxnSegments;
import io.pravega.client.stream.impl.WriterPosition;
import io.pravega.client.tables.KeyValueTableConfiguration;
import io.pravega.client.tables.impl.KeyValueTableSegments;
import io.pravega.common.Exceptions;
import io.pravega.common.LoggerHelpers;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.hash.RandomFactory;
import io.pravega.common.tracing.RequestTracker;
import io.pravega.common.tracing.TagLogger;
import io.pravega.common.util.AsyncIterator;
import io.pravega.common.util.ContinuationTokenAsyncIterator;
import io.pravega.common.util.Retry;
import io.pravega.common.util.Retry.RetryAndThrowConditionally;
import io.pravega.controller.stream.api.grpc.v1.Controller.ContinuationToken;
import io.pravega.controller.stream.api.grpc.v1.Controller.CreateKeyValueTableStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.CreateScopeStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.CreateStreamStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.CreateTxnRequest;
import io.pravega.controller.stream.api.grpc.v1.Controller.CreateTxnResponse;
import io.pravega.controller.stream.api.grpc.v1.Controller.DelegationToken;
import io.pravega.controller.stream.api.grpc.v1.Controller.DeleteScopeStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.DeleteStreamStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.GetEpochSegmentsRequest;
import io.pravega.controller.stream.api.grpc.v1.Controller.GetSegmentsRequest;
import io.pravega.controller.stream.api.grpc.v1.Controller.KeyValueTableConfig;
import io.pravega.controller.stream.api.grpc.v1.Controller.NodeUri;
import io.pravega.controller.stream.api.grpc.v1.Controller.PingTxnRequest;
import io.pravega.controller.stream.api.grpc.v1.Controller.PingTxnStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.RemoveWriterRequest;
import io.pravega.controller.stream.api.grpc.v1.Controller.RemoveWriterResponse;
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
import io.pravega.controller.stream.api.grpc.v1.Controller.StreamCutRangeResponse;
import io.pravega.controller.stream.api.grpc.v1.Controller.StreamInfo;
import io.pravega.controller.stream.api.grpc.v1.Controller.StreamsInScopeRequest;
import io.pravega.controller.stream.api.grpc.v1.Controller.StreamsInScopeResponse;
import io.pravega.controller.stream.api.grpc.v1.Controller.SuccessorResponse;
import io.pravega.controller.stream.api.grpc.v1.Controller.SubscribersResponse;
import io.pravega.controller.stream.api.grpc.v1.Controller.TimestampFromWriter;
import io.pravega.controller.stream.api.grpc.v1.Controller.TimestampResponse;
import io.pravega.controller.stream.api.grpc.v1.Controller.TxnRequest;
import io.pravega.controller.stream.api.grpc.v1.Controller.TxnState;
import io.pravega.controller.stream.api.grpc.v1.Controller.TxnStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.UpdateStreamStatus;
import io.pravega.controller.stream.api.grpc.v1.ControllerServiceGrpc;
import io.pravega.controller.stream.api.grpc.v1.ControllerServiceGrpc.ControllerServiceStub;
import io.pravega.controller.stream.api.grpc.v1.Controller.KVTablesInScopeRequest;
import io.pravega.controller.stream.api.grpc.v1.Controller.KVTablesInScopeResponse;
import io.pravega.controller.stream.api.grpc.v1.Controller.DeleteKVTableStatus;

import io.pravega.shared.controller.tracing.RPCTracingHelpers;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import java.io.File;
import java.util.AbstractMap;
import java.util.Collection;
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
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.net.ssl.SSLException;
import org.slf4j.LoggerFactory;

import static io.pravega.controller.stream.api.grpc.v1.Controller.*;

/**
 * RPC based client implementation of Stream Controller V1 API.
 */
public class ControllerImpl implements Controller {

    private static final TagLogger log = new TagLogger(LoggerFactory.getLogger(ControllerImpl.class));

    // The default keepalive interval for the gRPC transport to ensure long running RPCs are tested for connectivity.
    // This value should be greater than the permissible value configured at the server which is by default 5 minutes.
    private static final long DEFAULT_KEEPALIVE_TIME_MINUTES = 6;

    // The internal retry object to handle RPC failures.
    private final Retry.RetryAndThrowConditionally retryConfig;

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

    private final long timeoutMillis;
    
    /**
     * Creates a new instance of the Controller client class.
     *
     * @param config        The configuration for this client implementation.
     * @param executor      The executor service to be used for handling retries.
     */
    public ControllerImpl(final ControllerImplConfig config,
                          final ScheduledExecutorService executor) {
        this(NettyChannelBuilder.forTarget(config.getClientConfig().getControllerURI().toString())
                                .nameResolverFactory(new ControllerResolverFactory(executor))
                                .defaultLoadBalancingPolicy("round_robin")
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
        this.retryConfig = createRetryConfig(config);

        if (config.getClientConfig().isEnableTlsToController()) {
            log.debug("Setting up a SSL/TLS channel builder");
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
            log.debug("Using a plaintext channel builder");
            channelBuilder = ((NettyChannelBuilder) channelBuilder).negotiationType(NegotiationType.PLAINTEXT);
        }

        // Trace channel.
        channelBuilder = channelBuilder.intercept(RPCTracingHelpers.getClientInterceptor());

        // Create Async RPC client.
        this.channel = channelBuilder.build();
        this.client = getClientWithCredentials(config);
        this.timeoutMillis = config.getTimeoutMillis();
    }

    private ControllerServiceStub getClientWithCredentials(ControllerImplConfig config) {
        ControllerServiceStub client = ControllerServiceGrpc.newStub(this.channel);
        try {
            Credentials credentials = config.getClientConfig().getCredentials();
            if (credentials != null) {
                PravegaCredentialsWrapper wrapper = new PravegaCredentialsWrapper(credentials);
                client = client.withCallCredentials(MoreCallCredentials.from(wrapper));
            }
        } catch (Exception e) {
            log.error("Error while setting credentials to controller client", e);
            closeChannel();
            throw e;
        }
        return client;
    }

    @SuppressWarnings("checkstyle:ReturnCount")
    private RetryAndThrowConditionally createRetryConfig(final ControllerImplConfig config) {
        return Retry.withExpBackoff(config.getInitialBackoffMillis(), config.getBackoffMultiple(),
                                    config.getRetryAttempts(), config.getMaxBackoffMillis())
                .retryWhen(e -> {
                    Throwable cause = Exceptions.unwrap(e);
                    if (cause instanceof StatusRuntimeException) {
                        Code code = ((StatusRuntimeException) cause).getStatus().getCode();
                        switch (code) {
                        case ABORTED: return true;
                        case ALREADY_EXISTS: return false;
                        case CANCELLED: return true;
                        case DATA_LOSS: return true;
                        case DEADLINE_EXCEEDED: return true;
                        case FAILED_PRECONDITION: return true;
                        case INTERNAL: return true;
                        case INVALID_ARGUMENT: return false;
                        case NOT_FOUND: return false;
                        case OK: return false;
                        case OUT_OF_RANGE: return false;
                        case PERMISSION_DENIED: return false;
                        case RESOURCE_EXHAUSTED: return true;
                        case UNAUTHENTICATED: return false;
                        case UNAVAILABLE: return true;
                        case UNIMPLEMENTED: return false;
                        case UNKNOWN: return true;
                        }
                        return true;
                    } else {
                        return false;
                    }
                });
    }

    @Override
    public CompletableFuture<Boolean> createScope(final String scopeName) {
        Exceptions.checkNotClosed(closed.get(), this);
        final long requestId = requestIdGenerator.get();
        long traceId = LoggerHelpers.traceEnter(log, "createScope", scopeName, requestId);

        final CompletableFuture<CreateScopeStatus> result = this.retryConfig.runAsync(() -> {
            RPCAsyncCallback<CreateScopeStatus> callback = new RPCAsyncCallback<>(requestId, "createScope", scopeName);
            new ControllerClientTagger(client, timeoutMillis).withTag(requestId, "createScope", scopeName)
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
                    log.warn(requestId, "createScope {} failed: ", scopeName, e);
                }
                LoggerHelpers.traceLeave(log, "createScope", traceId, scopeName, requestId);
            });
    }

    @Override
    public CompletableFuture<Boolean> checkScopeExists(String scopeName) {
        Exceptions.checkNotClosed(closed.get(), this);
        final long requestId = requestIdGenerator.get();
        long traceId = LoggerHelpers.traceEnter(log, "checkScopeExists", scopeName, requestId);

        final CompletableFuture<Boolean> result = this.retryConfig.runAsync(() -> {
            RPCAsyncCallback<ExistsResponse> callback = new RPCAsyncCallback<>(requestId, "checkScopeExists", scopeName);
            new ControllerClientTagger(client, timeoutMillis).withTag(requestId, "checkScopeExists", scopeName)
                                                             .checkScopeExists(ScopeInfo.newBuilder().setScope(scopeName).build(), callback);
            return callback.getFuture().thenApply(ExistsResponse::getExists);
        }, this.executor);
        return result.whenComplete((x, e) -> {
            if (e != null) {
                log.warn(requestId, "checkScopeExists {} failed: ", scopeName, e);
            }
            LoggerHelpers.traceLeave(log, "checkScopeExists", traceId, scopeName, requestId);
        });
    }

    @Override
    public AsyncIterator<String> listScopes() {
        Exceptions.checkNotClosed(closed.get(), this);
        long traceId = LoggerHelpers.traceEnter(log, "listScopes");
        long requestId = requestIdGenerator.get();
        try {
            final Function<ContinuationToken, CompletableFuture<Map.Entry<ContinuationToken, Collection<String>>>> function =
                    token -> this.retryConfig.runAsync(() -> {
                        RPCAsyncCallback<ScopesResponse> callback = new RPCAsyncCallback<>(requestId, "listScopes");
                        
                        new ControllerClientTagger(client, timeoutMillis).withTag(requestId, "listScopes")
                                                                    .listScopes(ScopesRequest
                                                                  .newBuilder().setContinuationToken(token).build(), callback);
                        return callback.getFuture()
                                       .thenApply(x -> {
                                               List<String> result = x.getScopesList();
                                               return new AbstractMap.SimpleEntry<>(x.getContinuationToken(), result);
                                       });
                    }, this.executor);
            return new ContinuationTokenAsyncIterator<>(function, ContinuationToken.newBuilder().build());
        } finally {
            LoggerHelpers.traceLeave(log, "listStreams", traceId);
        }
    }
    
    @Override
    public AsyncIterator<Stream> listStreams(String scopeName) {
        Exceptions.checkNotClosed(closed.get(), this);
        long traceId = LoggerHelpers.traceEnter(log, "listStreams", scopeName);
        long requestId = requestIdGenerator.get();
        try {
            final Function<ContinuationToken, CompletableFuture<Map.Entry<ContinuationToken, Collection<Stream>>>> function =
                    token -> this.retryConfig.runAsync(() -> {
                        RPCAsyncCallback<StreamsInScopeResponse> callback = new RPCAsyncCallback<>(requestId, "listStreams", scopeName);
                        ScopeInfo scopeInfo = ScopeInfo.newBuilder().setScope(scopeName).build();
                        new ControllerClientTagger(client, timeoutMillis).withTag(requestId, "listStreams", scopeName)
                                                                    .listStreamsInScope(StreamsInScopeRequest
                                                                  .newBuilder().setScope(scopeInfo).setContinuationToken(token).build(), callback);
                        return callback.getFuture()
                                       .thenApply(x -> {
                                           switch (x.getStatus()) {
                                               case SCOPE_NOT_FOUND:
                                                   log.warn(requestId, "Scope not found: {}", scopeName);
                                                   throw new NoSuchScopeException();
                                               case FAILURE:
                                                   log.warn(requestId, "Internal Server Error while trying to list streams in scope: {}", scopeName);
                                                   throw new RuntimeException("Failure while trying to list streams");
                                               case SUCCESS:
                                               // we will treat all other case as success for backward 
                                               // compatibility reasons
                                               default: 
                                                   List<Stream> result = x.getStreamsList().stream()
                                                                          .map(y -> new StreamImpl(y.getScope(), y.getStream())).collect(Collectors.toList());
                                                   return new AbstractMap.SimpleEntry<>(x.getContinuationToken(), result);
                                           }
                                       });
                    }, this.executor);
            return new ContinuationTokenAsyncIterator<>(function, ContinuationToken.newBuilder().build());
        } finally {
            LoggerHelpers.traceLeave(log, "listStreams", traceId);
        }
    }

    @Override
    public CompletableFuture<Boolean> deleteScope(String scopeName) {
        Exceptions.checkNotClosed(closed.get(), this);
        final long requestId = requestIdGenerator.get();
        long traceId = LoggerHelpers.traceEnter(log, "deleteScope", scopeName, requestId);

        final CompletableFuture<DeleteScopeStatus> result = this.retryConfig.runAsync(() -> {
            RPCAsyncCallback<DeleteScopeStatus> callback = new RPCAsyncCallback<>(requestId, "deleteScope", scopeName);
            new ControllerClientTagger(client, timeoutMillis).withTag(requestId, "deleteScope", scopeName)
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
                    log.warn(requestId, "deleteScope {} failed: ", scopeName, e);
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
            RPCAsyncCallback<CreateStreamStatus> callback = new RPCAsyncCallback<>(requestId, "createStream", scope, streamName, streamConfig);
            new ControllerClientTagger(client, timeoutMillis).withTag(requestId, "createStream", scope, streamName)
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
                log.warn(requestId, "createStream {}/{} failed: ", scope, streamName, e);
            }
            LoggerHelpers.traceLeave(log, "createStream", traceId, streamConfig, requestId);
        });
    }

    @Override
    public CompletableFuture<Boolean> checkStreamExists(String scopeName, String streamName) {
        Exceptions.checkNotClosed(closed.get(), this);
        final long requestId = requestIdGenerator.get();
        long traceId = LoggerHelpers.traceEnter(log, "checkStreamExists", scopeName, streamName, requestId);

        final CompletableFuture<Boolean> result = this.retryConfig.runAsync(() -> {
            RPCAsyncCallback<ExistsResponse> callback = new RPCAsyncCallback<>(requestId, "checkStreamExists", scopeName, streamName);
            new ControllerClientTagger(client, timeoutMillis).withTag(requestId, "checkStreamExists", scopeName, streamName)
                                                             .checkStreamExists(StreamInfo.newBuilder().setScope(scopeName)
                                                                                          .setStream(streamName).build(), callback);
            return callback.getFuture().thenApply(ExistsResponse::getExists);
        }, this.executor);
        return result.whenComplete((x, e) -> {
            if (e != null) {
                log.warn(requestId, "checkStreamExists {}/{} failed: ", scopeName, streamName, e);
            }
            LoggerHelpers.traceLeave(log, "checkStreamExists", traceId, scopeName, streamName, requestId);
        });
    }

    @Override
    public CompletableFuture<Boolean> updateStream(String scope, String streamName, final StreamConfiguration streamConfig) {
        Exceptions.checkNotClosed(closed.get(), this);
        Preconditions.checkNotNull(streamConfig, "streamConfig");
        final long requestId = requestIdGenerator.get();
        long traceId = LoggerHelpers.traceEnter(log, "updateStream", streamConfig, requestId);

        final CompletableFuture<UpdateStreamStatus> result = this.retryConfig.runAsync(() -> {
            RPCAsyncCallback<UpdateStreamStatus> callback = new RPCAsyncCallback<>(requestId, "updateStream", scope, streamName, streamConfig);
            new ControllerClientTagger(client, timeoutMillis).withTag(requestId, "updateStream", scope, streamName)
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
                log.warn(requestId, "updateStream {}/{} failed: ", scope, streamName, e);
            }
            LoggerHelpers.traceLeave(log, "updateStream", traceId, streamConfig, requestId);
        });
    }

    @Override
    public CompletableFuture<Boolean> addSubscriber(String scope, String streamName, String subscriber) {
        Exceptions.checkNotClosed(closed.get(), this);
        Preconditions.checkNotNull(scope, "scope");
        Preconditions.checkNotNull(streamName, "stream");
        Preconditions.checkNotNull(subscriber, "subscriber");
        final long requestId = requestIdGenerator.get();
        long traceId = LoggerHelpers.traceEnter(log, "addSubscriber", subscriber, requestId);

        final CompletableFuture<AddSubscriberStatus> result = this.retryConfig.runAsync(() -> {
            RPCAsyncCallback<AddSubscriberStatus> callback = new RPCAsyncCallback<>(requestId, "addSubscriber", scope, streamName, subscriber);
            new ControllerClientTagger(client, timeoutMillis).withTag(requestId, "addSubscriber", scope, streamName)
                    .addSubscriber(ModelHelper.decode(scope, streamName, subscriber), callback);
            return callback.getFuture();
        }, this.executor);
        return result.thenApply(x -> {
            switch (x.getStatus()) {
                case FAILURE:
                    log.warn(requestId, "Failed to update stream: {}", streamName);
                    throw new ControllerFailureException("Failed to add subscriber: " + subscriber);
                case STREAM_NOT_FOUND:
                    log.warn(requestId, "Stream does not exist: {}/{}", scope, streamName);
                    throw new IllegalArgumentException("Stream does not exist: " + scope + "/" + streamName);
                case SUBSCRIBER_EXISTS:
                    log.warn(requestId, "Subscriber {} for stream {}/{} already exists {}.", subscriber, scope, streamName);
                    return false;
                case SUCCESS:
                    log.info(requestId, "Successfully updated stream: {}", streamName);
                    return true;
                case UNRECOGNIZED:
                default:
                    throw new ControllerFailureException("Unknown return status adding subscriber " + subscriber
                            + " " + x.getStatus());
            }
        }).whenComplete((x, e) -> {
            if (e != null) {
                log.warn(requestId, "addSubscriber {} for stream {}/{} failed: ", subscriber, scope, streamName, e);
            }
            LoggerHelpers.traceLeave(log, "addSubscriber", traceId, subscriber, requestId);
        });
    }

    @Override
    public CompletableFuture<Boolean> deleteSubscriber(String scope, String streamName, String subscriber) {
        Exceptions.checkNotClosed(closed.get(), this);
        Preconditions.checkNotNull(scope, "scope");
        Preconditions.checkNotNull(streamName, "stream");
        Preconditions.checkNotNull(subscriber, "subscriber");
        final long requestId = requestIdGenerator.get();
        long traceId = LoggerHelpers.traceEnter(log, "deleteSubscriber", subscriber, requestId);

        final CompletableFuture<DeleteSubscriberStatus> result = this.retryConfig.runAsync(() -> {
            RPCAsyncCallback<DeleteSubscriberStatus> callback = new RPCAsyncCallback<>(requestId, "deleteSubscriber", scope, streamName, subscriber);
            new ControllerClientTagger(client, timeoutMillis).withTag(requestId, "deleteSubscriber", scope, streamName)
                    .deleteSubscriber(ModelHelper.decode(scope, streamName, subscriber), callback);
            return callback.getFuture();
        }, this.executor);
        return result.thenApply(x -> {
            switch (x.getStatus()) {
                case FAILURE:
                    log.warn(requestId, "Failed to delete subscriber: {} to stream {}/{}", subscriber, scope, streamName);
                    throw new ControllerFailureException("Failed to delete subscriber: " + subscriber);
                case STREAM_NOT_FOUND:
                    log.warn(requestId, "Stream does not exist: {}", streamName);
                    throw new IllegalArgumentException("Stream does not exist: " + streamName);
                case SUBSCRIBER_NOT_FOUND:
                    log.warn(requestId, "Subscriber does not exist: {} for stream {}/{}", subscriber, scope, streamName);
                    return false;
                case SUCCESS:
                    log.info(requestId, "Successfully delete subscriber {} from stream: {}/{}", subscriber, scope, streamName);
                    return true;
                case UNRECOGNIZED:
                default:
                    throw new ControllerFailureException("Unknown return status delete subscriber " + subscriber
                            + " " + x.getStatus());
            }
        }).whenComplete((x, e) -> {
            if (e != null) {
                log.warn(requestId, "deleteSubscriber {} for stream {}/{} failed: ", subscriber, scope, streamName, e);
            }
            LoggerHelpers.traceLeave(log, "deleteSubscriber", traceId, subscriber, requestId);
        });
    }

    @Override
    public CompletableFuture<List<String>> listSubscribers(String scope, String streamName) {
        Exceptions.checkNotClosed(closed.get(), this);
        Preconditions.checkNotNull(scope, "scope");
        Preconditions.checkNotNull(streamName, "stream");
        long traceId = LoggerHelpers.traceEnter(log, "listSubscribers");
        long requestId = requestIdGenerator.get();
        final CompletableFuture<SubscribersResponse> result = this.retryConfig.runAsync(() -> {
        RPCAsyncCallback<SubscribersResponse> callback = new RPCAsyncCallback<>(requestId, "listSubscribers");

        new ControllerClientTagger(client, timeoutMillis).withTag(requestId, "listSubscribers")
                .listSubscribers(ModelHelper.createStreamInfo(scope, streamName), callback);
        return callback.getFuture();
        }, this.executor);
        return result.thenApply(x -> {
            switch (x.getStatus()) {
                case FAILURE:
                    log.warn(requestId, "Failed to list subscribers for stream {}/{}", scope, streamName);
                    throw new ControllerFailureException("Failed to list subscribers for stream" + streamName);
                case STREAM_NOT_FOUND:
                    log.warn(requestId, "Stream does not exist: {}", streamName);
                    throw new IllegalArgumentException("Stream does not exist: " + streamName);
                case SUCCESS:
                    log.info(requestId, "Successfully listed subscribers for stream: {}/{}", scope, streamName);
                    return x.getSubscribersList().stream().collect(Collectors.toList());
                case UNRECOGNIZED:
                default:
                    throw new ControllerFailureException("Unknown return status listing subscribers " + x.getStatus());
            }
        }).whenComplete((x, e) -> {
            if (e != null) {
                log.warn(requestId, "listSubscribers for stream {}/{} failed: ", scope, streamName, e);
            }
            LoggerHelpers.traceLeave(log, "listSubscribers", traceId, requestId);
        });
    }

    @Override
    public CompletableFuture<Boolean> updateSubscriberStreamCut(String scope, String streamName, String subscriber, StreamCut streamCut) {
        Exceptions.checkNotClosed(closed.get(), this);
        Preconditions.checkNotNull(scope, "scope");
        Preconditions.checkNotNull(streamName, "stream");
        Preconditions.checkNotNull(subscriber, "subscriber");
        final long requestId = requestIdGenerator.get();
        long traceId = LoggerHelpers.traceEnter(log, "updateTruncationStreamCut", subscriber, requestId);

        final CompletableFuture<UpdateSubscriberStatus> result = this.retryConfig.runAsync(() -> {
            RPCAsyncCallback<UpdateSubscriberStatus> callback = new RPCAsyncCallback<>(requestId, "updateTruncationStreamCut", scope, streamName, subscriber, streamCut);
            new ControllerClientTagger(client, timeoutMillis).withTag(requestId, "updateTruncationStreamCut", scope, streamName)
                    .updateTruncationStreamCut(ModelHelper.decode(scope, streamName, subscriber, getStreamCutMap(streamCut)), callback);
            return callback.getFuture();
        }, this.executor);
        return result.thenApply(x -> {
            switch (x.getStatus()) {
                case FAILURE:
                    log.warn(requestId, "Failed to update stream: {}", streamName);
                    throw new ControllerFailureException("Failed to updateTruncationStreamcut for Stream:" + scope + "/" + streamName
                            + ": subscriber:" + subscriber);
                case STREAM_NOT_FOUND:
                    log.warn(requestId, "Stream does not exist: {}", streamName);
                    throw new IllegalArgumentException("Stream does not exist: " + streamName);
                case SUBSCRIBER_NOT_FOUND:
                    log.warn(requestId, "Subscriber does not exist: {} for stream {}/{}", subscriber, scope, streamName);
                    throw new IllegalArgumentException("Subscriber does not exist: " + subscriber);
                case STREAMCUT_NOT_VALID:
                    log.warn(requestId, "StreamCut not valid for stream {}/{} subscriber {}.", scope, streamName, subscriber);
                    throw new IllegalArgumentException("StreamCut not valid for stream " + scope + "/" + streamName + ": subscriber:" + subscriber);
                case SUCCESS:
                    log.info(requestId, "Successfully updated truncationStreamCut for subscriber {} from stream: {}/{}", subscriber, scope, streamName);
                    return true;
                case UNRECOGNIZED:
                default:
                    throw new ControllerFailureException("Unknown return status for updateTruncationStreamCut for Stream :"
                            + scope + "/" + streamName + ": subscriber:" + subscriber + ": status=" + x.getStatus());
            }
        }).whenComplete((x, e) -> {
            if (e != null) {
                log.warn(requestId, "updateTruncationStreamCut for Subscriber {} for stream {}/{} failed: ", subscriber, scope, streamName, e);
            }
            LoggerHelpers.traceLeave(log, "updateTruncationStreamCut", traceId, subscriber, requestId);
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
            RPCAsyncCallback<UpdateStreamStatus> callback = new RPCAsyncCallback<>(requestId, "truncateStream", scope, stream);
            new ControllerClientTagger(client, timeoutMillis).withTag(requestId, "truncateStream", scope, stream)
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
                log.warn(requestId, "truncateStream {}/{} failed: ", scope, stream, e);
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
                        log.error(requestId, "Failed to start scale for stream {}", stream, e);
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
                            log.warn(requestId, "Failed to handle scale response: ", ex);
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
                        log.warn(requestId, "Failed to start scale of stream: {} ", stream.getStreamName(), e);
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
            RPCAsyncCallback<ScaleStatusResponse> callback = new RPCAsyncCallback<>(traceId, "checkScale", stream, scaleEpoch);
            client.withDeadlineAfter(timeoutMillis, TimeUnit.MILLISECONDS).checkScale(ScaleStatusRequest.newBuilder()
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
                    log.warn("Failed to check scale status of stream " + stream.getStreamName() + " because of invalid input");
                    throw new ControllerFailureException("invalid input");
                case INTERNAL_ERROR:
                default:
                    throw new ControllerFailureException("Unknown return status checking scale of stream "  + stream + " "
                            + response.getStatus());
            }
        }).whenComplete((x, e) -> {
            if (e != null) {
                log.warn("checkScaleStatus {} failed: ", stream.getStreamName(), e);
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
            RPCAsyncCallback<ScaleResponse> callback = new RPCAsyncCallback<>(requestId, method, stream);
            long scaleTimestamp = System.currentTimeMillis();
            new ControllerClientTagger(client, timeoutMillis)
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
            RPCAsyncCallback<UpdateStreamStatus> callback = new RPCAsyncCallback<>(requestId, "sealStream", scope, streamName);
            new ControllerClientTagger(client, timeoutMillis).withTag(requestId, "sealStream", scope, streamName)
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
                log.warn(requestId, "sealStream {}/{} failed: ", scope, streamName, e);
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
            RPCAsyncCallback<DeleteStreamStatus> callback = new RPCAsyncCallback<>(requestId, "deleteStream", scope, streamName);
            new ControllerClientTagger(client, timeoutMillis).withTag(requestId, "deleteStream", scope, streamName)
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
                log.warn(requestId, "deleteStream {}/{} failed: ", scope, streamName, e);
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
            RPCAsyncCallback<SegmentsAtTime> callback = new RPCAsyncCallback<>(traceId, "getSegmentsAtTime", stream, timestamp);
            StreamInfo streamInfo = ModelHelper.createStreamInfo(stream.getScope(), stream.getStreamName());
            GetSegmentsRequest request = GetSegmentsRequest.newBuilder()
                    .setStreamInfo(streamInfo)
                    .setTimestamp(timestamp)
                    .build();
            client.withDeadlineAfter(timeoutMillis, TimeUnit.MILLISECONDS).getSegments(request, callback);
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
                log.warn("get Segments of {} at time {} failed: ", stream.getStreamName(), timestamp,  e);
            }
            LoggerHelpers.traceLeave(log, "getSegmentsAtTime", traceId);
        });
    }

    @Override
    public CompletableFuture<StreamSegmentsWithPredecessors> getSuccessors(Segment segment) {
        Exceptions.checkNotClosed(closed.get(), this);
        long traceId = LoggerHelpers.traceEnter(log, "getSuccessors", segment);

        final CompletableFuture<SuccessorResponse> resultFuture = this.retryConfig.runAsync(() -> {
            RPCAsyncCallback<SuccessorResponse> callback = new RPCAsyncCallback<>(traceId, "getSuccessors", segment);
            client.withDeadlineAfter(timeoutMillis, TimeUnit.MILLISECONDS)
                  .getSegmentsImmediatelyFollowing(ModelHelper.decode(segment), callback);
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
                log.warn("getSuccessors of segment {} failed: ", segment.getSegmentId(), e);
            }
            LoggerHelpers.traceLeave(log, "getSuccessors", traceId);
        });
    }

    @Override
    public CompletableFuture<StreamSegmentSuccessors> getSuccessors(final StreamCut from) {
        Exceptions.checkNotClosed(closed.get(), this);
        Stream stream = from.asImpl().getStream();
        long traceId = LoggerHelpers.traceEnter(log, "getSuccessors", stream);

        return getSegmentsBetweenStreamCuts(from, StreamCut.UNBOUNDED).whenComplete((x, e) -> {
            if (e != null) {
                log.warn("getSuccessorsFromCut for {} failed: ", stream.getStreamName(), e);
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
                log.warn("getSegments for {} failed: ", stream.getStreamName(), e);
            }
            LoggerHelpers.traceLeave(log, "getSegments", traceId);
        });
    }

    private CompletableFuture<StreamSegmentSuccessors> getSegmentsBetweenStreamCuts(final StreamCut fromStreamCut, final StreamCut toStreamCut) {
        Exceptions.checkNotClosed(closed.get(), this);

        final Stream stream = fromStreamCut.asImpl().getStream();
        long traceId = LoggerHelpers.traceEnter(log, "getSegments", stream);
        CompletableFuture<String> token = getOrRefreshDelegationTokenFor(stream.getScope(), stream.getStreamName());
        final CompletableFuture<StreamCutRangeResponse> resultFuture = this.retryConfig.runAsync(() -> {
            RPCAsyncCallback<StreamCutRangeResponse> callback = new RPCAsyncCallback<>(traceId, "getSuccessorsFromCut");
            client.withDeadlineAfter(timeoutMillis, TimeUnit.MILLISECONDS)
                  .getSegmentsBetween(ModelHelper.decode(stream.getScope(), stream.getStreamName(),
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
            RPCAsyncCallback<SegmentRanges> callback = new RPCAsyncCallback<>(traceId, "getCurrentSegments", scope, stream);
            client.withDeadlineAfter(timeoutMillis, TimeUnit.MILLISECONDS)
                  .getCurrentSegments(ModelHelper.createStreamInfo(scope, stream), callback);
            return callback.getFuture();
        }, this.executor);
        return result.thenApply(this::getStreamSegments)
                     .whenComplete((x, e) -> {
                         if (e != null) {
                             log.warn("getCurrentSegments for {}/{} failed: ", scope, stream, e);
                         }
                         if (x.getNumberOfSegments() == 0 ) {
                             log.warn("getCurrentSegments for {}/{} returned zero segments since the Stream is sealed", scope, stream);
                         }
                         LoggerHelpers.traceLeave(log, "getCurrentSegments", traceId);
                     });
    }

    @Override
    public CompletableFuture<StreamSegments> getEpochSegments(final String scope, final String stream, int epoch) {
        Exceptions.checkNotClosed(closed.get(), this);
        Exceptions.checkNotNullOrEmpty(scope, "scope");
        Exceptions.checkNotNullOrEmpty(stream, "stream");
        Exceptions.checkArgument(epoch >= 0, "epoch", "Should be a positive integer");
        long traceId = LoggerHelpers.traceEnter(log, "getEpochSegments", scope, stream);

        final CompletableFuture<SegmentRanges> result = this.retryConfig.runAsync(() -> {
            RPCAsyncCallback<SegmentRanges> callback = new RPCAsyncCallback<>(traceId, "getEpochSegments", scope, stream);
            GetEpochSegmentsRequest request = GetEpochSegmentsRequest.newBuilder()
                                                                     .setStreamInfo(ModelHelper.createStreamInfo(scope, stream))
                                                                     .setEpoch(epoch)
                                                                     .build();
            client.withDeadlineAfter(timeoutMillis, TimeUnit.MILLISECONDS)
                  .getEpochSegments(request, callback);
            return callback.getFuture();
        }, this.executor);
        return result.thenApply(this::getStreamSegments)
                     .whenComplete((x, e) -> {
                         if (e != null) {
                             log.warn("getEpochSegments for {}/{} with for epoch {} failed: ", scope, stream, epoch, e);
                         }
                         LoggerHelpers.traceLeave(log, "getEpochSegments", traceId);
                     });
    }

    private StreamSegments getStreamSegments(final SegmentRanges ranges) {
        log.debug("Received the following data from the controller {}", ranges.getSegmentRangesList());
        NavigableMap<Double, SegmentWithRange> rangeMap = new TreeMap<>();
        for (SegmentRange r : ranges.getSegmentRangesList()) {
            Preconditions.checkState(r.getMinKey() <= r.getMaxKey(),
                    "Min keyrange %s was not less than maximum keyRange %s for segment %s",
                    r.getMinKey(), r.getMaxKey(), r.getSegmentId());
            rangeMap.put(r.getMaxKey(), new SegmentWithRange(ModelHelper.encode(r.getSegmentId()), r.getMinKey(), r.getMaxKey()));
        }
        return new StreamSegments(rangeMap, ranges.getDelegationToken());
    }

    @Override
    public CompletableFuture<PravegaNodeUri> getEndpointForSegment(final String qualifiedSegmentName) {
        Exceptions.checkNotClosed(closed.get(), this);
        Exceptions.checkNotNullOrEmpty(qualifiedSegmentName, "qualifiedSegmentName");
        long traceId = LoggerHelpers.traceEnter(log, "getEndpointForSegment", qualifiedSegmentName);

        final CompletableFuture<NodeUri> result = this.retryConfig.runAsync(() -> {
            RPCAsyncCallback<NodeUri> callback = new RPCAsyncCallback<>(traceId, "getEndpointForSegment", qualifiedSegmentName);
            Segment segment = Segment.fromScopedName(qualifiedSegmentName);
            client.withDeadlineAfter(timeoutMillis, TimeUnit.MILLISECONDS)
                  .getURI(ModelHelper.createSegmentId(segment.getScope(),
                    segment.getStreamName(),
                    segment.getSegmentId()),
                    callback);
            return callback.getFuture();
        }, this.executor);
        return result.thenApply(ModelHelper::encode)
                .whenComplete((x, e) -> {
                    if (e != null) {
                        log.warn("getEndpointForSegment {} failed: ", qualifiedSegmentName, e);
                    }
                    LoggerHelpers.traceLeave(log, "getEndpointForSegment", traceId);
                });
    }

    @Override
    public CompletableFuture<Boolean> isSegmentOpen(final Segment segment) {
        Exceptions.checkNotClosed(closed.get(), this);
        long traceId = LoggerHelpers.traceEnter(log, "isSegmentOpen", segment);

        final CompletableFuture<SegmentValidityResponse> result = this.retryConfig.runAsync(() -> {
            RPCAsyncCallback<SegmentValidityResponse> callback = new RPCAsyncCallback<>(traceId, "isSegmentOpen", segment);
            client.withDeadlineAfter(timeoutMillis, TimeUnit.MILLISECONDS).isSegmentValid(ModelHelper.createSegmentId(segment.getScope(),
                    segment.getStreamName(),
                    segment.getSegmentId()),
                    callback);
            return callback.getFuture();
        }, this.executor);
        return result.thenApply(SegmentValidityResponse::getResponse)
                .whenComplete((x, e) -> {
                    if (e != null) {
                        log.warn("isSegmentOpen for segment {} failed: ", segment, e);
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
            RPCAsyncCallback<CreateTxnResponse> callback = new RPCAsyncCallback<>(traceId, "createTransaction", stream, lease);
            client.withDeadlineAfter(timeoutMillis, TimeUnit.MILLISECONDS).createTransaction(
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
                        log.warn("createTransaction on stream {} failed: ", stream.getStreamName(), e);
                    }
                    LoggerHelpers.traceLeave(log, "createTransaction", traceId);
                });
    }

    private TxnSegments convert(CreateTxnResponse response) {
        NavigableMap<Double, SegmentWithRange> rangeMap = new TreeMap<>();

        for (SegmentRange r : response.getActiveSegmentsList()) {
            Preconditions.checkState(r.getMinKey() <= r.getMaxKey());
            rangeMap.put(r.getMaxKey(), new SegmentWithRange(ModelHelper.encode(r.getSegmentId()), r.getMinKey(), r.getMaxKey()));
        }
        StreamSegments segments = new StreamSegments(rangeMap, response.getDelegationToken());
        return new TxnSegments(segments, ModelHelper.encode(response.getTxnId()));
    }

    @Override
    public CompletableFuture<Transaction.PingStatus> pingTransaction(final Stream stream, final UUID txId, final long lease) {
        Exceptions.checkNotClosed(closed.get(), this);
        long traceId = LoggerHelpers.traceEnter(log, "pingTransaction", stream, txId, lease);

        final CompletableFuture<PingTxnStatus> result = this.retryConfig.runAsync(() -> {
            RPCAsyncCallback<PingTxnStatus> callback = new RPCAsyncCallback<>(traceId, "pingTransaction", txId, lease);
            client.withDeadlineAfter(timeoutMillis, TimeUnit.MILLISECONDS).pingTransaction(PingTxnRequest.newBuilder()
                                                 .setStreamInfo(ModelHelper.createStreamInfo(stream.getScope(), stream.getStreamName()))
                                                 .setTxnId(ModelHelper.decode(txId))
                                                 .setLease(lease).build(), callback);
            return callback.getFuture();
        }, this.executor);
        return result.thenApply(status -> {
            try {
                return ModelHelper.encode(status.getStatus(), stream + " " + txId);
            } catch (PingFailedException ex) {
                throw new CompletionException(ex);
            }
        }).whenComplete((s, e) -> {
            if (e != null) {
                log.warn("PingTransaction {} failed:", txId, e);
            }
            LoggerHelpers.traceLeave(log, "pingTransaction", traceId);
        });
    }

    @Override
    public CompletableFuture<Void> commitTransaction(final Stream stream, final String writerId, final Long timestamp, final UUID txId) {
        Exceptions.checkNotClosed(closed.get(), this);
        Preconditions.checkNotNull(stream, "stream");
        Preconditions.checkNotNull(txId, "txId");
        long traceId = LoggerHelpers.traceEnter(log, "commitTransaction", stream, txId);

        final CompletableFuture<TxnStatus> result = this.retryConfig.runAsync(() -> {
            RPCAsyncCallback<TxnStatus> callback = new RPCAsyncCallback<>(traceId, "commitTransaction", stream, writerId, timestamp, txId);
            TxnRequest.Builder txnRequest = TxnRequest.newBuilder()
                                                      .setStreamInfo(ModelHelper.createStreamInfo(stream.getScope(),
                                                                                                  stream.getStreamName()))
                                                      .setWriterId(writerId)
                                                      .setTxnId(ModelHelper.decode(txId));
            if (timestamp != null) {
                txnRequest.setTimestamp(timestamp);
            } else {
                txnRequest.setTimestamp(Long.MIN_VALUE);
            }
            client.withDeadlineAfter(timeoutMillis, TimeUnit.MILLISECONDS).commitTransaction(txnRequest.build(), callback);
            return callback.getFuture();
        }, this.executor);
        return result.thenApply(txnStatus -> {
            if (txnStatus.getStatus().equals(TxnStatus.Status.STREAM_NOT_FOUND)) {
                log.warn("Stream not found: {}", stream.getStreamName());
                throw new InvalidStreamException("Stream no longer exists: " + stream);
            }
            if (txnStatus.getStatus().equals(TxnStatus.Status.TRANSACTION_NOT_FOUND)) {
                log.warn("transaction not found: {}", txId);
                throw Exceptions.sneakyThrow(new TxnFailedException("Transaction was already either committed or aborted"));
            }
            if (txnStatus.getStatus().equals(TxnStatus.Status.SUCCESS)) {                
                return null;
            }
            log.warn("Unable to commit transaction {} commit status is {}", txId, txnStatus.getStatus());
            throw Exceptions.sneakyThrow(new TxnFailedException("Commit transaction failed with status: " + txnStatus.getStatus()));
        });
    }

    @Override
    public CompletableFuture<Void> abortTransaction(final Stream stream, final UUID txId) {
        Exceptions.checkNotClosed(closed.get(), this);
        Preconditions.checkNotNull(stream, "stream");
        Preconditions.checkNotNull(txId, "txId");
        long traceId = LoggerHelpers.traceEnter(log, "abortTransaction", stream, txId);

        final CompletableFuture<TxnStatus> result = this.retryConfig.runAsync(() -> {
            RPCAsyncCallback<TxnStatus> callback = new RPCAsyncCallback<>(traceId, "abortTransaction", stream, txId);
            client.withDeadlineAfter(timeoutMillis, TimeUnit.MILLISECONDS).abortTransaction(TxnRequest.newBuilder()
                            .setStreamInfo(ModelHelper.createStreamInfo(stream.getScope(),
                                    stream.getStreamName()))
                            .setTxnId(ModelHelper.decode(txId))
                            .build(),
                    callback);
            return callback.getFuture();
        }, this.executor);
        return result.thenApply(txnStatus -> {
            LoggerHelpers.traceLeave(log, "abortTransaction", traceId);
            if (txnStatus.getStatus().equals(TxnStatus.Status.STREAM_NOT_FOUND)) {
                log.warn("Stream not found: {}", stream.getStreamName());
                throw new InvalidStreamException("Stream no longer exists: " + stream);
            }
            if (txnStatus.getStatus().equals(TxnStatus.Status.TRANSACTION_NOT_FOUND)) {
                log.warn("transaction not found: {}", txId);
                throw Exceptions.sneakyThrow(new TxnFailedException("Transaction was already either committed or aborted"));
            }
            if (txnStatus.getStatus().equals(TxnStatus.Status.SUCCESS)) {                
                return null;
            }
            log.warn("Unable to abort transaction {} abort status is {} ", txId, txnStatus.getStatus());
            throw new RuntimeException("Error aborting transaction: " + txnStatus.getStatus());
        });
    }

    @Override
    public CompletableFuture<Transaction.Status> checkTransactionStatus(final Stream stream, final UUID txId) {
        Exceptions.checkNotClosed(closed.get(), this);
        Preconditions.checkNotNull(stream, "stream");
        Preconditions.checkNotNull(txId, "txId");
        long traceId = LoggerHelpers.traceEnter(log, "checkTransactionStatus", stream, txId);

        final CompletableFuture<TxnState> result = this.retryConfig.runAsync(() -> {
            RPCAsyncCallback<TxnState> callback = new RPCAsyncCallback<>(traceId, "checkTransactionStatus", stream, txId);
            client.withDeadlineAfter(timeoutMillis, TimeUnit.MILLISECONDS).checkTransactionState(TxnRequest.newBuilder()
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
                        log.warn("checkTransactionStatus on " + stream + " " + txId + " failed: ", e);
                    }
                    LoggerHelpers.traceLeave(log, "checkTransactionStatus", traceId);
                });
    }

    @Override
    public CompletableFuture<Void> noteTimestampFromWriter(String writer, Stream stream, long timestamp, WriterPosition lastWrittenPosition) {
        Exceptions.checkNotClosed(closed.get(), this);
        Preconditions.checkNotNull(stream, "stream");
        Preconditions.checkNotNull(writer, "writer");
        Preconditions.checkNotNull(lastWrittenPosition, "lastWrittenPosition");
        long traceId = LoggerHelpers.traceEnter(log, "noteTimestampFromWriter", writer, stream);

        final CompletableFuture<TimestampResponse> result = this.retryConfig.runAsync(() -> {
            RPCAsyncCallback<TimestampResponse> callback = new RPCAsyncCallback<>(traceId, "lastWrittenPosition", writer, stream, timestamp);
            client.withDeadlineAfter(timeoutMillis, TimeUnit.MILLISECONDS).noteTimestampFromWriter(TimestampFromWriter.newBuilder()
                                                              .setWriter(writer)
                                                              .setTimestamp(timestamp)
                                                              .setPosition(ModelHelper.createStreamCut(stream, lastWrittenPosition))
                                                              .build(),
                                           callback);
            return callback.getFuture();
        }, this.executor);
        return result.thenApply(response -> {
            LoggerHelpers.traceLeave(log, "noteTimestampFromWriter", traceId);
            if (response.getResult().equals(TimestampResponse.Status.SUCCESS)) {
                return null;
            }
            log.warn("Writer " + writer + " failed to note time because: " + response.getResult()
                    + " time was: " + timestamp + " position=" + lastWrittenPosition);
            throw new RuntimeException("failed to note time because: " + response.getResult());
        });
    }

    @Override
    public CompletableFuture<Void> removeWriter(String writerId, Stream stream) {
        Exceptions.checkNotClosed(closed.get(), this);
        Preconditions.checkNotNull(stream, "stream");
        Preconditions.checkNotNull(writerId, "writerId");
        long traceId = LoggerHelpers.traceEnter(log, "writerShutdown", writerId, stream);
        final CompletableFuture<RemoveWriterResponse> result = this.retryConfig.runAsync(() -> {
            RPCAsyncCallback<RemoveWriterResponse> callback = new RPCAsyncCallback<>(traceId, "writerShutdown", writerId, stream);
            client.withDeadlineAfter(timeoutMillis, TimeUnit.MILLISECONDS).removeWriter(RemoveWriterRequest.newBuilder()
                                                       .setWriter(writerId)
                                                       .setStream(ModelHelper.createStreamInfo(stream.getScope(),
                                                                                               stream.getStreamName()))
                                                       .build(),
                                  callback);
            return callback.getFuture();
        }, this.executor);
        return result.thenApply(response -> {
            LoggerHelpers.traceLeave(log, "writerShutdown", traceId);
            if (response.getResult().equals(RemoveWriterResponse.Status.SUCCESS)) {
                return null;
            }
            log.warn("Notifying the controller of writer shutdown failed for writer: " + writerId + " because of "
                    + response.getResult());
            throw new RuntimeException("Unable to remove writer due to: " + response.getResult());
        });
    }

    @Override
    public void close() {
        if (!closed.getAndSet(true)) {
            closeChannel();
        }
    }

    private void closeChannel() {
        this.channel.shutdownNow(); // Initiates a shutdown of channel. Although forceful, the shutdown is not instantaneous.
        Exceptions.handleInterrupted(() -> {
            boolean shutdownStatus = channel.awaitTermination(20, TimeUnit.SECONDS);
            log.debug("Controller client shutdown has been initiated. Channel status: channel.isTerminated():{}", shutdownStatus);
        });
    }

    @Override
    public CompletableFuture<String> getOrRefreshDelegationTokenFor(String scope, String streamName) {
        Exceptions.checkNotClosed(closed.get(), this);
        Exceptions.checkNotNullOrEmpty(scope, "scope");
        Exceptions.checkNotNullOrEmpty(streamName, "stream");
        long traceId = LoggerHelpers.traceEnter(log, "getOrRefreshDelegationTokenFor", scope, streamName);

        final CompletableFuture<DelegationToken> result = this.retryConfig.runAsync(() -> {
            RPCAsyncCallback<DelegationToken> callback = new RPCAsyncCallback<>(traceId, "getOrRefreshDelegationTokenFor", scope, streamName);
            client.withDeadlineAfter(timeoutMillis, TimeUnit.MILLISECONDS)
                  .getDelegationToken(ModelHelper.createStreamInfo(scope, streamName), callback);
            return callback.getFuture();
        }, this.executor);

        return result.thenApply( token -> token.getDelegationToken())
        .whenComplete((x, e) -> {
            if (e != null) {
                log.warn("getOrRefreshDelegationTokenFor {}/{} failed: ", scope, streamName, e);
            }
            LoggerHelpers.traceLeave(log, "getOrRefreshDelegationTokenFor", traceId);
        });
    }

    //region KeyValueTables

    @Override
    public CompletableFuture<Boolean> createKeyValueTable(String scope, String kvtName, KeyValueTableConfiguration kvtConfig) {
        Exceptions.checkNotNullOrEmpty(scope, "scope");
        Exceptions.checkNotClosed(closed.get(), this);
        Preconditions.checkNotNull(kvtConfig, "KeyValueTableConfig");
        final long requestId = requestIdGenerator.get();
        long traceId = LoggerHelpers.traceEnter(log, "createKeyValueTable", kvtConfig, requestId);

        final CompletableFuture<CreateKeyValueTableStatus> result = this.retryConfig.runAsync(() -> {
            RPCAsyncCallback<CreateKeyValueTableStatus> callback = new RPCAsyncCallback<>(requestId, "createKeyValueTable", scope, kvtName, kvtConfig);
            new ControllerClientTagger(client, timeoutMillis)
                    .withTag(requestId, "createKeyValueTable", scope, kvtName)
                    .createKeyValueTable(ModelHelper.decode(scope, kvtName, kvtConfig), callback);
            return callback.getFuture();
        }, this.executor);
        return result.thenApply(x -> {
            switch (x.getStatus()) {
                case FAILURE:
                    log.warn(requestId, "Failed to create KeyValueTable: {}", kvtName);
                    throw new ControllerFailureException("Failed to create KeyValueTable: " + kvtConfig);
                case INVALID_TABLE_NAME:
                    log.warn(requestId, "Illegal KeyValueTable name: {}", kvtName);
                    throw new IllegalArgumentException("Illegal KeyValueTable name: " + kvtName);
                case SCOPE_NOT_FOUND:
                    log.warn(requestId, "Scope not found: {}", scope);
                    throw new IllegalArgumentException("Scope does not exist: " + scope);
                case TABLE_EXISTS:
                    log.warn(requestId, "KeyValueTable already exists: {}", kvtName);
                    return false;
                case SUCCESS:
                    log.info(requestId, "KeyValueTable created successfully: {}/{}", scope, kvtName);
                    return true;
                case UNRECOGNIZED:
                default:
                    throw new ControllerFailureException("Unknown return status creating KeyValueTable " + kvtConfig
                            + " " + x.getStatus());
            }
        }).whenComplete((x, e) -> {
            if (e != null) {
                log.warn(requestId, "createKeyValueTable {}/{} failed: ", scope, kvtName, e);
            }
            LoggerHelpers.traceLeave(log, "createKeyValueTable", traceId, kvtName, requestId);
        });
    }

    @Override
    public AsyncIterator<KeyValueTableInfo> listKeyValueTables(String scopeName) {
        Exceptions.checkNotClosed(closed.get(), this);
        long traceId = LoggerHelpers.traceEnter(log, "listKeyValueTables", scopeName);
        long requestId = requestIdGenerator.get();
        try {
            final Function<ContinuationToken, CompletableFuture<Map.Entry<ContinuationToken, Collection<KeyValueTableInfo>>>> function =
                    token -> this.retryConfig.runAsync(() -> {
                        RPCAsyncCallback<KVTablesInScopeResponse> callback = new RPCAsyncCallback<>(requestId, "listKeyValueTables", scopeName);
                        ScopeInfo scopeInfo = ScopeInfo.newBuilder().setScope(scopeName).build();
                        new ControllerClientTagger(client, timeoutMillis).withTag(requestId, "listKeyValueTables", scopeName)
                                .listKeyValueTables(KVTablesInScopeRequest.newBuilder().setScope(scopeInfo)
                                                        .setContinuationToken(token).build(), callback);
                        return callback.getFuture()
                                .thenApply(x -> {
                                    switch (x.getStatus()) {
                                        case SCOPE_NOT_FOUND:
                                            log.warn(requestId, "Scope not found: {}", scopeName);
                                            throw new NoSuchScopeException();
                                        case FAILURE:
                                            log.warn(requestId, "Internal Server Error while trying to list streams in scope: {}", scopeName);
                                            throw new RuntimeException("Failure while trying to list streams");
                                        case SUCCESS:
                                            // we will treat all other case as success for backward
                                            // compatibility reasons
                                        default:
                                            List<KeyValueTableInfo> kvtList = x.getKvtablesList().stream()
                                                    .map(y -> new KeyValueTableInfo(y.getScope(), y.getKvtName())).collect(Collectors.toList());
                                            return new AbstractMap.SimpleEntry<>(x.getContinuationToken(), kvtList);
                                    }
                                });
                    }, this.executor);
            return new ContinuationTokenAsyncIterator<>(function, ContinuationToken.newBuilder().build());
        } finally {
            LoggerHelpers.traceLeave(log, "listKeyValueTables", traceId);
        }
    }

    @Override
    public CompletableFuture<Boolean> deleteKeyValueTable(String scope, String kvtName) {
        Exceptions.checkNotClosed(closed.get(), this);
        Exceptions.checkNotNullOrEmpty(scope, "scope");
        Exceptions.checkNotNullOrEmpty(kvtName, "KeyValueTableName");
        final long requestId = requestIdGenerator.get();
        long traceId = LoggerHelpers.traceEnter(log, "deleteKeyValueTable", scope, kvtName, requestId);

        final CompletableFuture<DeleteKVTableStatus> result = this.retryConfig.runAsync(() -> {
            RPCAsyncCallback<DeleteKVTableStatus> callback = new RPCAsyncCallback<>(requestId, "deleteKeyValueTable", scope, kvtName);
            new ControllerClientTagger(client, timeoutMillis).withTag(requestId, "deleteKeyValueTable", scope, kvtName)
                    .deleteKeyValueTable(ModelHelper.createKeyValueTableInfo(scope, kvtName), callback);
            return callback.getFuture();
        }, this.executor);
        return result.thenApply(x -> {
            switch (x.getStatus()) {
                case FAILURE:
                    log.warn(requestId, "Failed to delete KeyValueTable: {}", kvtName);
                    throw new ControllerFailureException("Failed to delete KeyValueTable: " + kvtName);
                case TABLE_NOT_FOUND:
                    log.warn(requestId, "KeyValueTable does not exist: {}", kvtName);
                    return false;
                case SUCCESS:
                    log.info(requestId, "Successfully deleted KeyValueTable: {}", kvtName);
                    return true;
                case UNRECOGNIZED:
                default:
                    throw new ControllerFailureException("Unknown return status deleting KeyValueTable " + kvtName + " " + x.getStatus());
            }
        }).whenComplete((x, e) -> {
            if (e != null) {
                log.warn(requestId, "deleteKeyValueTable {}/{} failed: ", scope, kvtName, e);
            }
            LoggerHelpers.traceLeave(log, "deleteKeyValueTable", traceId, scope, kvtName, requestId);
        });
    }

    @Override
    public CompletableFuture<KeyValueTableSegments> getCurrentSegmentsForKeyValueTable(String scope, String kvtName) {
            Exceptions.checkNotClosed(closed.get(), this);
            Exceptions.checkNotNullOrEmpty(scope, "scope");
            Exceptions.checkNotNullOrEmpty(kvtName, "stream");
            long traceId = LoggerHelpers.traceEnter(log, "getCurrentSegmentsForKeyValueTable", scope, kvtName);

            final CompletableFuture<SegmentRanges> result = this.retryConfig.runAsync(() -> {
                RPCAsyncCallback<SegmentRanges> callback = new RPCAsyncCallback<>(traceId, "getCurrentSegmentsKeyValueTable", scope, kvtName);
                client.withDeadlineAfter(timeoutMillis, TimeUnit.MILLISECONDS)
                        .getCurrentSegmentsKeyValueTable(ModelHelper.createKeyValueTableInfo(scope, kvtName), callback);
                return callback.getFuture();
            }, this.executor);
            return result.thenApply(ranges -> {
                log.debug("Received the following data from the controller {}", ranges.getSegmentRangesList());
                NavigableMap<Double, SegmentWithRange> rangeMap = new TreeMap<>();
                for (SegmentRange r : ranges.getSegmentRangesList()) {
                    Preconditions.checkState(r.getMinKey() <= r.getMaxKey(),
                            "Min keyrange %s was not less than maximum keyRange %s for segment %s",
                            r.getMinKey(), r.getMaxKey(), r.getSegmentId());
                    rangeMap.put(r.getMaxKey(), new SegmentWithRange(ModelHelper.encode(r.getSegmentId()), r.getMinKey(), r.getMaxKey()));
                }
                return new KeyValueTableSegments(rangeMap, ranges.getDelegationToken());
            }).whenComplete((x, e) -> {
                if (e != null) {
                    log.warn("getCurrentSegmentsForKeyValueTable for {}/{} failed: ", scope, kvtName, e);
                }
                LoggerHelpers.traceLeave(log, "getCurrentSegmentsForKeyValueTable", traceId);
            });
        }

    //endregion

    // Local callback definition to wrap gRPC responses in CompletableFutures used by the rest of our code.
    private static final class RPCAsyncCallback<T> implements StreamObserver<T> {
        private final long traceId;
        private final String method;
        private final Object[] parameters;
        private T result = null;
        private final CompletableFuture<T> future = new CompletableFuture<>();

        RPCAsyncCallback(long traceId, String method, Object... args) {
            this.traceId = traceId;
            this.method = method;
            parameters = args;
        }

        @Override
        public void onNext(T value) {
            result = value;
        }

        @Override
        public void onError(Throwable t) {
            log.warn("gRPC call for {} with trace id {} and parameters {} failed with server error.", method, traceId, parameters, t);
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
        private final long timeoutMillis;

        ControllerClientTagger(ControllerServiceStub clientStub, long timeoutMillis) {
            this.clientStub = clientStub;
            this.timeoutMillis = timeoutMillis;
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
            clientStub.withDeadlineAfter(timeoutMillis, TimeUnit.MILLISECONDS)
                    .createScope(scopeInfo, callback);
        }

        public void listScopes(ScopesRequest request, 
                                       RPCAsyncCallback<ScopesResponse> callback) {
            clientStub.withDeadlineAfter(timeoutMillis, TimeUnit.MILLISECONDS)
                      .listScopes(request, callback);
        }

        public void checkScopeExists(ScopeInfo request, RPCAsyncCallback<ExistsResponse> callback) {
            clientStub.withDeadlineAfter(timeoutMillis, TimeUnit.MILLISECONDS)
                      .checkScopeExists(request, callback);
        }

        public void checkStreamExists(StreamInfo request, RPCAsyncCallback<ExistsResponse> callback) {
            clientStub.withDeadlineAfter(timeoutMillis, TimeUnit.MILLISECONDS)
                      .checkStreamExists(request, callback);
        }
        
        public void listStreamsInScope(StreamsInScopeRequest request, 
                                       RPCAsyncCallback<StreamsInScopeResponse> callback) {
            clientStub.withDeadlineAfter(timeoutMillis, TimeUnit.MILLISECONDS)
                      .listStreamsInScope(request, callback);
        }

        public void deleteScope(ScopeInfo scopeInfo, RPCAsyncCallback<DeleteScopeStatus> callback) {
            clientStub.withDeadlineAfter(timeoutMillis, TimeUnit.MILLISECONDS)
                      .deleteScope(scopeInfo, callback);
        }

        public void createStream(StreamConfig streamConfig, RPCAsyncCallback<CreateStreamStatus> callback) {
            clientStub.withDeadlineAfter(timeoutMillis, TimeUnit.MILLISECONDS)
                      .createStream(streamConfig, callback);
        }

        public void scale(ScaleRequest scaleRequest, RPCAsyncCallback<ScaleResponse> callback) {
            clientStub.withDeadlineAfter(timeoutMillis, TimeUnit.MILLISECONDS)
                      .scale(scaleRequest, callback);
        }

        public void updateStream(StreamConfig streamConfig, RPCAsyncCallback<UpdateStreamStatus> callback) {
            clientStub.withDeadlineAfter(timeoutMillis, TimeUnit.MILLISECONDS)
                      .updateStream(streamConfig, callback);
        }

        public void truncateStream(io.pravega.controller.stream.api.grpc.v1.Controller.StreamCut streamCut,
                                   RPCAsyncCallback<UpdateStreamStatus> callback) {
            clientStub.withDeadlineAfter(timeoutMillis, TimeUnit.MILLISECONDS)
                      .truncateStream(streamCut, callback);
        }

        public void sealStream(StreamInfo streamInfo, RPCAsyncCallback<UpdateStreamStatus> callback) {
            clientStub.withDeadlineAfter(timeoutMillis, TimeUnit.MILLISECONDS)
                      .sealStream(streamInfo, callback);
        }

        public void deleteStream(StreamInfo streamInfo, RPCAsyncCallback<DeleteStreamStatus> callback) {
            clientStub.withDeadlineAfter(timeoutMillis, TimeUnit.MILLISECONDS)
                      .deleteStream(streamInfo, callback);
        }

        public void addSubscriber(StreamSubscriberInfo streamSubscriberInfo, RPCAsyncCallback<AddSubscriberStatus> callback) {
            clientStub.withDeadlineAfter(timeoutMillis, TimeUnit.MILLISECONDS)
                    .addSubscriber(streamSubscriberInfo, callback);
        }

        public void deleteSubscriber(StreamSubscriberInfo streamSubscriberInfo, RPCAsyncCallback<DeleteSubscriberStatus> callback) {
            clientStub.withDeadlineAfter(timeoutMillis, TimeUnit.MILLISECONDS)
                    .deleteSubscriber(streamSubscriberInfo, callback);
        }

        public void updateTruncationStreamCut(SubscriberStreamCut subscriberStreamCut, RPCAsyncCallback<UpdateSubscriberStatus> callback) {
            clientStub.withDeadlineAfter(timeoutMillis, TimeUnit.MILLISECONDS)
                    .updateTruncationStreamCut(subscriberStreamCut, callback);
        }

        public void listSubscribers(StreamInfo request,
                               RPCAsyncCallback<SubscribersResponse> callback) {
            clientStub.withDeadlineAfter(timeoutMillis, TimeUnit.MILLISECONDS)
                    .listSubscribers(request, callback);
        }

        public void createKeyValueTable(KeyValueTableConfig kvtConfig, RPCAsyncCallback<CreateKeyValueTableStatus> callback) {
            clientStub.withDeadlineAfter(timeoutMillis, TimeUnit.MILLISECONDS)
                    .createKeyValueTable(kvtConfig, callback);
        }

        void listKeyValueTables(KVTablesInScopeRequest request,
                                       RPCAsyncCallback<KVTablesInScopeResponse> callback) {
            clientStub.withDeadlineAfter(timeoutMillis, TimeUnit.MILLISECONDS)
                    .listKeyValueTablesInScope(request, callback);
        }

        void deleteKeyValueTable(io.pravega.controller.stream.api.grpc.v1.Controller.KeyValueTableInfo kvtInfo, RPCAsyncCallback<DeleteKVTableStatus> callback) {
            clientStub.withDeadlineAfter(timeoutMillis, TimeUnit.MILLISECONDS)
                    .deleteKeyValueTable(kvtInfo, callback);
        }
    }
}
