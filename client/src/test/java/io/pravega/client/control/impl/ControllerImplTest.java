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

import com.google.common.base.Strings;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import io.pravega.client.ClientConfig;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.InvalidStreamException;
import io.pravega.client.stream.NoSuchScopeException;
import io.pravega.client.stream.PingFailedException;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.StreamCut;
import io.pravega.client.stream.Transaction;
import io.pravega.client.stream.TxnFailedException;
import io.pravega.client.stream.impl.DefaultCredentials;
import io.pravega.client.stream.impl.SegmentWithRange;
import io.pravega.client.stream.impl.StreamCutImpl;
import io.pravega.client.stream.impl.StreamImpl;
import io.pravega.client.stream.impl.StreamSegmentSuccessors;
import io.pravega.client.stream.impl.StreamSegments;
import io.pravega.client.stream.impl.StreamSegmentsWithPredecessors;
import io.pravega.client.stream.impl.TxnSegments;
import io.pravega.client.stream.impl.WriterPosition;
import io.pravega.client.tables.KeyValueTableConfiguration;
import io.pravega.client.tables.impl.KeyValueTableSegments;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.util.AsyncIterator;
import io.pravega.common.util.RetriesExhaustedException;
import io.pravega.controller.stream.api.grpc.v1.Controller;
import io.pravega.controller.stream.api.grpc.v1.Controller.CreateScopeStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.CreateStreamStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.CreateTxnRequest;
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
import io.pravega.controller.stream.api.grpc.v1.Controller.SegmentId;
import io.pravega.controller.stream.api.grpc.v1.Controller.SegmentRanges;
import io.pravega.controller.stream.api.grpc.v1.Controller.SegmentValidityResponse;
import io.pravega.controller.stream.api.grpc.v1.Controller.SegmentsAtTime;
import io.pravega.controller.stream.api.grpc.v1.Controller.SegmentsAtTime.SegmentLocation;
import io.pravega.controller.stream.api.grpc.v1.Controller.StreamConfig;
import io.pravega.controller.stream.api.grpc.v1.Controller.StreamInfo;
import io.pravega.controller.stream.api.grpc.v1.Controller.GetEpochSegmentsRequest;
import io.pravega.controller.stream.api.grpc.v1.Controller.SuccessorResponse;
import io.pravega.controller.stream.api.grpc.v1.Controller.TxnId;
import io.pravega.controller.stream.api.grpc.v1.Controller.TxnRequest;
import io.pravega.controller.stream.api.grpc.v1.Controller.TxnState;
import io.pravega.controller.stream.api.grpc.v1.Controller.UpdateStreamStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.KeyValueTableConfig;
import io.pravega.controller.stream.api.grpc.v1.Controller.CreateKeyValueTableStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.KeyValueTableInfo;
import io.pravega.controller.stream.api.grpc.v1.Controller.DeleteKVTableStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.AddSubscriberStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.DeleteSubscriberStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.UpdateSubscriberStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.StreamSubscriberInfo;
import io.pravega.controller.stream.api.grpc.v1.Controller.SubscriberStreamCut;
import io.pravega.controller.stream.api.grpc.v1.Controller.SubscribersResponse;
import io.pravega.controller.stream.api.grpc.v1.ControllerServiceGrpc.ControllerServiceImplBase;
import io.pravega.shared.NameUtils;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.SecurityConfigDefaults;
import io.pravega.test.common.TestUtils;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import lombok.Cleanup;
import lombok.val;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import static io.pravega.test.common.AssertExtensions.assertThrows;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Unit tests for ControllerImpl.
 *
 */
@Slf4j
public class ControllerImplTest {
    private static final int SERVICE_PORT = 12345;
    private static final String NON_EXISTENT = "non-existent";
    private static final String FAILING = "failing";

    @Rule
    public final Timeout globalTimeout = new Timeout(120, TimeUnit.SECONDS);
    boolean testSecure;

    // Global variable to track number of attempts to verify retries.
    private final AtomicInteger retryAttempts = new AtomicInteger(0);

    // Test implementation for simulating the server responses.
    private ControllerServiceImplBase testServerImpl;
    private Server testGRPCServer = null;

    private int serverPort;

    // The controller RPC client.
    private ControllerImpl controllerClient = null;
    private ScheduledExecutorService executor;
    private NettyServerBuilder serverBuilder;
    private final AtomicReference<Object> lastRequest = new AtomicReference<>();

    @Before
    public void setup() throws IOException {

        // Setup test server generating different success and failure responses.
        testServerImpl = new ControllerServiceImplBase() {
            @Override
            public void listScopes(Controller.ScopesRequest request, StreamObserver<Controller.ScopesResponse> responseObserver) {
                if (Strings.isNullOrEmpty(request.getContinuationToken().getToken())) {
                    List<String> list1 = new LinkedList<>();
                    list1.add("scope1");
                    list1.add("scope2");
                    responseObserver.onNext(Controller.ScopesResponse
                            .newBuilder().addAllScopes(list1)
                            .setContinuationToken(Controller.ContinuationToken.newBuilder().setToken("myToken").build()).build());
                    responseObserver.onCompleted();
                } else if (request.getContinuationToken().getToken().equals("myToken")) {
                    List<String> list2 = new LinkedList<>();
                    list2.add("scope3");

                    responseObserver.onNext(Controller.ScopesResponse
                            .newBuilder().addAllScopes(list2)
                            .setContinuationToken(Controller.ContinuationToken.newBuilder().setToken("myToken2").build()).build());
                    responseObserver.onCompleted();
                } else if (request.getContinuationToken().getToken().equals("myToken2")) {
                    List<String> list3 = new LinkedList<>();
                    responseObserver.onNext(Controller.ScopesResponse
                            .newBuilder().addAllScopes(list3)
                            .setContinuationToken(Controller.ContinuationToken.newBuilder().setToken("myToken2").build()).build());
                    responseObserver.onCompleted();
                } else {
                    responseObserver.onError(Status.INTERNAL.withDescription("Server error").asRuntimeException());
                }
            }

            @Override
            public void checkScopeExists(ScopeInfo request, StreamObserver<Controller.ExistsResponse> responseObserver) {
                if (!request.getScope().equals("throwing")) {
                    responseObserver.onNext(Controller.ExistsResponse.newBuilder()
                                                                     .setExists(request.getScope().equals("scope1"))
                                                                     .build());
                    responseObserver.onCompleted();
                } else {
                    responseObserver.onError(Status.INTERNAL.withDescription("Server error").asRuntimeException());
                }
            }

            @Override
            public void checkStreamExists(StreamInfo request, StreamObserver<Controller.ExistsResponse> responseObserver) {
                if (!request.getScope().equals("throwing")) {
                    responseObserver.onNext(Controller.ExistsResponse.newBuilder()
                                                                 .setExists(request.getStream().equals("stream1")).build());
                    responseObserver.onCompleted();
                } else {
                    responseObserver.onError(Status.INTERNAL.withDescription("Server error").asRuntimeException());
                }
            }

            @Override
            public void createStream(StreamConfig request,
                    StreamObserver<CreateStreamStatus> responseObserver) {
                if (request.getStreamInfo().getStream().equals("stream1") ||
                        request.getStreamInfo().getStream().equals("stream8")) {
                    responseObserver.onNext(CreateStreamStatus.newBuilder()
                                                    .setStatus(CreateStreamStatus.Status.SUCCESS)
                                                    .build());
                    responseObserver.onCompleted();
                } else if (request.getStreamInfo().getStream().equals("stream2")) {
                    responseObserver.onNext(CreateStreamStatus.newBuilder()
                                                    .setStatus(CreateStreamStatus.Status.FAILURE)
                                                    .build());
                    responseObserver.onCompleted();
                } else if (request.getStreamInfo().getStream().equals("stream3")) {
                    responseObserver.onNext(CreateStreamStatus.newBuilder()
                                                    .setStatus(CreateStreamStatus.Status.SCOPE_NOT_FOUND)
                                                    .build());
                    responseObserver.onCompleted();
                } else if (request.getStreamInfo().getStream().equals("stream4")) {
                    responseObserver.onNext(CreateStreamStatus.newBuilder()
                                                    .setStatus(CreateStreamStatus.Status.STREAM_EXISTS)
                                                    .build());
                    responseObserver.onCompleted();
                } else if (request.getStreamInfo().getStream().equals("stream5")) {
                    responseObserver.onNext(CreateStreamStatus.newBuilder()
                                                    .setStatus(CreateStreamStatus.Status.INVALID_STREAM_NAME)
                                                    .build());
                    responseObserver.onCompleted();
                } else if (request.getStreamInfo().getStream().equals("streamparallel")) {

                    // Simulating delay in sending response.
                    Exceptions.handleInterrupted(() -> Thread.sleep(500));
                    responseObserver.onNext(CreateStreamStatus.newBuilder()
                                                    .setStatus(CreateStreamStatus.Status.SUCCESS)
                                                    .build());
                    responseObserver.onCompleted();
                } else if (request.getStreamInfo().getStream().equals("streamdelayed")) {

                    // Simulating delay in sending response. This is used for the keepalive test,
                    // where response time > 30 seconds is required to simulate a failure.
                    Exceptions.handleInterrupted(() -> Thread.sleep(40000));
                    responseObserver.onNext(CreateStreamStatus.newBuilder()
                            .setStatus(CreateStreamStatus.Status.SUCCESS)
                            .build());
                    responseObserver.onCompleted();
                } else if (request.getStreamInfo().getStream().equals("streamretryfailure")) {
                    responseObserver.onError(Status.UNKNOWN.withDescription("Transport error").asRuntimeException());
                } else if (request.getStreamInfo().getStream().equals("streamretrysuccess")) {
                    if (retryAttempts.incrementAndGet() > 3) {
                        responseObserver.onNext(CreateStreamStatus.newBuilder()
                                .setStatus(CreateStreamStatus.Status.SUCCESS)
                                .build());
                        responseObserver.onCompleted();
                    } else {
                        responseObserver.onError(Status.INTERNAL.withDescription("Server error").asRuntimeException());
                    }
                } else if (request.getStreamInfo().getStream().equals("deadline")) {
                    // dont send any response
                } else {
                    responseObserver.onError(Status.INTERNAL.withDescription("Server error").asRuntimeException());
                }
            }

            @Override
            public void updateStream(StreamConfig request,
                    StreamObserver<UpdateStreamStatus> responseObserver) {
                if (request.getStreamInfo().getStream().equals("stream1")) {
                    responseObserver.onNext(UpdateStreamStatus.newBuilder()
                                                    .setStatus(UpdateStreamStatus.Status.SUCCESS)
                                                    .build());
                    responseObserver.onCompleted();
                } else if (request.getStreamInfo().getStream().equals("stream2")) {
                    responseObserver.onNext(UpdateStreamStatus.newBuilder()
                                                    .setStatus(UpdateStreamStatus.Status.FAILURE)
                                                    .build());
                    responseObserver.onCompleted();
                } else if (request.getStreamInfo().getStream().equals("stream3")) {
                    responseObserver.onNext(UpdateStreamStatus.newBuilder()
                                                    .setStatus(UpdateStreamStatus.Status.SCOPE_NOT_FOUND)
                                                    .build());
                    responseObserver.onCompleted();
                } else if (request.getStreamInfo().getStream().equals("stream4")) {
                    responseObserver.onNext(UpdateStreamStatus.newBuilder()
                                                    .setStatus(UpdateStreamStatus.Status.STREAM_NOT_FOUND)
                                                    .build());
                    responseObserver.onCompleted();
                } else if (request.getStreamInfo().getStream().equals("stream5")) {
                    responseObserver.onNext(UpdateStreamStatus.newBuilder()
                            .setStatus(UpdateStreamStatus.Status.UNRECOGNIZED)
                            .build());
                    responseObserver.onCompleted();
                } else if (request.getStreamInfo().getStream().equals("deadline")) {
                    // dont send any response
                } else {
                    responseObserver.onError(Status.INTERNAL.withDescription("Server error").asRuntimeException());
                }
            }

            @Override
            public void addSubscriber(StreamSubscriberInfo request,
                                     StreamObserver<AddSubscriberStatus> responseObserver) {
                if (request.getStream().equals("stream1")) {
                    responseObserver.onNext(AddSubscriberStatus.newBuilder()
                            .setStatus(AddSubscriberStatus.Status.SUCCESS)
                            .build());
                    responseObserver.onCompleted();
                } else if (request.getStream().equals("stream2")) {
                    responseObserver.onNext(AddSubscriberStatus.newBuilder()
                            .setStatus(AddSubscriberStatus.Status.FAILURE)
                            .build());
                    responseObserver.onCompleted();
                } else if (request.getStream().equals("stream3")) {
                    responseObserver.onNext(AddSubscriberStatus.newBuilder()
                            .setStatus(AddSubscriberStatus.Status.STREAM_NOT_FOUND)
                            .build());
                    responseObserver.onCompleted();
                } else if (request.getStream().equals("stream4")) {
                    responseObserver.onNext(AddSubscriberStatus.newBuilder()
                            .setStatus(AddSubscriberStatus.Status.UNRECOGNIZED)
                            .build());
                    responseObserver.onCompleted();
                } else if (request.getStream().equals("stream5")) {
                    responseObserver.onNext(AddSubscriberStatus.newBuilder()
                            .setStatus(AddSubscriberStatus.Status.SUBSCRIBER_EXISTS)
                            .build());
                    responseObserver.onCompleted();
                } else if (request.getStream().equals("deadline")) {
                    // dont send any response
                } else {
                    responseObserver.onError(Status.INTERNAL.withDescription("Server error").asRuntimeException());
                }
            }

            @Override
            public void deleteSubscriber(StreamSubscriberInfo request,
                                     StreamObserver<DeleteSubscriberStatus> responseObserver) {
                if (request.getStream().equals("stream1")) {
                    responseObserver.onNext(DeleteSubscriberStatus.newBuilder()
                            .setStatus(DeleteSubscriberStatus.Status.SUCCESS)
                            .build());
                    responseObserver.onCompleted();
                } else if (request.getStream().equals("stream2")) {
                    responseObserver.onNext(DeleteSubscriberStatus.newBuilder()
                            .setStatus(DeleteSubscriberStatus.Status.FAILURE)
                            .build());
                    responseObserver.onCompleted();
                } else if (request.getStream().equals("stream3")) {
                    responseObserver.onNext(DeleteSubscriberStatus.newBuilder()
                            .setStatus(DeleteSubscriberStatus.Status.STREAM_NOT_FOUND)
                            .build());
                    responseObserver.onCompleted();
                } else if (request.getStream().equals("stream4")) {
                    responseObserver.onNext(DeleteSubscriberStatus.newBuilder()
                            .setStatus(DeleteSubscriberStatus.Status.SUBSCRIBER_NOT_FOUND)
                            .build());
                    responseObserver.onCompleted();
                } else if (request.getStream().equals("stream5")) {
                    responseObserver.onNext(DeleteSubscriberStatus.newBuilder()
                            .setStatus(DeleteSubscriberStatus.Status.UNRECOGNIZED)
                            .build());
                    responseObserver.onCompleted();
                } else if (request.getStream().equals("deadline")) {
                    // dont send any response
                } else {
                    responseObserver.onError(Status.INTERNAL.withDescription("Server error").asRuntimeException());
                }
            }

            @Override
            public void listSubscribers(StreamInfo request,
                                         StreamObserver<SubscribersResponse> responseObserver) {
                if (request.getStream().equals("stream1")) {
                    responseObserver.onNext(SubscribersResponse.newBuilder().setStatus(SubscribersResponse.Status.SUCCESS)
                            .addSubscribers("sub1")
                            .addSubscribers("sub2").addSubscribers("sub3")
                            .build());
                    responseObserver.onCompleted();
                } else if (request.getStream().equals("stream2")) {
                    responseObserver.onNext(SubscribersResponse.newBuilder().build());
                    responseObserver.onCompleted();
                } else if (request.getStream().equals("deadline")) {
                    // dont send any response
                } else {
                    responseObserver.onError(Status.INTERNAL.withDescription("Server error").asRuntimeException());
                }
            }

            @Override
            public void updateTruncationStreamCut(SubscriberStreamCut request,
                                     StreamObserver<UpdateSubscriberStatus> responseObserver) {
                if (request.getStreamCut().getStreamInfo().getStream().equals("stream1")) {
                    responseObserver.onNext(UpdateSubscriberStatus.newBuilder()
                            .setStatus(UpdateSubscriberStatus.Status.SUCCESS)
                            .build());
                    responseObserver.onCompleted();
                } else if (request.getStreamCut().getStreamInfo().getStream().equals("stream2")) {
                    responseObserver.onNext(UpdateSubscriberStatus.newBuilder()
                            .setStatus(UpdateSubscriberStatus.Status.FAILURE)
                            .build());
                    responseObserver.onCompleted();
                } else if (request.getStreamCut().getStreamInfo().getStream().equals("stream3")) {
                    responseObserver.onNext(UpdateSubscriberStatus.newBuilder()
                            .setStatus(UpdateSubscriberStatus.Status.STREAM_NOT_FOUND)
                            .build());
                    responseObserver.onCompleted();
                } else if (request.getStreamCut().getStreamInfo().getStream().equals("stream4")) {
                    responseObserver.onNext(UpdateSubscriberStatus.newBuilder()
                            .setStatus(UpdateSubscriberStatus.Status.UNRECOGNIZED)
                            .build());
                    responseObserver.onCompleted();
                } else if (request.getStreamCut().getStreamInfo().getStream().equals("stream5")) {
                    responseObserver.onNext(UpdateSubscriberStatus.newBuilder()
                            .setStatus(UpdateSubscriberStatus.Status.STREAMCUT_NOT_VALID)
                            .build());
                    responseObserver.onCompleted();
                } else if (request.getStreamCut().getStreamInfo().getStream().equals("stream6")) {
                    responseObserver.onNext(UpdateSubscriberStatus.newBuilder()
                            .setStatus(UpdateSubscriberStatus.Status.SUBSCRIBER_NOT_FOUND)
                            .build());
                    responseObserver.onCompleted();
                } else if (request.getStreamCut().getStreamInfo().getStream().equals("deadline")) {
                    // dont send any response
                } else {
                    responseObserver.onError(Status.INTERNAL.withDescription("Server error").asRuntimeException());
                }
            }

            @Override
            public void truncateStream(Controller.StreamCut request,
                    StreamObserver<UpdateStreamStatus> responseObserver) {
                if (request.getStreamInfo().getStream().equals("stream1")) {
                    responseObserver.onNext(UpdateStreamStatus.newBuilder()
                                                    .setStatus(UpdateStreamStatus.Status.SUCCESS)
                                                    .build());
                    responseObserver.onCompleted();
                } else if (request.getStreamInfo().getStream().equals("stream2")) {
                    responseObserver.onNext(UpdateStreamStatus.newBuilder()
                                                    .setStatus(UpdateStreamStatus.Status.FAILURE)
                                                    .build());
                    responseObserver.onCompleted();
                } else if (request.getStreamInfo().getStream().equals("stream3")) {
                    responseObserver.onNext(UpdateStreamStatus.newBuilder()
                                                    .setStatus(UpdateStreamStatus.Status.SCOPE_NOT_FOUND)
                                                    .build());
                    responseObserver.onCompleted();
                } else if (request.getStreamInfo().getStream().equals("stream4")) {
                    responseObserver.onNext(UpdateStreamStatus.newBuilder()
                                                    .setStatus(UpdateStreamStatus.Status.STREAM_NOT_FOUND)
                                                    .build());
                    responseObserver.onCompleted();
                } else if (request.getStreamInfo().getStream().equals("stream5")) {
                    responseObserver.onNext(UpdateStreamStatus.newBuilder()
                            .setStatus(UpdateStreamStatus.Status.UNRECOGNIZED)
                            .build());
                    responseObserver.onCompleted();
                } else if (request.getStreamInfo().getStream().equals("deadline")) {
                    // dont send any response
                } else {
                    responseObserver.onError(Status.INTERNAL.withDescription("Server error").asRuntimeException());
                }
            }

            @Override
            public void sealStream(StreamInfo request, StreamObserver<UpdateStreamStatus> responseObserver) {
                if (request.getStream().equals("stream1")) {
                    responseObserver.onNext(UpdateStreamStatus.newBuilder()
                                                    .setStatus(UpdateStreamStatus.Status.SUCCESS)
                                                    .build());
                    responseObserver.onCompleted();
                } else if (request.getStream().equals("stream2")) {
                    responseObserver.onNext(UpdateStreamStatus.newBuilder()
                                                    .setStatus(UpdateStreamStatus.Status.FAILURE)
                                                    .build());
                    responseObserver.onCompleted();
                } else if (request.getStream().equals("stream3")) {
                    responseObserver.onNext(UpdateStreamStatus.newBuilder()
                                                    .setStatus(UpdateStreamStatus.Status.SCOPE_NOT_FOUND)
                                                    .build());
                    responseObserver.onCompleted();
                } else if (request.getStream().equals("stream4")) {
                    responseObserver.onNext(UpdateStreamStatus.newBuilder()
                                                    .setStatus(UpdateStreamStatus.Status.STREAM_NOT_FOUND)
                                                    .build());
                    responseObserver.onCompleted();
                } else if (request.getStream().equals("deadline")) {
                    // dont send any response
                } else {
                    responseObserver.onError(Status.INTERNAL.withDescription("Server error").asRuntimeException());
                }
            }

            @Override
            public void deleteStream(StreamInfo request,
                                     StreamObserver<DeleteStreamStatus> responseObserver) {
                if (request.getStream().equals("stream1")) {
                    responseObserver.onNext(DeleteStreamStatus.newBuilder()
                            .setStatus(DeleteStreamStatus.Status.SUCCESS)
                            .build());
                    responseObserver.onCompleted();
                } else if (request.getStream().equals("stream2")) {
                    responseObserver.onNext(DeleteStreamStatus.newBuilder()
                            .setStatus(DeleteStreamStatus.Status.FAILURE)
                            .build());
                    responseObserver.onCompleted();
                } else if (request.getStream().equals("stream3")) {
                    responseObserver.onNext(DeleteStreamStatus.newBuilder()
                            .setStatus(DeleteStreamStatus.Status.STREAM_NOT_FOUND)
                            .build());
                    responseObserver.onCompleted();
                } else if (request.getStream().equals("stream4")) {
                    responseObserver.onNext(DeleteStreamStatus.newBuilder()
                            .setStatus(DeleteStreamStatus.Status.STREAM_NOT_SEALED)
                            .build());
                    responseObserver.onCompleted();
                } else if (request.getStream().equals("deadline")) {
                    // dont send any response
                } else {
                    responseObserver.onError(Status.INTERNAL.withDescription("Server error").asRuntimeException());
                }
            }

            @Override
            public void getCurrentSegments(StreamInfo request,
                    StreamObserver<SegmentRanges> responseObserver) {
                if (request.getStream().equals("stream1")) {
                    responseObserver.onNext(SegmentRanges.newBuilder()
                                                    .addSegmentRanges(ModelHelper.createSegmentRange("scope1",
                                                                                                     "stream1",
                                                                                                     6,
                                                                                                     0.0,
                                                                                                     0.4))
                                                    .addSegmentRanges(ModelHelper.createSegmentRange("scope1",
                                                                                                     "stream1",
                                                                                                     7,
                                                                                                     0.4,
                                                                                                     1.0))
                                                    .build());
                    responseObserver.onCompleted();
                } else  if (request.getStream().equals("stream8")) {
                    responseObserver.onNext(SegmentRanges.newBuilder()
                                                         .addSegmentRanges(ModelHelper.createSegmentRange("scope1",
                                                                 "stream8",
                                                                 9,
                                                                 0.0,
                                                                 0.5))
                                                         .addSegmentRanges(ModelHelper.createSegmentRange("scope1",
                                                                 "stream8",
                                                                 10,
                                                                 0.5,
                                                                 1.0))
                                                         .build());
                    responseObserver.onCompleted();
                } else if (request.getStream().equals("streamparallel")) {
                    Exceptions.handleInterrupted(() -> Thread.sleep(500));
                    responseObserver.onNext(SegmentRanges.newBuilder()
                                                    .addSegmentRanges(ModelHelper.createSegmentRange("scope1",
                                                                                                     "streamparallel",
                                                                                                     0,
                                                                                                     0.0,
                                                                                                     0.4))
                                                    .addSegmentRanges(ModelHelper.createSegmentRange("scope1",
                                                                                                     "streamparallel",
                                                                                                     1,
                                                                                                     0.4,
                                                                                                     1.0))
                                                    .build());
                    responseObserver.onCompleted();
                } else if (request.getStream().equals("deadline")) {
                    // dont send any response
                } else if (request.getStream().equals("sealedStream")) {
                    // empty response if the Stream is sealed.
                    responseObserver.onNext( SegmentRanges.newBuilder().build());
                    responseObserver.onCompleted();
                } else {
                    responseObserver.onError(Status.INTERNAL.withDescription("Server error").asRuntimeException());
                }
            }

            @Override
            public void getEpochSegments(GetEpochSegmentsRequest request, StreamObserver<io.pravega.controller.stream.api.grpc.v1.Controller.SegmentRanges> responseObserver) {
                if (request.getStreamInfo().getStream().equals("stream1")) {
                    responseObserver.onNext(SegmentRanges.newBuilder()
                                                         .addSegmentRanges(ModelHelper.createSegmentRange("scope1",
                                                                 "stream1",
                                                                 6,
                                                                 0.0,
                                                                 0.4))
                                                         .addSegmentRanges(ModelHelper.createSegmentRange("scope1",
                                                                 "stream1",
                                                                 7,
                                                                 0.4,
                                                                 1.0))
                                                         .build());
                    responseObserver.onCompleted();
                } else {
                    responseObserver.onError(Status.INTERNAL.withDescription("Server error").asRuntimeException());
                }
            }

            @Override
            public void getSegments(GetSegmentsRequest request, StreamObserver<SegmentsAtTime> responseObserver) {
                if (request.getStreamInfo().getStream().equals("stream1")) {
                    SegmentId segment1 = ModelHelper.createSegmentId("scope1", "stream1", 0);
                    SegmentId segment2 = ModelHelper.createSegmentId("scope1", "stream1", 1);
                    responseObserver.onNext(SegmentsAtTime.newBuilder()
                                                          .addSegments(SegmentLocation.newBuilder()
                                                                                      .setSegmentId(segment1)
                                                                                      .setOffset(10)
                                                                                      .build())
                                                          .addSegments(SegmentLocation.newBuilder()
                                                                                      .setSegmentId(segment2)
                                                                                      .setOffset(20)
                                                                                      .build())
                                                          .build());
                    responseObserver.onCompleted();
                } else if (request.getStreamInfo().getStream().equals("stream8")) {
                    SegmentId segment1 = ModelHelper.createSegmentId("scope1", "stream8", 0);
                    SegmentId segment2 = ModelHelper.createSegmentId("scope1", "stream8", 1);
                    SegmentId segment3 = ModelHelper.createSegmentId("scope1", "stream8", 2);
                    responseObserver.onNext(SegmentsAtTime.newBuilder()
                                                          .addSegments(SegmentLocation.newBuilder()
                                                                                      .setSegmentId(segment1)
                                                                                      .setOffset(10)
                                                                                      .build())
                                                          .addSegments(SegmentLocation.newBuilder()
                                                                                      .setSegmentId(segment2)
                                                                                      .setOffset(20)
                                                                                      .build())
                                                          .build());
                    responseObserver.onCompleted();
                } else if (request.getStreamInfo().getStream().equals("deadline")) {
                    // dont send any response
                } else {
                    responseObserver.onError(Status.INTERNAL.withDescription("Server error").asRuntimeException());
                }
            }

            @Override
            public void getSegmentsImmediatelyFollowing(SegmentId request, StreamObserver<SuccessorResponse> responseObserver) {
                if (request.getStreamInfo().getStream().equals("stream1")) {
                    Map<SegmentId, Pair<Double, Double>> result = new HashMap<>();
                    if (request.getSegmentId() == 0) {
                        result.put(ModelHelper.createSegmentId("scope1", "stream1", 2), Pair.of(0.0, 0.25));
                        result.put(ModelHelper.createSegmentId("scope1", "stream1", 3), Pair.of(0.25, 0.5));
                    } else if (request.getSegmentId() == 1) {
                        result.put(ModelHelper.createSegmentId("scope1", "stream1", 4), Pair.of(0.5, 0.75));
                        result.put(ModelHelper.createSegmentId("scope1", "stream1", 5), Pair.of(0.75, 1.0));
                    } else if (request.getSegmentId() == 2 || request.getSegmentId() == 3) {
                        result.put(ModelHelper.createSegmentId("scope1", "stream1", 6), Pair.of(0.0, 0.5));
                    } else if (request.getSegmentId() == 4 || request.getSegmentId() == 5) {
                        result.put(ModelHelper.createSegmentId("scope1", "stream1", 7), Pair.of(0.5, 0.25));
                    }
                    val builder = SuccessorResponse.newBuilder();
                    for (Entry<SegmentId, Pair<Double, Double>> entry : result.entrySet()) {
                        builder.addSegments(SuccessorResponse.SegmentEntry.newBuilder()
                                            .setSegment(Controller.SegmentRange.newBuilder()
                                                        .setSegmentId(entry.getKey())
                                                        .setMinKey(entry.getValue().getLeft())
                                                        .setMaxKey(entry.getValue().getRight())
                                                        .build())
                                            .addValue(10 * entry.getKey().getSegmentId())
                                            .build());
                    }
                    responseObserver.onNext(builder.build());
                    responseObserver.onCompleted();
                } else if (request.getStreamInfo().getStream().equals("stream8")) {
                    Map<SegmentId, Pair<Double, Double>> result = new HashMap<>();
                    if (request.getSegmentId() == 0) {
                        result.put(ModelHelper.createSegmentId("scope1", "stream8", 3), Pair.of(0.0, 0.2));
                        result.put(ModelHelper.createSegmentId("scope1", "stream8", 4), Pair.of(0.2, 0.33));
                    } else if (request.getSegmentId() == 1) {
                        result.put(ModelHelper.createSegmentId("scope1", "stream8", 5), Pair.of(0.33, 0.5));
                        result.put(ModelHelper.createSegmentId("scope1", "stream8", 6), Pair.of(0.5, 0.66));
                    } else if (request.getSegmentId() == 2) {
                        result.put(ModelHelper.createSegmentId("scope1", "stream8", 7), Pair.of(0.66, 0.8));
                        result.put(ModelHelper.createSegmentId("scope1", "stream8", 8), Pair.of(0.8, 1.0));
                    } else if (request.getSegmentId() == 3 || request.getSegmentId() == 4 || request.getSegmentId() == 5) {
                        result.put(ModelHelper.createSegmentId("scope1", "stream8", 9), Pair.of(0.0, 0.5));
                    } else if (request.getSegmentId() == 6 || request.getSegmentId() == 7 || request.getSegmentId() == 8) {
                        result.put(ModelHelper.createSegmentId("scope1", "stream8", 10), Pair.of(0.5, 1.0));
                    }
                    val builder = SuccessorResponse.newBuilder();
                    for (Entry<SegmentId, Pair<Double, Double>> entry : result.entrySet()) {
                        builder.addSegments(SuccessorResponse.SegmentEntry.newBuilder()
                                                                          .setSegment(Controller.SegmentRange.newBuilder()
                                                                                                             .setSegmentId(entry.getKey())
                                                                                                             .setMinKey(entry.getValue().getLeft())
                                                                                                             .setMaxKey(entry.getValue().getRight())
                                                                                                             .build())
                                                                          .addValue(10 * entry.getKey().getSegmentId())
                                                                          .build());
                    }
                    responseObserver.onNext(builder.build());
                    responseObserver.onCompleted();
                } else if (request.getStreamInfo().getStream().equals("deadline")) {
                    // dont send any response
                } else {
                    responseObserver.onError(Status.INTERNAL.withDescription("Server error").asRuntimeException());
                }
            }

            @Override
            public void getSegmentsBetween(Controller.StreamCutRange request, StreamObserver<Controller.StreamCutRangeResponse> responseObserver) {
                if (request.getStreamInfo().getStream().equals("stream1")) {
                    SegmentId segment1 = ModelHelper.createSegmentId("scope1", "stream1", 0L);
                    SegmentId segment2 = ModelHelper.createSegmentId("scope1", "stream1", 1L);
                    responseObserver.onNext(Controller.StreamCutRangeResponse.newBuilder().addSegments(segment1).addSegments(segment2).build());
                    responseObserver.onCompleted();
                } else if (request.getStreamInfo().getStream().equals("deadline")) {
                    // dont send any response
                } else {
                    responseObserver.onError(Status.INTERNAL.withDescription("Server error").asRuntimeException());
                }
            }

            @Override
            public void scale(ScaleRequest request, StreamObserver<ScaleResponse> responseObserver) {
                if (request.getStreamInfo().getStream().equals("stream1")) {
                    responseObserver.onNext(ScaleResponse.newBuilder()
                                                    .setStatus(ScaleResponse.ScaleStreamStatus.STARTED)
                                                    .addSegments(ModelHelper.createSegmentRange("scope1",
                                                                                                "stream1",
                                                                                                0,
                                                                                                0.0,
                                                                                                0.5))
                                                    .addSegments(ModelHelper.createSegmentRange("scope1",
                                                                                                "stream1",
                                                                                                1,
                                                                                                0.5,
                                                                                                1.0))
                                                    .setEpoch(0)
                                                    .build());
                    responseObserver.onCompleted();
                } else if (request.getStreamInfo().getStream().equals("deadline")) {
                    // dont send response
                } else {
                    responseObserver.onError(Status.INTERNAL.withDescription("Server error").asRuntimeException());
                }
            }

            @Override
            public void checkScale(ScaleStatusRequest request, StreamObserver<ScaleStatusResponse> responseObserver) {
                if (request.getStreamInfo().getStream().equals("stream1")) {
                    responseObserver.onNext(ScaleStatusResponse.newBuilder()
                            .setStatus(ScaleStatusResponse.ScaleStatus.SUCCESS).build());
                    responseObserver.onCompleted();
                } else if (request.getStreamInfo().getStream().equals("deadline")) {
                    // dont send any response
                } else {
                    responseObserver.onError(Status.INTERNAL.withDescription("Server error").asRuntimeException());
                }
            }

            @Override
            public void getURI(SegmentId request, StreamObserver<NodeUri> responseObserver) {
                if (request.getStreamInfo().getStream().equals("stream1")) {
                    responseObserver.onNext(NodeUri.newBuilder().setEndpoint("localhost").
                            setPort(SERVICE_PORT).build());
                    responseObserver.onCompleted();
                } else if (request.getStreamInfo().getStream().equals("deadline")) {
                    // dont send any response
                } else {
                    responseObserver.onError(Status.INTERNAL.withDescription("Server error").asRuntimeException());
                }
            }

            @Override
            public void isSegmentValid(SegmentId request,
                    StreamObserver<SegmentValidityResponse> responseObserver) {
                if (request.getStreamInfo().getStream().equals("stream1")) {
                    responseObserver.onNext(SegmentValidityResponse.newBuilder().setResponse(true).build());
                    responseObserver.onCompleted();
                } else if (request.getStreamInfo().getStream().equals("stream2")) {
                    responseObserver.onNext(SegmentValidityResponse.newBuilder().setResponse(false).build());
                    responseObserver.onCompleted();
                } else if (request.getStreamInfo().getStream().equals("deadline")) {
                    // dont send any response
                } else {
                    responseObserver.onError(Status.INTERNAL.withDescription("Server error").asRuntimeException());
                }
            }

            @Override
            public void createTransaction(CreateTxnRequest request, StreamObserver<Controller.CreateTxnResponse> responseObserver) {
                Controller.CreateTxnResponse.Builder builder = Controller.CreateTxnResponse.newBuilder();

                if (request.getStreamInfo().getStream().equals("stream1")) {
                    builder.setTxnId(TxnId.newBuilder().setHighBits(11L).setLowBits(22L).build());
                    builder.addActiveSegments(ModelHelper.createSegmentRange("scope1", "stream1", 0, 0.0, 0.5));
                    builder.addActiveSegments(ModelHelper.createSegmentRange("scope1", "stream1", 1, 0.5, 1.0));
                    responseObserver.onNext(builder.build());
                    responseObserver.onCompleted();
                } else if (request.getStreamInfo().getStream().equals("stream2")) {
                    builder.addActiveSegments(ModelHelper.createSegmentRange("scope1", "stream2", 0, 0.0, 1.0));
                    builder.setTxnId(TxnId.newBuilder().setHighBits(33L).setLowBits(44L).build());
                    responseObserver.onNext(builder.build());
                    responseObserver.onCompleted();
                } else if (request.getStreamInfo().getStream().equals("deadline")) {
                    // dont send any response
                } else {
                    responseObserver.onError(Status.INTERNAL.withDescription("Server error").asRuntimeException());
                }
            }

            @Override
            public void commitTransaction(TxnRequest request,
                    StreamObserver<Controller.TxnStatus> responseObserver) {
                lastRequest.set(request);

                if (request.getStreamInfo().getStream().equals("stream1")) {
                    responseObserver.onNext(Controller.TxnStatus.newBuilder()
                                                    .setStatus(Controller.TxnStatus.Status.SUCCESS)
                                                    .build());
                    responseObserver.onCompleted();
                } else if (request.getStreamInfo().getStream().equals("stream2")) {
                    responseObserver.onNext(Controller.TxnStatus.newBuilder()
                                                    .setStatus(Controller.TxnStatus.Status.FAILURE)
                                                    .build());
                    responseObserver.onCompleted();
                } else if (request.getStreamInfo().getStream().equals("stream3")) {
                    responseObserver.onNext(Controller.TxnStatus.newBuilder()
                                                    .setStatus(Controller.TxnStatus.Status.STREAM_NOT_FOUND)
                                                    .build());
                    responseObserver.onCompleted();
                } else if (request.getStreamInfo().getStream().equals("stream4")) {
                    responseObserver.onNext(Controller.TxnStatus.newBuilder()
                                                    .setStatus(Controller.TxnStatus.Status.TRANSACTION_NOT_FOUND)
                                                    .build());
                    responseObserver.onCompleted();
                } else if (request.getStreamInfo().getStream().equals("deadline")) {
                    // dont send any response
                } else {
                    responseObserver.onError(Status.INTERNAL.withDescription("Server error").asRuntimeException());
                }
            }

            @Override
            public void abortTransaction(TxnRequest request,
                    StreamObserver<Controller.TxnStatus> responseObserver) {
                if (request.getStreamInfo().getStream().equals("stream1")) {
                    responseObserver.onNext(Controller.TxnStatus.newBuilder()
                                                    .setStatus(Controller.TxnStatus.Status.SUCCESS)
                                                    .build());
                    responseObserver.onCompleted();
                } else if (request.getStreamInfo().getStream().equals("stream2")) {
                    responseObserver.onNext(Controller.TxnStatus.newBuilder()
                                                    .setStatus(Controller.TxnStatus.Status.FAILURE)
                                                    .build());
                    responseObserver.onCompleted();
                } else if (request.getStreamInfo().getStream().equals("stream3")) {
                    responseObserver.onNext(Controller.TxnStatus.newBuilder()
                                                    .setStatus(Controller.TxnStatus.Status.STREAM_NOT_FOUND)
                                                    .build());
                    responseObserver.onCompleted();
                } else if (request.getStreamInfo().getStream().equals("stream4")) {
                    responseObserver.onNext(Controller.TxnStatus.newBuilder()
                                                    .setStatus(Controller.TxnStatus.Status.TRANSACTION_NOT_FOUND)
                                                    .build());
                    responseObserver.onCompleted();
                } else if (request.getStreamInfo().getStream().equals("deadline")) {
                    // dont send any response
                } else {
                    responseObserver.onError(Status.INTERNAL.withDescription("Server error").asRuntimeException());
                }
            }

            @Override
            public void pingTransaction(PingTxnRequest request,
                    StreamObserver<PingTxnStatus> responseObserver) {
                switch (request.getStreamInfo().getStream()) {
                    case "stream1":
                        responseObserver.onNext(PingTxnStatus.newBuilder().setStatus(PingTxnStatus.Status.OK).build());
                        responseObserver.onCompleted();
                        break;
                    case "stream2":
                        responseObserver.onNext(PingTxnStatus.newBuilder().setStatus(PingTxnStatus.Status.LEASE_TOO_LARGE).build());
                        responseObserver.onCompleted();
                        break;
                    case "stream3":
                        responseObserver.onNext(PingTxnStatus.newBuilder().setStatus(PingTxnStatus.Status.MAX_EXECUTION_TIME_EXCEEDED).build());
                        responseObserver.onCompleted();
                        break;
                    case "stream4":
                        responseObserver.onNext(PingTxnStatus.newBuilder().setStatus(PingTxnStatus.Status.DISCONNECTED).build());
                        responseObserver.onCompleted();
                        break;
                    case "stream5":
                        responseObserver.onNext(PingTxnStatus.newBuilder().setStatus(PingTxnStatus.Status.COMMITTED).build());
                        responseObserver.onCompleted();
                        break;
                    case "stream6":
                        responseObserver.onNext(PingTxnStatus.newBuilder().setStatus(PingTxnStatus.Status.ABORTED).build());
                        responseObserver.onCompleted();
                        break;
                    case "stream8":
                        responseObserver.onNext(PingTxnStatus.newBuilder().setStatus(PingTxnStatus.Status.UNKNOWN).build());
                        responseObserver.onCompleted();
                        break;
                    case "deadline":
                        break;
                    default:
                        responseObserver.onError(Status.INTERNAL.withDescription("Server error").asRuntimeException());
                        break;
                }
            }

            @Override
            public void checkTransactionState(TxnRequest request, StreamObserver<TxnState> responseObserver) {
                if (request.getStreamInfo().getStream().equals("stream1")) {
                    responseObserver.onNext(TxnState.newBuilder().setState(TxnState.State.OPEN).build());
                    responseObserver.onCompleted();
                } else if (request.getStreamInfo().getStream().equals("stream2")) {
                    responseObserver.onNext(TxnState.newBuilder().setState(TxnState.State.UNKNOWN).build());
                    responseObserver.onCompleted();
                } else if (request.getStreamInfo().getStream().equals("stream3")) {
                    responseObserver.onNext(TxnState.newBuilder().setState(TxnState.State.COMMITTING).build());
                    responseObserver.onCompleted();
                } else if (request.getStreamInfo().getStream().equals("stream4")) {
                    responseObserver.onNext(TxnState.newBuilder().setState(TxnState.State.COMMITTED).build());
                    responseObserver.onCompleted();
                } else if (request.getStreamInfo().getStream().equals("stream5")) {
                    responseObserver.onNext(TxnState.newBuilder().setState(TxnState.State.ABORTING).build());
                    responseObserver.onCompleted();
                } else if (request.getStreamInfo().getStream().equals("stream6")) {
                    responseObserver.onNext(TxnState.newBuilder().setState(TxnState.State.ABORTED).build());
                    responseObserver.onCompleted();
                } else if (request.getStreamInfo().getStream().equals("deadline")) {
                    // dont send any response
                } else {
                    responseObserver.onError(Status.INTERNAL.withDescription("Server error").asRuntimeException());
                }
            }

            @Override
            public void createScope(ScopeInfo request, StreamObserver<CreateScopeStatus> responseObserver) {
                if (request.getScope().equals("scope1")) {
                    responseObserver.onNext(CreateScopeStatus.newBuilder().setStatus(
                            CreateScopeStatus.Status.SUCCESS).build());
                    responseObserver.onCompleted();
                } else if (request.getScope().equals("scope2")) {
                    responseObserver.onNext(CreateScopeStatus.newBuilder().setStatus(
                            CreateScopeStatus.Status.FAILURE).build());
                    responseObserver.onCompleted();
                } else if (request.getScope().equals("scope3")) {
                    responseObserver.onNext(CreateScopeStatus.newBuilder()
                                                    .setStatus(CreateScopeStatus.Status.INVALID_SCOPE_NAME)
                                                    .build());
                    responseObserver.onCompleted();
                } else if (request.getScope().equals("scope4")) {
                    responseObserver.onNext(CreateScopeStatus.newBuilder()
                                                    .setStatus(CreateScopeStatus.Status.SCOPE_EXISTS)
                                                    .build());
                    responseObserver.onCompleted();
                } else if (request.getScope().equals("deadline")) {
                    // dont send any response
                } else {
                    responseObserver.onError(Status.INTERNAL.withDescription("Server error").asRuntimeException());
                }
            }

            @Override
            public void getDelegationToken(StreamInfo request,
                                           StreamObserver<Controller.DelegationToken> responseObserver) {
                if (request.getStream().equals("deadline")) {
                    // dont send any response
                } else {
                    responseObserver.onNext(Controller.DelegationToken.newBuilder().setDelegationToken("token").build());
                    responseObserver.onCompleted();
                }
            }

            @Override
            public void deleteScope(ScopeInfo request, StreamObserver<DeleteScopeStatus> responseObserver) {
                if (request.getScope().equals("scope1")) {
                    responseObserver.onNext(DeleteScopeStatus.newBuilder().setStatus(
                            DeleteScopeStatus.Status.SUCCESS).build());
                    responseObserver.onCompleted();
                } else if (request.getScope().equals("scope2")) {
                    responseObserver.onNext(DeleteScopeStatus.newBuilder().setStatus(
                            DeleteScopeStatus.Status.FAILURE).build());
                    responseObserver.onCompleted();
                } else if (request.getScope().equals("scope3")) {
                    responseObserver.onNext(DeleteScopeStatus.newBuilder()
                                                    .setStatus(DeleteScopeStatus.Status.SCOPE_NOT_EMPTY)
                                                    .build());
                    responseObserver.onCompleted();
                } else if (request.getScope().equals("scope4")) {
                    responseObserver.onNext(DeleteScopeStatus.newBuilder()
                                                    .setStatus(DeleteScopeStatus.Status.SCOPE_NOT_FOUND)
                                                    .build());
                    responseObserver.onCompleted();
                } else if (request.getScope().equals("deadline")) {
                    // dont send any response
                } else {
                    responseObserver.onError(Status.INTERNAL.withDescription("Server error").asRuntimeException());
                }
            }

            @Override
            public void listStreamsInScope(Controller.StreamsInScopeRequest request, StreamObserver<Controller.StreamsInScopeResponse> responseObserver) {
                if (request.getScope().getScope().equals(NON_EXISTENT)) {
                    responseObserver.onNext(Controller.StreamsInScopeResponse
                            .newBuilder().setStatus(Controller.StreamsInScopeResponse.Status.SCOPE_NOT_FOUND)
                            .build());
                    responseObserver.onCompleted();
                } else if (request.getScope().getScope().equals("deadline")) {
                    // dont send any response
                } else if (request.getScope().getScope().equals(FAILING)) {
                    responseObserver.onNext(Controller.StreamsInScopeResponse
                            .newBuilder().setStatus(Controller.StreamsInScopeResponse.Status.FAILURE)
                            .build());
                    responseObserver.onCompleted();
                } else if (Strings.isNullOrEmpty(request.getContinuationToken().getToken())) {
                    List<StreamInfo> list1 = new LinkedList<>();
                    list1.add(StreamInfo.newBuilder().setScope(request.getScope().getScope()).setStream("stream1").build());
                    list1.add(StreamInfo.newBuilder().setScope(request.getScope().getScope()).setStream("stream2").build());
                    responseObserver.onNext(Controller.StreamsInScopeResponse
                            .newBuilder().setStatus(Controller.StreamsInScopeResponse.Status.SUCCESS).addAllStreams(list1)
                            .setContinuationToken(Controller.ContinuationToken.newBuilder().setToken("myToken").build()).build());
                    responseObserver.onCompleted();
                } else if (request.getContinuationToken().getToken().equals("myToken")) {
                    List<StreamInfo> list2 = new LinkedList<>();
                    list2.add(StreamInfo.newBuilder().setScope(request.getScope().getScope()).setStream("stream3").build());
                    responseObserver.onNext(Controller.StreamsInScopeResponse
                            .newBuilder().addAllStreams(list2).setStatus(Controller.StreamsInScopeResponse.Status.SUCCESS)
                            .setContinuationToken(Controller.ContinuationToken.newBuilder().setToken("myToken2").build()).build());
                    responseObserver.onCompleted();
                } else if (request.getContinuationToken().getToken().equals("myToken2")) {
                    List<StreamInfo> list3 = new LinkedList<>();
                    responseObserver.onNext(Controller.StreamsInScopeResponse
                            .newBuilder().addAllStreams(list3).setStatus(Controller.StreamsInScopeResponse.Status.SUCCESS)
                            .setContinuationToken(Controller.ContinuationToken.newBuilder().setToken("").build()).build());
                    responseObserver.onCompleted();
                } else {
                    responseObserver.onError(Status.INTERNAL.withDescription("Server error").asRuntimeException());
                }
            }
            
            @Override
            public void isStreamCutValid(Controller.StreamCut request, StreamObserver<Controller.StreamCutValidityResponse> responseObserver) {
                if (request.getStreamInfo().getStream().equals("deadline")) {
                    // do nothing
                } else {
                    responseObserver.onNext(Controller.StreamCutValidityResponse.newBuilder().setResponse(true)
                                                                                .build());
                    responseObserver.onCompleted();
                }
            }

            @Override
            public void removeWriter(Controller.RemoveWriterRequest request, StreamObserver<Controller.RemoveWriterResponse> responseObserver) {
                if (request.getStream().getStream().equals("deadline")) {
                    // do nothing
                } else {
                    responseObserver.onNext(Controller.RemoveWriterResponse
                            .newBuilder().setResult(Controller.RemoveWriterResponse.Status.SUCCESS).build());
                    responseObserver.onCompleted();
                }
            }
            
            @Override
            public void noteTimestampFromWriter(Controller.TimestampFromWriter request, StreamObserver<Controller.TimestampResponse> responseObserver) {
                if (request.getWriter().equals("deadline")) {
                    // do nothing
                } else {
                    responseObserver.onNext(Controller.TimestampResponse
                            .newBuilder().setResult(Controller.TimestampResponse.Status.SUCCESS).build());
                    responseObserver.onCompleted();
                }
            }

            @Override
            public void createKeyValueTable(KeyValueTableConfig request,
                                            StreamObserver<CreateKeyValueTableStatus> responseObserver) {
                if (request.getKvtName().equals("kvtable1")) {
                    responseObserver.onNext(CreateKeyValueTableStatus.newBuilder()
                            .setStatus(CreateKeyValueTableStatus.Status.SUCCESS)
                            .build());
                    responseObserver.onCompleted();
                } else if (request.getKvtName().equals("kvtable2")) {
                    responseObserver.onNext(CreateKeyValueTableStatus.newBuilder()
                        .setStatus(CreateKeyValueTableStatus.Status.FAILURE)
                        .build());
                    responseObserver.onCompleted();
                } else if (request.getKvtName().equals("kvtable3")) {
                    responseObserver.onNext(CreateKeyValueTableStatus.newBuilder()
                        .setStatus(CreateKeyValueTableStatus.Status.SCOPE_NOT_FOUND)
                        .build());
                    responseObserver.onCompleted();
                } else if (request.getKvtName().equals("kvtable4")) {
                    responseObserver.onNext(CreateKeyValueTableStatus.newBuilder()
                        .setStatus(CreateKeyValueTableStatus.Status.TABLE_EXISTS)
                        .build());
                    responseObserver.onCompleted();
                } else if (request.getKvtName().equals("kvtable5")) {
                    responseObserver.onNext(CreateKeyValueTableStatus.newBuilder()
                            .setStatus(CreateKeyValueTableStatus.Status.INVALID_TABLE_NAME)
                            .build());
                    responseObserver.onCompleted();
                }
            }

            @Override
            public void getCurrentSegmentsKeyValueTable(Controller.KeyValueTableInfo request,
                                                        StreamObserver<SegmentRanges> responseObserver) {
                if (request.getKvtName().equals("kvtable1")) {
                    responseObserver.onNext(SegmentRanges.newBuilder()
                            .addSegmentRanges(ModelHelper.createSegmentRange("scope1",
                                    "kvtable1",
                                    4,
                                    0.0,
                                    0.4))
                            .addSegmentRanges(ModelHelper.createSegmentRange("scope1",
                                    "kvtable1",
                                    5,
                                    0.4,
                                    1.0))
                            .build());
                    responseObserver.onCompleted();
                } else {
                    responseObserver.onError(Status.INTERNAL.withDescription("Server error").asRuntimeException());
                }
            }

            @Override
            public void listKeyValueTablesInScope(Controller.KVTablesInScopeRequest request,
                                                  StreamObserver<Controller.KVTablesInScopeResponse> responseObserver) {
                if (request.getScope().getScope().equals(NON_EXISTENT)) {
                    responseObserver.onNext(Controller.KVTablesInScopeResponse
                            .newBuilder().setStatus(Controller.KVTablesInScopeResponse.Status.SCOPE_NOT_FOUND)
                            .build());
                    responseObserver.onCompleted();
                } else if (request.getScope().getScope().equals(FAILING)) {
                    responseObserver.onNext(Controller.KVTablesInScopeResponse
                            .newBuilder().setStatus(Controller.KVTablesInScopeResponse.Status.FAILURE)
                            .build());
                    responseObserver.onCompleted();
                } else if (Strings.isNullOrEmpty(request.getContinuationToken().getToken())) {
                    List<KeyValueTableInfo> list1 = new LinkedList<>();
                    list1.add(KeyValueTableInfo.newBuilder().setScope(request.getScope().getScope()).setKvtName("kvtable1").build());
                    list1.add(KeyValueTableInfo.newBuilder().setScope(request.getScope().getScope()).setKvtName("kvtable2").build());
                    responseObserver.onNext(Controller.KVTablesInScopeResponse
                            .newBuilder().setStatus(Controller.KVTablesInScopeResponse.Status.SUCCESS).addAllKvtables(list1)
                            .setContinuationToken(Controller.ContinuationToken.newBuilder().setToken("myToken").build()).build());
                    responseObserver.onCompleted();
                } else if (request.getContinuationToken().getToken().equals("myToken")) {
                    List<KeyValueTableInfo> list2 = new LinkedList<>();
                    list2.add(KeyValueTableInfo.newBuilder().setScope(request.getScope().getScope()).setKvtName("kvtable3").build());
                    responseObserver.onNext(Controller.KVTablesInScopeResponse
                            .newBuilder().addAllKvtables(list2).setStatus(Controller.KVTablesInScopeResponse.Status.SUCCESS)
                            .setContinuationToken(Controller.ContinuationToken.newBuilder().setToken("myToken2").build()).build());
                    responseObserver.onCompleted();
                } else if (request.getContinuationToken().getToken().equals("myToken2")) {
                    List<KeyValueTableInfo> list3 = new LinkedList<>();
                    responseObserver.onNext(Controller.KVTablesInScopeResponse
                            .newBuilder().addAllKvtables(list3).setStatus(Controller.KVTablesInScopeResponse.Status.SUCCESS)
                            .setContinuationToken(Controller.ContinuationToken.newBuilder().setToken("").build()).build());
                    responseObserver.onCompleted();
                } else {
                    responseObserver.onError(Status.INTERNAL.withDescription("Server error").asRuntimeException());
                }
            }

            @Override
            public void deleteKeyValueTable(KeyValueTableInfo request,
                                     StreamObserver<DeleteKVTableStatus> responseObserver) {
                if (request.getKvtName().equals("kvtable1")) {
                    responseObserver.onNext(DeleteKVTableStatus.newBuilder()
                            .setStatus(DeleteKVTableStatus.Status.SUCCESS)
                            .build());
                    responseObserver.onCompleted();
                } else if (request.getKvtName().equals("kvtable2")) {
                    responseObserver.onNext(DeleteKVTableStatus.newBuilder()
                            .setStatus(DeleteKVTableStatus.Status.FAILURE)
                            .build());
                    responseObserver.onCompleted();
                } else if (request.getKvtName().equals("kvtable3")) {
                    responseObserver.onNext(DeleteKVTableStatus.newBuilder()
                            .setStatus(DeleteKVTableStatus.Status.TABLE_NOT_FOUND)
                            .build());
                    responseObserver.onCompleted();
                } else {
                    responseObserver.onError(Status.INTERNAL.withDescription("Server error").asRuntimeException());
                }
            }
        };

        serverPort = TestUtils.getAvailableListenPort();
        serverBuilder = NettyServerBuilder.forPort(serverPort)
                                          .addService(testServerImpl);
        if (testSecure) {
         serverBuilder = serverBuilder.useTransportSecurity(new File(SecurityConfigDefaults.TLS_SERVER_CERT_PATH),
                 new File(SecurityConfigDefaults.TLS_SERVER_PRIVATE_KEY_PATH));
        }
        testGRPCServer = serverBuilder
                .build()
                .start();
        executor = Executors.newSingleThreadScheduledExecutor();
        controllerClient = new ControllerImpl( ControllerImplConfig.builder()
                .clientConfig(
                        ClientConfig.builder().controllerURI(URI.create((testSecure ? "tls://" : "tcp://") + "localhost:" + serverPort))
                                    .credentials(new DefaultCredentials("1111_aaaa", "admin"))
                                    .trustStore(SecurityConfigDefaults.TLS_CA_CERT_PATH)
                                    .build())
                .retryAttempts(1).build(), executor);
    }

    @After
    public void tearDown() {
        ExecutorServiceHelpers.shutdown(executor);
        testGRPCServer.shutdownNow();
    }

    @Test
    public void testCredPluginException() throws Exception {
        NettyChannelBuilder builder = spy(NettyChannelBuilder.forAddress("localhost", serverPort)
                .keepAliveTime(10, TimeUnit.SECONDS));

        final NettyChannelBuilder channelBuilder;
        if (testSecure) {
            channelBuilder = builder.sslContext(GrpcSslContexts.forClient().trustManager(
                    new File(SecurityConfigDefaults.TLS_CA_CERT_PATH)).build());
        } else {
            channelBuilder = builder.usePlaintext();
        }
        // Setup mocks.
        ClientConfig cfg = spy(ClientConfig.builder()
                .credentials(new DefaultCredentials("pass", "user"))
                .trustStore(SecurityConfigDefaults.TLS_CA_CERT_PATH)
                .controllerURI(URI.create((testSecure ? "tls://" : "tcp://") + "localhost:" + serverPort))
                .build());
        doThrow(new IllegalStateException("Exception thrown by cred plugin")).when(cfg).getCredentials();
        ManagedChannel channel = mock(ManagedChannel.class);
        doReturn(channel).when(builder).build();
        ControllerImplConfig controllerCfg = new ControllerImplConfig(1, 1, 1, 1, 1000, cfg);
        //Verify exception scenario.
        assertThrows(IllegalStateException.class, () -> new ControllerImpl(channelBuilder, controllerCfg, this.executor));
        verify(channel, times(1)).shutdownNow();
        verify(channel, times(1)).awaitTermination(anyLong(), any(TimeUnit.class));
    }

    @Test
    public void testKeepAlive() throws IOException, ExecutionException, InterruptedException {

        // Verify that keep-alive timeout less than permissible by the server results in a failure.
        NettyChannelBuilder builder = NettyChannelBuilder.forAddress("localhost", serverPort)
                                                         .keepAliveTime(10, TimeUnit.SECONDS);
        if (testSecure) {
            builder = builder.sslContext(GrpcSslContexts.forClient().trustManager(
                    new File(SecurityConfigDefaults.TLS_CA_CERT_PATH)).build());
        } else {
            builder = builder.usePlaintext();
        }
        @Cleanup
        final ControllerImpl controller = new ControllerImpl(builder,
                ControllerImplConfig.builder().clientConfig(ClientConfig.builder()
                                                                        .trustStore(SecurityConfigDefaults.TLS_CA_CERT_PATH)
                                                                        .controllerURI(URI.create((testSecure ? "tls://" : "tcp://") + "localhost:" + serverPort))
                                                                        .build())
                                    .retryAttempts(1).build(),
                this.executor);
        CompletableFuture<Boolean> createStreamStatus = controller.createStream("scope1", "streamdelayed", StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(1))
                .build());
        AssertExtensions.assertFutureThrows("Should throw RetriesExhaustedException", createStreamStatus,
                throwable -> throwable instanceof RetriesExhaustedException);

        // Verify that the same RPC with permissible keepalive time succeeds.
        int serverPort2 = TestUtils.getAvailableListenPort();
        NettyServerBuilder testServerBuilder = NettyServerBuilder.forPort(serverPort2)
                                                                 .addService(testServerImpl)
                                                                 .permitKeepAliveTime(5, TimeUnit.SECONDS);

        if (testSecure) {
           testServerBuilder = testServerBuilder.useTransportSecurity(
                   new File(SecurityConfigDefaults.TLS_SERVER_CERT_PATH),
                   new File(SecurityConfigDefaults.TLS_SERVER_PRIVATE_KEY_PATH));
        }

        Server testServer = testServerBuilder.build()
                .start();

        builder = NettyChannelBuilder.forAddress("localhost", serverPort2)
                           .keepAliveTime(10, TimeUnit.SECONDS);
        if (testSecure) {
            builder = builder.sslContext(GrpcSslContexts.forClient().trustManager(
                    new File(SecurityConfigDefaults.TLS_CA_CERT_PATH)).build());
        } else {
            builder = builder.usePlaintext();
        }
        @Cleanup
        final ControllerImpl controller1 = new ControllerImpl(builder,
                ControllerImplConfig.builder().clientConfig(ClientConfig.builder()
                                                                        .trustStore(SecurityConfigDefaults.TLS_CA_CERT_PATH)
                                                                        .controllerURI(URI.create((testSecure ? "tls://" : "tcp://") + "localhost:" + serverPort))
                                                                        .build())
                                    .retryAttempts(1).build(), this.executor);
        createStreamStatus = controller1.createStream("scope1", "streamdelayed", StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(1))
                .build());
        assertTrue(createStreamStatus.get());
        testServer.shutdownNow();
    }

    @Test
    public void testRetries() throws IOException, ExecutionException, InterruptedException {

        // Verify retries exhausted error after multiple attempts.
        @Cleanup
        final ControllerImpl controller1 = new ControllerImpl( ControllerImplConfig.builder()
                .clientConfig(ClientConfig.builder()
                                          .controllerURI(URI.create((testSecure ? "tls://" : "tcp://") + "localhost:" + serverPort))
                                          .trustStore(SecurityConfigDefaults.TLS_CA_CERT_PATH).build())
                .retryAttempts(3).build(), this.executor);
        CompletableFuture<Boolean> createStreamStatus = controller1.createStream("scope1", "streamretryfailure", StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(1))
                .build());
        AssertExtensions.assertFutureThrows("Should throw RetriesExhaustedException", createStreamStatus,
                throwable -> throwable instanceof RetriesExhaustedException);

        // The following call with retries should fail since number of retries is not sufficient.
        this.retryAttempts.set(0);
        createStreamStatus = controller1.createStream("scope1", "streamretrysuccess", StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(1))
                .build());
        AssertExtensions.assertFutureThrows("Should throw RetriesExhaustedException", createStreamStatus,
                throwable -> throwable instanceof RetriesExhaustedException);

        // The RPC should succeed when internal retry attempts is > 3 which is the hardcoded test value for success.
        this.retryAttempts.set(0);
        @Cleanup
        final ControllerImpl controller2 = new ControllerImpl( ControllerImplConfig.builder()
                .clientConfig(ClientConfig.builder()
                                          .controllerURI(URI.create((testSecure ? "tls://" : "tcp://") + "localhost:" + serverPort))
                                          .trustStore(SecurityConfigDefaults.TLS_CA_CERT_PATH).build())
                .retryAttempts(4).build(), this.executor);
        createStreamStatus = controller2.createStream("scope1", "streamretrysuccess", StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(1))
                .build());
        assertTrue(createStreamStatus.get());
    }

    @Test
    public void testGetDelegationToken() throws Exception {
        CompletableFuture<String> delegationTokenFuture;
        delegationTokenFuture = controllerClient.getOrRefreshDelegationTokenFor("stream1", "scope1");
        assertEquals(delegationTokenFuture.get(), "token");
    }

    @Test
    public void testCreateStream() throws Exception {
        CompletableFuture<Boolean> createStreamStatus;
        createStreamStatus = controllerClient.createStream("scope1", "stream1", StreamConfiguration.builder()
                                                                   .scalingPolicy(ScalingPolicy.fixed(1))
                                                                   .build());
        assertTrue(createStreamStatus.get());

        assertTrue(controllerClient.checkStreamExists("scope1", "stream1").join());
        assertFalse(controllerClient.checkStreamExists("scope1", "stream2").join());
        AssertExtensions.assertFutureThrows("Server should throw exception",
                controllerClient.checkStreamExists("throwing", "throwing"), t -> true);

            createStreamStatus = controllerClient.createStream("scope1", "stream2", StreamConfiguration.builder()
                                                                   .scalingPolicy(ScalingPolicy.fixed(1))
                                                                   .build());
        AssertExtensions.assertFutureThrows("Server should throw exception",
                createStreamStatus, Throwable -> true);

        createStreamStatus = controllerClient.createStream("scope1", "stream3", StreamConfiguration.builder()
                                                                   .scalingPolicy(ScalingPolicy.fixed(1))
                                                                   .build());
        AssertExtensions.assertFutureThrows("Server should throw exception",
                createStreamStatus, Throwable -> true);

        createStreamStatus = controllerClient.createStream("scope1", "stream4", StreamConfiguration.builder()
                                                                   .scalingPolicy(ScalingPolicy.fixed(1))
                                                                   .build());
        assertFalse(createStreamStatus.get());

        createStreamStatus = controllerClient.createStream("scope1", "stream5", StreamConfiguration.builder()
                                                                   .scalingPolicy(ScalingPolicy.fixed(1))
                                                                   .build());
        AssertExtensions.assertFutureThrows("Server should throw exception",
                createStreamStatus, Throwable -> true);

        createStreamStatus = controllerClient.createStream("scope1", "stream6", StreamConfiguration.builder()
                                                                   .scalingPolicy(ScalingPolicy.fixed(1))
                                                                   .build());
        AssertExtensions.assertFutureThrows("Should throw Exception",
                createStreamStatus, throwable -> true);
    }

    @Test
    public void testUpdateStream() throws Exception {
        CompletableFuture<Boolean> updateStreamStatus;
        updateStreamStatus = controllerClient.updateStream("scope1", "stream1", StreamConfiguration.builder()
                                                                  .scalingPolicy(ScalingPolicy.fixed(1))
                                                                  .build());
        assertTrue(updateStreamStatus.get());

        updateStreamStatus = controllerClient.updateStream("scope1", "stream2", StreamConfiguration.builder()
                                                                  .scalingPolicy(ScalingPolicy.fixed(1))
                                                                  .build());
        AssertExtensions.assertFutureThrows("Server should throw exception",
                updateStreamStatus, Throwable -> true);

        updateStreamStatus = controllerClient.updateStream("scope1", "stream3", StreamConfiguration.builder()
                                                                  .scalingPolicy(ScalingPolicy.fixed(1))
                                                                  .build());
        AssertExtensions.assertFutureThrows("Server should throw exception",
                updateStreamStatus, Throwable -> true);

        updateStreamStatus = controllerClient.updateStream("scope1", "stream4", StreamConfiguration.builder()
                                                                  .scalingPolicy(ScalingPolicy.fixed(1))
                                                                  .build());
        AssertExtensions.assertFutureThrows("Server should throw exception",
                updateStreamStatus, Throwable -> true);

        updateStreamStatus = controllerClient.updateStream("scope1", "stream5", StreamConfiguration.builder()
                                                                  .scalingPolicy(ScalingPolicy.fixed(1))
                                                                  .build());
        AssertExtensions.assertFutureThrows("Should throw Exception",
                updateStreamStatus, throwable -> true);

        updateStreamStatus = controllerClient.updateStream("scope1", "stream6", StreamConfiguration.builder()
                                                                  .scalingPolicy(ScalingPolicy.fixed(1))
                                                                  .build());
        AssertExtensions.assertFutureThrows("Should throw Exception",
                updateStreamStatus, throwable -> true);
    }


    @Test
    public void testAddSubscriber() throws Exception {
        CompletableFuture<Boolean> addSubscriberStatus;
        addSubscriberStatus = controllerClient.addSubscriber("scope1", "stream1", "subscriber1");
        assertTrue(addSubscriberStatus.get());

        addSubscriberStatus = controllerClient.addSubscriber("scope1", "stream2", "subscriber1");
        AssertExtensions.assertFutureThrows("Server should throw ControllerFailureException exception",
                addSubscriberStatus, throwable -> throwable instanceof ControllerFailureException);

        addSubscriberStatus = controllerClient.addSubscriber("scope1", "stream3", "subscriber1");
        AssertExtensions.assertFutureThrows("Server should throw IllegalArgumentException exception",
                addSubscriberStatus, throwable -> throwable instanceof IllegalArgumentException);

        addSubscriberStatus = controllerClient.addSubscriber("scope1", "stream4", "subscriber1");
        AssertExtensions.assertFutureThrows("Server should throw exception",
                addSubscriberStatus, Throwable -> true);

        addSubscriberStatus = controllerClient.addSubscriber("scope1", "stream5", "subscriber1");
        assertFalse(addSubscriberStatus.get());
    }

    @Test
    public void testRemoveSubscriber() throws Exception {
        CompletableFuture<Boolean> removeSubscriberStatus;
        removeSubscriberStatus = controllerClient.deleteSubscriber("scope1", "stream1", "subscriber1");
        assertTrue(removeSubscriberStatus.get());

        removeSubscriberStatus = controllerClient.deleteSubscriber("scope1", "stream2", "subscriber1");
        AssertExtensions.assertFutureThrows("Server should throw ControllerFailureException exception",
                removeSubscriberStatus, throwable -> throwable instanceof ControllerFailureException);

        removeSubscriberStatus = controllerClient.deleteSubscriber("scope1", "stream3", "subscriber1");
        AssertExtensions.assertFutureThrows("Server should throw IllegalArgumentException exception",
                removeSubscriberStatus, throwable -> throwable instanceof IllegalArgumentException);

        removeSubscriberStatus = controllerClient.deleteSubscriber("scope1", "stream4", "subscriber1");
        assertFalse(removeSubscriberStatus.get());

        removeSubscriberStatus = controllerClient.deleteSubscriber("scope1", "stream5", "subscriber1");
        AssertExtensions.assertFutureThrows("Server should throw exception",
                removeSubscriberStatus, Throwable -> true);
    }

    @Test
    public void testListSubscribers() throws Exception {
        CompletableFuture<List<String>> getSubscribersList = controllerClient.listSubscribers("scope1", "stream1");
        assertEquals(3, getSubscribersList.get().size());

        getSubscribersList = controllerClient.listSubscribers("scope1", "stream2");
        assertEquals(0, getSubscribersList.get().size());
    }

    @Test
    public void testUpdateTruncationStreamcut() throws Exception {
        CompletableFuture<Boolean> updateSubscriberStatus;
        StreamCut streamCut = new StreamCutImpl(new StreamImpl("scope1", "stream1"), Collections.emptyMap());
        updateSubscriberStatus = controllerClient.updateSubscriberStreamCut("scope1", "stream1", "subscriber1", streamCut);
        assertTrue(updateSubscriberStatus.get());

        updateSubscriberStatus = controllerClient.updateSubscriberStreamCut("scope1", "stream2", "subscriber1", streamCut);
        AssertExtensions.assertFutureThrows("Server should throw exception",
                updateSubscriberStatus, Throwable -> true);

        updateSubscriberStatus = controllerClient.updateSubscriberStreamCut("scope1", "stream3", "subscriber1", streamCut);
        AssertExtensions.assertFutureThrows("Server should throw IllegalArgumentException exception",
                updateSubscriberStatus, throwable -> throwable instanceof IllegalArgumentException);

        updateSubscriberStatus = controllerClient.updateSubscriberStreamCut("scope1", "stream4", "subscriber1", streamCut);
        AssertExtensions.assertFutureThrows("Server should throw exception",
                updateSubscriberStatus, Throwable -> true);

        updateSubscriberStatus = controllerClient.updateSubscriberStreamCut("scope1", "stream5", "subscriber1", streamCut);
        AssertExtensions.assertFutureThrows("Server should throw exception",
                updateSubscriberStatus, throwable -> throwable instanceof IllegalArgumentException);

        updateSubscriberStatus = controllerClient.updateSubscriberStreamCut("scope1", "stream6", "subscriber1", streamCut);
        AssertExtensions.assertFutureThrows("Server should throw IllegalArgumentException exception",
                updateSubscriberStatus, throwable -> throwable instanceof IllegalArgumentException);
    }

    @Test
    public void testSealStream() throws Exception {
        CompletableFuture<Boolean> updateStreamStatus;
        updateStreamStatus = controllerClient.sealStream("scope1", "stream1");
        assertTrue(updateStreamStatus.get());

        updateStreamStatus = controllerClient.sealStream("scope1", "stream2");
        AssertExtensions.assertFutureThrows("Should throw exception",
                updateStreamStatus, Throwable -> true);

        updateStreamStatus = controllerClient.sealStream("scope1", "stream3");
        AssertExtensions.assertFutureThrows("Should throw Exception",
                updateStreamStatus, throwable -> true);

        updateStreamStatus = controllerClient.sealStream("scope1", "stream4");
        AssertExtensions.assertFutureThrows("Should throw Exception",
                updateStreamStatus, throwable -> true);

        updateStreamStatus = controllerClient.sealStream("scope1", "stream5");
        AssertExtensions.assertFutureThrows("Should throw Exception",
                updateStreamStatus, throwable -> true);
    }

    @Test
    public void testDeleteStream() {
        CompletableFuture<Boolean> deleteStreamStatus;
        deleteStreamStatus = controllerClient.deleteStream("scope1", "stream1");
        assertTrue(deleteStreamStatus.join());

        deleteStreamStatus = controllerClient.deleteStream("scope1", "stream2");
        AssertExtensions.assertFutureThrows("Should throw Exception",
                deleteStreamStatus, throwable -> true);

        deleteStreamStatus = controllerClient.deleteStream("scope1", "stream3");
        assertFalse(deleteStreamStatus.join());

        deleteStreamStatus = controllerClient.deleteStream("scope1", "stream4");
        AssertExtensions.assertFutureThrows("Should throw Exception",
                deleteStreamStatus, throwable -> true);

        deleteStreamStatus = controllerClient.deleteStream("scope1", "stream5");
        AssertExtensions.assertFutureThrows("Should throw Exception",
                deleteStreamStatus, throwable -> true);
    }

    @Test
    public void testGetCurrentSegments() throws Exception {
        CompletableFuture<StreamSegments> streamSegments;
        streamSegments = controllerClient.getCurrentSegments("scope1", "stream1");
        assertTrue(streamSegments.get().getSegments().size() == 2);
        assertEquals(new Segment("scope1", "stream1", 6), streamSegments.get().getSegmentForKey(0.2));
        assertEquals(new Segment("scope1", "stream1", 7), streamSegments.get().getSegmentForKey(0.6));

        streamSegments = controllerClient.getCurrentSegments("scope1", "stream2");
        AssertExtensions.assertFutureThrows("Should throw Exception", streamSegments, throwable -> true);

        streamSegments = controllerClient.getCurrentSegments("scope1", "sealedStream");
        assertTrue(streamSegments.get().getNumberOfSegments() == 0);
    }

    @Test
    public void testGetEpochSegments() throws Exception {
        CompletableFuture<StreamSegments> streamSegments;
        streamSegments = controllerClient.getEpochSegments("scope1", "stream1", 0);
        assertTrue(streamSegments.get().getSegments().size() == 2);
        assertEquals(new Segment("scope1", "stream1", 6), streamSegments.get().getSegmentForKey(0.2));
        assertEquals(new Segment("scope1", "stream1", 7), streamSegments.get().getSegmentForKey(0.6));

        streamSegments = controllerClient.getEpochSegments("scope1", "stream2", 0);
        AssertExtensions.assertFutureThrows("Should throw Exception", streamSegments, throwable -> true);
    }

    @Test
    public void testGetSegmentsAtTime() throws Exception {
        CompletableFuture<Map<Segment, Long>> positions;
        positions = controllerClient.getSegmentsAtTime(new StreamImpl("scope1", "stream1"), 0);
        assertEquals(2, positions.get().size());
        assertEquals(10, positions.get().get(new Segment("scope1", "stream1", 0)).longValue());
        assertEquals(20, positions.get().get(new Segment("scope1", "stream1", 1)).longValue());
        positions = controllerClient.getSegmentsAtTime(new StreamImpl("scope1", "stream2"), 0);
        AssertExtensions.assertFutureThrows("Should throw Exception", positions, throwable -> true);
    }

    @Test
    public void testGetSegmentsImmediatlyFollowing() throws Exception {
        CompletableFuture<Map<SegmentWithRange, List<Long>>> successors;
        successors = controllerClient.getSuccessors(new Segment("scope1", "stream1", 0L))
                .thenApply(StreamSegmentsWithPredecessors::getSegmentToPredecessor);
        assertEquals(2, successors.get().size());
        assertEquals(20, successors.get().get(new SegmentWithRange(new Segment("scope1", "stream1", 2L), 0.0, 0.25))
                .get(0).longValue());
        assertEquals(30, successors.get().get(new SegmentWithRange(new Segment("scope1", "stream1", 3L), 0.25, 0.5))
                .get(0).longValue());

        successors = controllerClient.getSuccessors(new Segment("scope1", "stream2", 0L))
                .thenApply(StreamSegmentsWithPredecessors::getSegmentToPredecessor);
        AssertExtensions.assertFutureThrows("Should throw Exception", successors, throwable -> true);
    }

    @Test
    public void testGetStreamCutSuccessors() throws Exception {
        StreamCut from = new StreamCutImpl(new StreamImpl("scope1", "stream1"), Collections.emptyMap());
        CompletableFuture<StreamSegmentSuccessors> successors = controllerClient.getSuccessors(from);
        assertEquals(2, successors.get().getSegments().size());
    }

    @Test
    public void testScale() throws Exception {
        CompletableFuture<Boolean> scaleStream;
        StreamImpl stream = new StreamImpl("scope1", "stream1");
        scaleStream = controllerClient.scaleStream(stream, new ArrayList<>(), new HashMap<>(), executor).getFuture();
        assertTrue(scaleStream.get());
        CompletableFuture<StreamSegments> segments = controllerClient.getCurrentSegments("scope1", "stream1");
        assertEquals(2, segments.get().getSegments().size());
        assertEquals(new Segment("scope1", "stream1", 6), segments.get().getSegmentForKey(0.25));
        assertEquals(new Segment("scope1", "stream1", 7), segments.get().getSegmentForKey(0.75));

        scaleStream = controllerClient.scaleStream(new StreamImpl("scope1", "stream2"), new ArrayList<>(),
                                                   new HashMap<>(), executor).getFuture();
        AssertExtensions.assertFutureThrows("Should throw Exception", scaleStream, throwable -> true);

        scaleStream = controllerClient.scaleStream(new StreamImpl("UNKNOWN", "stream2"), new ArrayList<>(),
                new HashMap<>(), executor).getFuture();
        AssertExtensions.assertFutureThrows("Should throw Exception", scaleStream, throwable -> true);

        scaleStream = controllerClient.scaleStream(new StreamImpl("scope1", "UNKNOWN"), new ArrayList<>(),
                new HashMap<>(), executor).getFuture();
        AssertExtensions.assertFutureThrows("Should throw Exception", scaleStream, throwable -> true);
    }

    @Test
    public void testGetURI() throws Exception {
        CompletableFuture<PravegaNodeUri> endpointForSegment;
        endpointForSegment = controllerClient.getEndpointForSegment("scope1/stream1/0");
        assertEquals(new PravegaNodeUri("localhost", SERVICE_PORT), endpointForSegment.get());

        endpointForSegment = controllerClient.getEndpointForSegment("scope1/stream2/0");
        AssertExtensions.assertFutureThrows("Should throw Exception", endpointForSegment, throwable -> true);
    }

    @Test
    public void testIsSegmentValid() throws Exception {
        CompletableFuture<Boolean> segmentOpen;
        segmentOpen = controllerClient.isSegmentOpen(new Segment("scope1", "stream1", 0));
        assertTrue(segmentOpen.get());

        segmentOpen = controllerClient.isSegmentOpen(new Segment("scope1", "stream2", 0));
        assertFalse(segmentOpen.get());

        segmentOpen = controllerClient.isSegmentOpen(new Segment("scope1", "stream3", 0));
        AssertExtensions.assertFutureThrows("Should throw Exception", segmentOpen, throwable -> true);
    }

    @Test
    public void testCreateTransaction() throws Exception {
        CompletableFuture<TxnSegments> transaction;
        transaction = controllerClient.createTransaction(new StreamImpl("scope1", "stream1"), 0);
        assertEquals(new UUID(11L, 22L), transaction.get().getTxnId());
        assertEquals(2, transaction.get().getStreamSegments().getSegments().size());
        assertEquals(new Segment("scope1", "stream1", 0), transaction.get().getStreamSegments().getSegmentForKey(.2));
        assertEquals(new Segment("scope1", "stream1", 1), transaction.get().getStreamSegments().getSegmentForKey(.8));
        
        transaction = controllerClient.createTransaction(new StreamImpl("scope1", "stream2"), 0);
        assertEquals(new UUID(33L, 44L), transaction.get().getTxnId());
        assertEquals(1, transaction.get().getStreamSegments().getSegments().size());
        assertEquals(new Segment("scope1", "stream2", 0), transaction.get().getStreamSegments().getSegmentForKey(.2));
        assertEquals(new Segment("scope1", "stream2", 0), transaction.get().getStreamSegments().getSegmentForKey(.8));
        
        transaction = controllerClient.createTransaction(new StreamImpl("scope1", "stream3"), 0);
        AssertExtensions.assertFutureThrows("Should throw Exception", transaction, throwable -> true);
    }

    @Test
    public void testCommitTransaction() throws Exception {
        CompletableFuture<Void> transaction;
        transaction = controllerClient.commitTransaction(new StreamImpl("scope1", "stream1"), "writer", null, UUID.randomUUID());
        assertTrue(transaction.get() == null);
        assertTrue(lastRequest.get() instanceof TxnRequest);
        assertEquals(((TxnRequest) (lastRequest.get())).getTimestamp(), Long.MIN_VALUE);
        
        transaction = controllerClient.commitTransaction(new StreamImpl("scope1", "stream1"), "writer", 100L, UUID.randomUUID());
        assertTrue(transaction.get() == null);
        assertTrue(lastRequest.get() instanceof TxnRequest);
        assertEquals(((TxnRequest) (lastRequest.get())).getTimestamp(), 100L);

        transaction = controllerClient.commitTransaction(new StreamImpl("scope1", "stream2"), "writer", null, UUID.randomUUID());
        AssertExtensions.assertFutureThrows("Should throw Exception", transaction, throwable -> throwable instanceof TxnFailedException);

        transaction = controllerClient.commitTransaction(new StreamImpl("scope1", "stream3"), "writer", null, UUID.randomUUID());
        AssertExtensions.assertFutureThrows("Should throw Exception", transaction, throwable -> throwable instanceof InvalidStreamException);

        transaction = controllerClient.commitTransaction(new StreamImpl("scope1", "stream4"), "writer", null, UUID.randomUUID());
        AssertExtensions.assertFutureThrows("Should throw Exception", transaction, throwable -> true);

        transaction = controllerClient.commitTransaction(new StreamImpl("scope1", "stream5"), "writer", null, UUID.randomUUID());
        AssertExtensions.assertFutureThrows("Should throw Exception", transaction, throwable -> true);
           
    }

    @Test
    public void testAbortTransaction() throws Exception {
        CompletableFuture<Void> transaction;
        transaction = controllerClient.abortTransaction(new StreamImpl("scope1", "stream1"), UUID.randomUUID());
        assertTrue(transaction.get() == null);

        transaction = controllerClient.abortTransaction(new StreamImpl("scope1", "stream2"), UUID.randomUUID());
        AssertExtensions.assertFutureThrows("Should throw Exception", transaction, throwable -> true);

        transaction = controllerClient.abortTransaction(new StreamImpl("scope1", "stream3"), UUID.randomUUID());
        AssertExtensions.assertFutureThrows("Should throw Exception", transaction, throwable -> throwable instanceof InvalidStreamException);

        transaction = controllerClient.abortTransaction(new StreamImpl("scope1", "stream4"), UUID.randomUUID());
        AssertExtensions.assertFutureThrows("Should throw Exception", transaction, throwable -> true);

        transaction = controllerClient.abortTransaction(new StreamImpl("scope1", "stream5"), UUID.randomUUID());
        AssertExtensions.assertFutureThrows("Should throw Exception", transaction, throwable -> true);
    }

    @Test
    public void testPingTransaction() throws Exception {

        Predicate<Throwable> isPingFailedException = t -> t instanceof PingFailedException;

        CompletableFuture<Transaction.PingStatus> transaction;
        transaction = controllerClient.pingTransaction(new StreamImpl("scope1", "stream1"), UUID.randomUUID(), 0);
        assertEquals(transaction.get(), Transaction.PingStatus.OPEN);

        // controller returns error with status LEASE_TOO_LARGE
        transaction = controllerClient.pingTransaction(new StreamImpl("scope1", "stream2"), UUID.randomUUID(), 0);
        AssertExtensions.assertFutureThrows("Should throw Exception", transaction, isPingFailedException);

        // controller returns error with status MAX_EXECUTION_TIME_EXCEEDED
        transaction = controllerClient.pingTransaction(new StreamImpl("scope1", "stream3"), UUID.randomUUID(), 0);
        AssertExtensions.assertFutureThrows("Should throw Exception", transaction, isPingFailedException);

        // controller returns error with status DISCONNECTED
        transaction = controllerClient.pingTransaction(new StreamImpl("scope1", "stream4"), UUID.randomUUID(), 0);
        AssertExtensions.assertFutureThrows("Should throw Exception", transaction,  isPingFailedException);

        // controller returns error with status COMMITTED
        transaction = controllerClient.pingTransaction(new StreamImpl("scope1", "stream5"), UUID.randomUUID(), 0);
        assertEquals(transaction.get(), Transaction.PingStatus.COMMITTED);

        // controller returns error with status ABORTED
        transaction = controllerClient.pingTransaction(new StreamImpl("scope1", "stream6"), UUID.randomUUID(), 0);
        assertEquals(transaction.get(), Transaction.PingStatus.ABORTED);

        // controller fails with internal exception causing the controller client to retry.
        transaction = controllerClient.pingTransaction(new StreamImpl("scope1", "stream7"), UUID.randomUUID(), 0);
        AssertExtensions.assertFutureThrows("Should throw Exception", transaction, t -> t instanceof RetriesExhaustedException);

        // controller returns error with status UNKNOWN
        transaction = controllerClient.pingTransaction(new StreamImpl("scope1", "stream8"), UUID.randomUUID(), 0);
        AssertExtensions.assertFutureThrows("Should throw Exception", transaction, 
                t -> Exceptions.unwrap(t) instanceof StatusRuntimeException 
                        && ((StatusRuntimeException) Exceptions.unwrap(t)).getStatus().equals(Status.NOT_FOUND));
    }

    @Test
    public void testChecktransactionState() throws Exception {
        CompletableFuture<Transaction.Status> transaction;
        transaction = controllerClient.checkTransactionStatus(new StreamImpl("scope1", "stream1"), UUID.randomUUID());
        assertEquals(Transaction.Status.OPEN, transaction.get());

        transaction = controllerClient.checkTransactionStatus(new StreamImpl("scope1", "stream2"), UUID.randomUUID());
        AssertExtensions.assertFutureThrows("Should throw Exception", transaction, throwable -> true);

        transaction = controllerClient.checkTransactionStatus(new StreamImpl("scope1", "stream3"), UUID.randomUUID());
        assertEquals(Transaction.Status.COMMITTING, transaction.get());

        transaction = controllerClient.checkTransactionStatus(new StreamImpl("scope1", "stream4"), UUID.randomUUID());
        assertEquals(Transaction.Status.COMMITTED, transaction.get());

        transaction = controllerClient.checkTransactionStatus(new StreamImpl("scope1", "stream5"), UUID.randomUUID());
        assertEquals(Transaction.Status.ABORTING, transaction.get());

        transaction = controllerClient.checkTransactionStatus(new StreamImpl("scope1", "stream6"), UUID.randomUUID());
        assertEquals(Transaction.Status.ABORTED, transaction.get());

        transaction = controllerClient.checkTransactionStatus(new StreamImpl("scope1", "stream7"), UUID.randomUUID());
        AssertExtensions.assertFutureThrows("Should throw Exception", transaction, throwable -> true);
    }

    @Test
    public void testCreateScope() throws Exception {
        CompletableFuture<Boolean> scopeStatus;
        scopeStatus = controllerClient.createScope("scope1");
        assertTrue(scopeStatus.get());
        assertTrue(controllerClient.checkScopeExists("scope1").join());
        assertFalse(controllerClient.checkScopeExists("scope2").join());
        AssertExtensions.assertFutureThrows("Throwing", controllerClient.checkScopeExists("throwing"),
                t -> true);

        scopeStatus = controllerClient.createScope("scope2");
        AssertExtensions.assertFutureThrows("Server should throw exception", scopeStatus, Throwable -> true);

        scopeStatus = controllerClient.createScope("scope3");
        AssertExtensions.assertFutureThrows("Server should throw exception", scopeStatus, Throwable -> true);

        scopeStatus = controllerClient.createScope("scope4");
        assertFalse(scopeStatus.get());

        scopeStatus = controllerClient.createScope("scope5");
        AssertExtensions.assertFutureThrows("Should throw Exception", scopeStatus, throwable -> true);
    }
    
    @Test
    public void testListScopes() {
        AsyncIterator<String> iterator = controllerClient.listScopes();

        String m = iterator.getNext().join();
        assertEquals("scope1", m);
        m = iterator.getNext().join();
        assertEquals("scope2", m);
        m = iterator.getNext().join();
        assertEquals("scope3", m);
    }
    
    @Test
    public void testStreamsInScope() {
        String scope = "scopeList";
        AsyncIterator<Stream> iterator = controllerClient.listStreams(scope);

        Stream m = iterator.getNext().join();
        assertEquals("stream1", m.getStreamName());
        m = iterator.getNext().join();
        assertEquals("stream2", m.getStreamName());
        m = iterator.getNext().join();
        assertEquals("stream3", m.getStreamName());

        AssertExtensions.assertFutureThrows("Non existent scope",
                controllerClient.listStreams(NON_EXISTENT).getNext(),
                e -> Exceptions.unwrap(e) instanceof NoSuchScopeException);

        AssertExtensions.assertFutureThrows("failing request",
                controllerClient.listStreams(FAILING).getNext(),
                e -> Exceptions.unwrap(e) instanceof RuntimeException);
    }

    @Test
    public void testDeleteScope() {
        CompletableFuture<Boolean> deleteStatus;
        String scope1 = "scope1";
        String scope2 = "scope2";
        String scope3 = "scope3";
        String scope4 = "scope4";
        String scope5 = "scope5";

        deleteStatus = controllerClient.deleteScope(scope1);
        assertTrue(deleteStatus.join());

        deleteStatus = controllerClient.deleteScope(scope2);
        AssertExtensions.assertFutureThrows("Server should throw exception", deleteStatus, Throwable -> true);

        deleteStatus = controllerClient.deleteScope(scope3);
        AssertExtensions.assertFutureThrows("Server should throw exception", deleteStatus, Throwable -> true);

        deleteStatus = controllerClient.deleteScope(scope4);
        assertFalse(deleteStatus.join());

        deleteStatus = controllerClient.deleteScope(scope5);
        AssertExtensions.assertFutureThrows("Server should throw exception", deleteStatus, Throwable -> true);
    }

    @Test
    public void testParallelCreateStream() throws Exception {
        final ExecutorService executorService = ExecutorServiceHelpers.newScheduledThreadPool(10, "testParallelCreateStream");
        Semaphore createCount = new Semaphore(-19);
        AtomicBoolean success = new AtomicBoolean(true);
        for (int i = 0; i < 10; i++) {
            executorService.submit(() -> {
                for (int j = 0; j < 2; j++) {
                    try {
                        CompletableFuture<Boolean> createStreamStatus;
                        createStreamStatus = controllerClient.createStream("scope1", "streamparallel",
                                StreamConfiguration.builder()
                                        .scalingPolicy(ScalingPolicy.fixed(1))
                                        .build());
                        log.info("{}", createStreamStatus.get());
                        assertTrue(createStreamStatus.get());
                        createCount.release();
                    } catch (Exception e) {
                        log.error("Exception when creating stream: {}", e);

                        // Don't wait for other threads to complete.
                        success.set(false);
                        createCount.release(20);
                    }
                }
            });
        }
        createCount.acquire();
        ExecutorServiceHelpers.shutdown(executorService);
        assertTrue(success.get());
    }

    @Test
    public void testParallelGetCurrentSegments() throws Exception {
        final ExecutorService executorService = ExecutorServiceHelpers.newScheduledThreadPool(10, "testParallelGetCurrentSegments");
        Semaphore createCount = new Semaphore(-19);
        AtomicBoolean success = new AtomicBoolean(true);
        for (int i = 0; i < 10; i++) {
            executorService.submit(() -> {
                for (int j = 0; j < 2; j++) {
                    try {
                        CompletableFuture<StreamSegments> streamSegments;
                        streamSegments = controllerClient.getCurrentSegments("scope1", "streamparallel");
                        assertTrue(streamSegments.get().getSegments().size() == 2);
                        assertEquals(new Segment("scope1", "streamparallel", 0),
                                     streamSegments.get().getSegmentForKey(0.2));
                        assertEquals(new Segment("scope1", "streamparallel", 1),
                                     streamSegments.get().getSegmentForKey(0.6));
                        createCount.release();
                    } catch (Exception e) {
                        log.error("Exception when getting segments: {}", e);

                        // Don't wait for other threads to complete.
                        success.set(false);
                        createCount.release(20);
                    }
                }
            });
        }
        createCount.acquire();
        ExecutorServiceHelpers.shutdown(executorService);
        assertTrue(success.get());
    }

    @Test
    public void testDeadline() {
        @Cleanup
        final ControllerImpl controller = new ControllerImpl( ControllerImplConfig.builder()
                                                                   .clientConfig(
                                                                           ClientConfig.builder()
                                                                                       .controllerURI(URI.create((testSecure ? "tls://" : "tcp://") + "localhost:" + serverPort))
                                                                                       .credentials(new DefaultCredentials("1111_aaaa", "admin"))
                                                                                       .trustStore(SecurityConfigDefaults.TLS_CA_CERT_PATH)
                                                                                       .build())
                                                                                  .timeoutMillis(200)
                                                                   .retryAttempts(1).build(), executor);
        Predicate<Throwable> deadlinePredicate = e -> {
            Throwable unwrapped = Exceptions.unwrap(e);
            if (unwrapped instanceof RetriesExhaustedException) {
                unwrapped = Exceptions.unwrap(unwrapped.getCause());
            }
            StatusRuntimeException exception = (StatusRuntimeException) unwrapped;
            Status.Code code = exception.getStatus().getCode();
            return code.equals(Status.Code.DEADLINE_EXCEEDED);
        };

        String deadline = "deadline";
        
        // region scope
        CompletableFuture<Boolean> scopeFuture = controller.createScope(deadline);
        AssertExtensions.assertFutureThrows("", scopeFuture, deadlinePredicate);

        CompletableFuture<Boolean> deleteScopeFuture = controller.deleteScope(deadline);
        AssertExtensions.assertFutureThrows("", deleteScopeFuture, deadlinePredicate);

        CompletableFuture<Void> listFuture = controller.listStreams(deadline).collectRemaining(x -> true);
        AssertExtensions.assertFutureThrows("", listFuture, deadlinePredicate);
        // endregion

        CompletableFuture<String> tokenFuture = controller.getOrRefreshDelegationTokenFor(deadline, deadline);
        AssertExtensions.assertFutureThrows("", tokenFuture, deadlinePredicate);

        // region stream
        CompletableFuture<Boolean> createStreamFuture = controller.createStream(deadline, deadline,
                StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build());
        AssertExtensions.assertFutureThrows("", createStreamFuture, deadlinePredicate);
        
        Stream stream = Stream.of(deadline, deadline);

        CompletableFuture<PravegaNodeUri> getEndpointFuture = controller.getEndpointForSegment(
                NameUtils.getQualifiedStreamSegmentName(deadline, deadline, 0L));
        AssertExtensions.assertFutureThrows("", getEndpointFuture, deadlinePredicate);

        CompletableFuture<StreamSegments> getSegmentFuture = controller.getCurrentSegments(deadline, deadline);
        AssertExtensions.assertFutureThrows("", getSegmentFuture, deadlinePredicate);

        CompletableFuture<Map<Segment, Long>> getSegmentsAtTimeFuture = controller.getSegmentsAtTime(stream, 0L);
        AssertExtensions.assertFutureThrows("", getSegmentsAtTimeFuture, deadlinePredicate);
        
        CompletableFuture<StreamSegments> currentSegmentsFuture = controller.getCurrentSegments(deadline, deadline);
        AssertExtensions.assertFutureThrows("", currentSegmentsFuture, deadlinePredicate);

        Segment segment = new Segment(deadline, deadline, 0L);

        CompletableFuture<Boolean> isSegmentOpenFuture = controller.isSegmentOpen(segment);
        AssertExtensions.assertFutureThrows("", isSegmentOpenFuture, deadlinePredicate);

        CompletableFuture<StreamSegmentsWithPredecessors> successorFuture = controller.getSuccessors(segment);
        AssertExtensions.assertFutureThrows("", successorFuture, deadlinePredicate);

        StreamCut streamCut = new StreamCutImpl(stream, Collections.emptyMap());

        CompletableFuture<StreamSegmentSuccessors> streamCutFuture = controller.getSegments(streamCut, streamCut);
        AssertExtensions.assertFutureThrows("", streamCutFuture, deadlinePredicate);

        CompletableFuture<StreamSegmentSuccessors> streamcutSuccessorsFuture = controller.getSuccessors(streamCut);
        AssertExtensions.assertFutureThrows("", streamcutSuccessorsFuture, deadlinePredicate);

        CompletableFuture<Boolean> updateFuture = controller.updateStream(deadline, deadline, 
                StreamConfiguration.builder().build());
        AssertExtensions.assertFutureThrows("", updateFuture, deadlinePredicate);
        
        CompletableFuture<Boolean> scaleFuture = controller.scaleStream(stream,
                Collections.emptyList(), Collections.emptyMap(), executor).getFuture();
        AssertExtensions.assertFutureThrows("", scaleFuture, deadlinePredicate);

        CompletableFuture<Boolean> scaleStatusFuture = controller.checkScaleStatus(stream, 0);
        AssertExtensions.assertFutureThrows("", scaleStatusFuture, deadlinePredicate);
        
        CompletableFuture<Boolean> truncateFuture = controller.truncateStream(deadline, deadline, 
                new StreamCutImpl(Stream.of(deadline, deadline), Collections.emptyMap()));
        AssertExtensions.assertFutureThrows("", truncateFuture, deadlinePredicate);
        
        CompletableFuture<Boolean> sealFuture = controller.sealStream(deadline, deadline);
        AssertExtensions.assertFutureThrows("", sealFuture, deadlinePredicate);
        
        CompletableFuture<Boolean> deleteFuture = controller.deleteStream(deadline, deadline);
        AssertExtensions.assertFutureThrows("", deleteFuture, deadlinePredicate);

        // endregion
        
        // region transaction
        CompletableFuture<TxnSegments> createtxnFuture = controller.createTransaction(stream, 100L);
        AssertExtensions.assertFutureThrows("", createtxnFuture, deadlinePredicate);

        CompletableFuture<Transaction.PingStatus> pingTxnFuture = controller.pingTransaction(stream, UUID.randomUUID(), 100L);
        AssertExtensions.assertFutureThrows("", pingTxnFuture, deadlinePredicate);

        CompletableFuture<Void> abortFuture = controller.abortTransaction(stream, UUID.randomUUID());
        AssertExtensions.assertFutureThrows("", abortFuture, deadlinePredicate);

        CompletableFuture<Void> commitFuture = controller.commitTransaction(stream, "", 0L, UUID.randomUUID());
        AssertExtensions.assertFutureThrows("", commitFuture, deadlinePredicate);

        CompletableFuture<Transaction.Status> txnStatusFuture = controller.checkTransactionStatus(stream, UUID.randomUUID());
        AssertExtensions.assertFutureThrows("", txnStatusFuture, deadlinePredicate);
        
        // endregion
        
        // region writer mark
        CompletableFuture<Void> writerPosFuture = controller.noteTimestampFromWriter("deadline", stream, 0L, 
                mock(WriterPosition.class));
        AssertExtensions.assertFutureThrows("", writerPosFuture, deadlinePredicate);

        CompletableFuture<Void> removeWriterFuture = controller.removeWriter("deadline", stream);
        AssertExtensions.assertFutureThrows("", removeWriterFuture, deadlinePredicate);
        // endregion
        
        // verify that a stub level deadline is not set and that the stub can still make successful calls for which we 
        // have mocked successful responses.
        controller.createScope("scope1").join();
    }

    @Test
    public void testCreateKeyValueTable() throws Exception {
        CompletableFuture<Boolean> createKVTableStatus;
        createKVTableStatus = controllerClient.createKeyValueTable("scope1", "kvtable1",
                KeyValueTableConfiguration.builder().partitionCount(1).build());
        assertTrue(createKVTableStatus.get());

        createKVTableStatus = controllerClient.createKeyValueTable("scope1", "kvtable2",
                KeyValueTableConfiguration.builder().partitionCount(1).build());
        AssertExtensions.assertFutureThrows("Server should throw exception",
                createKVTableStatus, Throwable -> true);

        createKVTableStatus = controllerClient.createKeyValueTable("scope1", "kvtable3",
                KeyValueTableConfiguration.builder().partitionCount(1).build());
        AssertExtensions.assertFutureThrows("Server should throw exception",
                createKVTableStatus, Throwable -> true);

        createKVTableStatus = controllerClient.createKeyValueTable("scope1", "kvtable4",
                KeyValueTableConfiguration.builder().partitionCount(1).build());
        assertFalse(createKVTableStatus.get());

        createKVTableStatus = controllerClient.createKeyValueTable("scope1", "kvtable5",
                KeyValueTableConfiguration.builder().partitionCount(1).build());
        AssertExtensions.assertFutureThrows("Server should throw exception",
                createKVTableStatus, Throwable -> true);

        createKVTableStatus = controllerClient.createKeyValueTable("scope1", "kvtable6",
                KeyValueTableConfiguration.builder().partitionCount(0).build());
        AssertExtensions.assertFutureThrows("Server should throw IllegalArgumentException.",
                createKVTableStatus, IllegalArgumentException -> true);
    }

    @Test
    public void testGetCurrentSegmentsKeyValueTable() throws Exception {
        CompletableFuture<KeyValueTableSegments> kvtSegments;
        kvtSegments = controllerClient.getCurrentSegmentsForKeyValueTable("scope1", "kvtable1");
        assertTrue(kvtSegments.get().getSegments().size() == 2);
        assertEquals(new Segment("scope1", "kvtable1", 4), kvtSegments.get().getSegmentForKey(0.2));
        assertEquals(new Segment("scope1", "kvtable1", 5), kvtSegments.get().getSegmentForKey(0.6));

        kvtSegments = controllerClient.getCurrentSegmentsForKeyValueTable("scope1", "kvtable2");
        AssertExtensions.assertFutureThrows("Should throw Exception", kvtSegments, throwable -> true);
    }

    @Test
    public void testKVTablesInScope() {
        String scope = "scopeList";
        AsyncIterator<io.pravega.client.admin.KeyValueTableInfo> iterator = controllerClient.listKeyValueTables(scope);

        io.pravega.client.admin.KeyValueTableInfo m = iterator.getNext().join();
        assertEquals("kvtable1", m.getKeyValueTableName());
        m = iterator.getNext().join();
        assertEquals("kvtable2", m.getKeyValueTableName());
        m = iterator.getNext().join();
        assertEquals("kvtable3", m.getKeyValueTableName());

        AssertExtensions.assertFutureThrows("Non existent scope",
                controllerClient.listKeyValueTables(NON_EXISTENT).getNext(),
                e -> Exceptions.unwrap(e) instanceof NoSuchScopeException);

        AssertExtensions.assertFutureThrows("failing request",
                controllerClient.listKeyValueTables(FAILING).getNext(),
                e -> Exceptions.unwrap(e) instanceof RuntimeException);
    }

    @Test
    public void testDeleteKeyValueTable() {
        CompletableFuture<Boolean> deleteKVTableStatus = controllerClient.deleteKeyValueTable("scope1", "kvtable1");
        assertTrue(deleteKVTableStatus.join());

        deleteKVTableStatus = controllerClient.deleteKeyValueTable("scope1", "kvtable2");
        AssertExtensions.assertFutureThrows("Should throw Exception",
                deleteKVTableStatus, throwable -> true);

        deleteKVTableStatus = controllerClient.deleteKeyValueTable("scope1", "kvtable3");
        assertFalse(deleteKVTableStatus.join());
    }


}
