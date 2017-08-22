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

import com.google.common.collect.ImmutableSet;
import io.grpc.Status;
import io.grpc.internal.ServerImpl;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.Transaction;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
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
import io.pravega.controller.stream.api.grpc.v1.Controller.SuccessorResponse;
import io.pravega.controller.stream.api.grpc.v1.Controller.TxnId;
import io.pravega.controller.stream.api.grpc.v1.Controller.TxnRequest;
import io.pravega.controller.stream.api.grpc.v1.Controller.TxnState;
import io.pravega.controller.stream.api.grpc.v1.Controller.UpdateStreamStatus;
import io.pravega.controller.stream.api.grpc.v1.ControllerServiceGrpc.ControllerServiceImplBase;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.TestUtils;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
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
import lombok.val;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for ControllerImpl.
 *
 */
@Slf4j
public class ControllerImplTest {
    private static final int SERVICE_PORT = 12345;

    @Rule
    public final Timeout globalTimeout = new Timeout(120, TimeUnit.SECONDS);

    // Global variable to track number of attempts to verify retries.
    private final AtomicInteger retryAttempts = new AtomicInteger(0);

    // Test implementation for simulating the server responses.
    private ControllerServiceImplBase testServerImpl;
    private ServerImpl testGRPCServer = null;

    private int serverPort;

    // The controller RPC client.
    private ControllerImpl controllerClient = null;
    private ScheduledExecutorService executor;

    @Before
    public void setup() throws IOException {

        // Setup test server generating different success and failure responses.
        testServerImpl = new ControllerServiceImplBase() {
            @Override
            public void createStream(StreamConfig request,
                    StreamObserver<CreateStreamStatus> responseObserver) {
                if (request.getStreamInfo().getStream().equals("stream1")) {
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
                } else {
                    responseObserver.onError(Status.INTERNAL.withDescription("Server error").asRuntimeException());
                }
            }

            @Override
            public void getSegmentsImmediatlyFollowing(SegmentId request, StreamObserver<SuccessorResponse> responseObserver) {
                if (request.getStreamInfo().getStream().equals("stream1")) {
                    Map<SegmentId, Pair<Double, Double>> result = new HashMap<>();
                    if (request.getSegmentNumber() == 0) {
                        result.put(ModelHelper.createSegmentId("scope1", "stream1", 2), Pair.of(0.0, 0.25));
                        result.put(ModelHelper.createSegmentId("scope1", "stream1", 3), Pair.of(0.25, 0.5));
                    } else if (request.getSegmentNumber() == 1) {
                        result.put(ModelHelper.createSegmentId("scope1", "stream1", 4), Pair.of(0.5, 0.75));
                        result.put(ModelHelper.createSegmentId("scope1", "stream1", 5), Pair.of(0.75, 1.0));
                    } else if (request.getSegmentNumber() == 2 || request.getSegmentNumber() == 3) {
                        result.put(ModelHelper.createSegmentId("scope1", "stream1", 6), Pair.of(0.0, 0.5));
                    } else if (request.getSegmentNumber() == 4 || request.getSegmentNumber() == 5) {
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
                                            .addValue(10 * entry.getKey().getSegmentNumber())
                                            .build());
                    }
                    responseObserver.onNext(builder.build());
                    responseObserver.onCompleted();
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
                } else {
                    responseObserver.onError(Status.INTERNAL.withDescription("Server error").asRuntimeException());
                }
            }

            @Override
            public void commitTransaction(TxnRequest request,
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
                } else {
                    responseObserver.onError(Status.INTERNAL.withDescription("Server error").asRuntimeException());
                }
            }

            @Override
            public void pingTransaction(PingTxnRequest request,
                    StreamObserver<PingTxnStatus> responseObserver) {
                if (request.getStreamInfo().getStream().equals("stream1")) {
                    responseObserver.onNext(PingTxnStatus.newBuilder().setStatus(PingTxnStatus.Status.OK).build());
                    responseObserver.onCompleted();
                } else {
                    responseObserver.onError(Status.INTERNAL.withDescription("Server error").asRuntimeException());
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
                } else {
                    responseObserver.onError(Status.INTERNAL.withDescription("Server error").asRuntimeException());
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
                } else {
                    responseObserver.onError(Status.INTERNAL.withDescription("Server error").asRuntimeException());
                }
            }
        };

        serverPort = TestUtils.getAvailableListenPort();
        testGRPCServer = NettyServerBuilder.forPort(serverPort)
                .addService(testServerImpl)
                .build()
                .start();
        executor = Executors.newSingleThreadScheduledExecutor();
        controllerClient = new ControllerImpl(URI.create("tcp://localhost:" + serverPort),
                ControllerImplConfig.builder().retryAttempts(1).build(), executor);
    }

    @After
    public void tearDown() {
        executor.shutdown();
        testGRPCServer.shutdownNow();
    }

    @Test
    public void testKeepAlive() throws IOException, ExecutionException, InterruptedException {

        // Verify that keep-alive timeout less than permissible by the server results in a failure.
        final ControllerImpl controller = new ControllerImpl(NettyChannelBuilder.forAddress("localhost", serverPort)
                .keepAliveTime(10, TimeUnit.SECONDS).usePlaintext(true),
                ControllerImplConfig.builder().retryAttempts(1).build(), this.executor);
        CompletableFuture<Boolean> createStreamStatus = controller.createStream(StreamConfiguration.builder()
                .streamName("streamdelayed")
                .scope("scope1")
                .scalingPolicy(ScalingPolicy.fixed(1))
                .build());
        AssertExtensions.assertThrows("Should throw RetriesExhaustedException", createStreamStatus,
                throwable -> throwable instanceof RetriesExhaustedException);

        // Verify that the same RPC with permissible keepalive time succeeds.
        int serverPort2 = TestUtils.getAvailableListenPort();
        ServerImpl testServer = NettyServerBuilder.forPort(serverPort2)
                .addService(testServerImpl)
                .permitKeepAliveTime(5, TimeUnit.SECONDS)
                .build()
                .start();
        final ControllerImpl controller1 = new ControllerImpl(NettyChannelBuilder.forAddress("localhost", serverPort2)
                .keepAliveTime(10, TimeUnit.SECONDS).usePlaintext(true),
                ControllerImplConfig.builder().retryAttempts(1).build(), this.executor);
        createStreamStatus = controller1.createStream(StreamConfiguration.builder()
                .streamName("streamdelayed")
                .scope("scope1")
                .scalingPolicy(ScalingPolicy.fixed(1))
                .build());
        assertTrue(createStreamStatus.get());
        testServer.shutdownNow();
    }

    @Test
    public void testRetries() throws IOException, ExecutionException, InterruptedException {

        // Verify retries exhausted error after multiple attempts.
        final ControllerImpl controller1 = new ControllerImpl(URI.create("tcp://localhost:" + serverPort),
                ControllerImplConfig.builder().retryAttempts(3).build(), this.executor);
        CompletableFuture<Boolean> createStreamStatus = controller1.createStream(StreamConfiguration.builder()
                .streamName("streamretryfailure")
                .scope("scope1")
                .scalingPolicy(ScalingPolicy.fixed(1))
                .build());
        AssertExtensions.assertThrows("Should throw RetriesExhaustedException", createStreamStatus,
                throwable -> throwable instanceof RetriesExhaustedException);

        // The following call with retries should fail since number of retries is not sufficient.
        this.retryAttempts.set(0);
        createStreamStatus = controller1.createStream(StreamConfiguration.builder()
                .streamName("streamretrysuccess")
                .scope("scope1")
                .scalingPolicy(ScalingPolicy.fixed(1))
                .build());
        AssertExtensions.assertThrows("Should throw RetriesExhaustedException", createStreamStatus,
                throwable -> throwable instanceof RetriesExhaustedException);

        // The RPC should succeed when internal retry attempts is > 3 which is the hardcoded test value for success.
        this.retryAttempts.set(0);
        final ControllerImpl controller2 = new ControllerImpl(URI.create("tcp://localhost:" + serverPort),
                ControllerImplConfig.builder().retryAttempts(4).build(), this.executor);
        createStreamStatus = controller2.createStream(StreamConfiguration.builder()
                .streamName("streamretrysuccess")
                .scope("scope1")
                .scalingPolicy(ScalingPolicy.fixed(1))
                .build());
        assertTrue(createStreamStatus.get());
    }

    @Test
    public void testCreateStream() throws Exception {
        CompletableFuture<Boolean> createStreamStatus;
        createStreamStatus = controllerClient.createStream(StreamConfiguration.builder()
                                                                   .streamName("stream1")
                                                                   .scope("scope1")
                                                                   .scalingPolicy(ScalingPolicy.fixed(1))
                                                                   .build());
        assertTrue(createStreamStatus.get());

        createStreamStatus = controllerClient.createStream(StreamConfiguration.builder()
                                                                   .streamName("stream2")
                                                                   .scope("scope1")
                                                                   .scalingPolicy(ScalingPolicy.fixed(1))
                                                                   .build());
        AssertExtensions.assertThrows("Server should throw exception", createStreamStatus, Throwable -> true);

        createStreamStatus = controllerClient.createStream(StreamConfiguration.builder()
                                                                   .streamName("stream3")
                                                                   .scope("scope1")
                                                                   .scalingPolicy(ScalingPolicy.fixed(1))
                                                                   .build());
        AssertExtensions.assertThrows("Server should throw exception", createStreamStatus, Throwable -> true);

        createStreamStatus = controllerClient.createStream(StreamConfiguration.builder()
                                                                   .streamName("stream4")
                                                                   .scope("scope1")
                                                                   .scalingPolicy(ScalingPolicy.fixed(1))
                                                                   .build());
        assertFalse(createStreamStatus.get());

        createStreamStatus = controllerClient.createStream(StreamConfiguration.builder()
                                                                   .streamName("stream5")
                                                                   .scope("scope1")
                                                                   .scalingPolicy(ScalingPolicy.fixed(1))
                                                                   .build());
        AssertExtensions.assertThrows("Server should throw exception", createStreamStatus, Throwable -> true);

        createStreamStatus = controllerClient.createStream(StreamConfiguration.builder()
                                                                   .streamName("stream6")
                                                                   .scope("scope1")
                                                                   .scalingPolicy(ScalingPolicy.fixed(1))
                                                                   .build());
        AssertExtensions.assertThrows("Should throw Exception", createStreamStatus, throwable -> true);
    }

    @Test
    public void testUpdateStream() throws Exception {
        CompletableFuture<Boolean> updateStreamStatus;
        updateStreamStatus = controllerClient.updateStream(StreamConfiguration.builder()
                                                                  .streamName("stream1")
                                                                  .scope("scope1")
                                                                  .scalingPolicy(ScalingPolicy.fixed(1))
                                                                  .build());
        assertTrue(updateStreamStatus.get());

        updateStreamStatus = controllerClient.updateStream(StreamConfiguration.builder()
                                                                  .streamName("stream2")
                                                                  .scope("scope1")
                                                                  .scalingPolicy(ScalingPolicy.fixed(1))
                                                                  .build());
        AssertExtensions.assertThrows("Server should throw exception", updateStreamStatus, Throwable -> true);

        updateStreamStatus = controllerClient.updateStream(StreamConfiguration.builder()
                                                                  .streamName("stream3")
                                                                  .scope("scope1")
                                                                  .scalingPolicy(ScalingPolicy.fixed(1))
                                                                  .build());
        AssertExtensions.assertThrows("Server should throw exception", updateStreamStatus, Throwable -> true);

        updateStreamStatus = controllerClient.updateStream(StreamConfiguration.builder()
                                                                  .streamName("stream4")
                                                                  .scope("scope1")
                                                                  .scalingPolicy(ScalingPolicy.fixed(1))
                                                                  .build());
        AssertExtensions.assertThrows("Server should throw exception", updateStreamStatus, Throwable -> true);

        updateStreamStatus = controllerClient.updateStream(StreamConfiguration.builder()
                                                                  .streamName("stream5")
                                                                  .scope("scope1")
                                                                  .scalingPolicy(ScalingPolicy.fixed(1))
                                                                  .build());
        AssertExtensions.assertThrows("Should throw Exception", updateStreamStatus, throwable -> true);

        updateStreamStatus = controllerClient.updateStream(StreamConfiguration.builder()
                                                                  .streamName("stream6")
                                                                  .scope("scope1")
                                                                  .scalingPolicy(ScalingPolicy.fixed(1))
                                                                  .build());
        AssertExtensions.assertThrows("Should throw Exception", updateStreamStatus, throwable -> true);
    }

    @Test
    public void testSealStream() throws Exception {
        CompletableFuture<Boolean> updateStreamStatus;
        updateStreamStatus = controllerClient.sealStream("scope1", "stream1");
        assertTrue(updateStreamStatus.get());

        updateStreamStatus = controllerClient.sealStream("scope1", "stream2");
        AssertExtensions.assertThrows("Should throw exception", updateStreamStatus, Throwable -> true);

        updateStreamStatus = controllerClient.sealStream("scope1", "stream3");
        AssertExtensions.assertThrows("Should throw Exception", updateStreamStatus, throwable -> true);

        updateStreamStatus = controllerClient.sealStream("scope1", "stream4");
        AssertExtensions.assertThrows("Should throw Exception", updateStreamStatus, throwable -> true);

        updateStreamStatus = controllerClient.sealStream("scope1", "stream5");
        AssertExtensions.assertThrows("Should throw Exception", updateStreamStatus, throwable -> true);
    }

    @Test
    public void testDeleteStream() {
        CompletableFuture<Boolean> deleteStreamStatus;
        deleteStreamStatus = controllerClient.deleteStream("scope1", "stream1");
        assertTrue(deleteStreamStatus.join());

        deleteStreamStatus = controllerClient.deleteStream("scope1", "stream2");
        AssertExtensions.assertThrows("Should throw Exception", deleteStreamStatus, throwable -> true);

        deleteStreamStatus = controllerClient.deleteStream("scope1", "stream3");
        assertFalse(deleteStreamStatus.join());

        deleteStreamStatus = controllerClient.deleteStream("scope1", "stream4");
        AssertExtensions.assertThrows("Should throw Exception", deleteStreamStatus, throwable -> true);

        deleteStreamStatus = controllerClient.deleteStream("scope1", "stream5");
        AssertExtensions.assertThrows("Should throw Exception", deleteStreamStatus, throwable -> true);
    }

    @Test
    public void testGetCurrentSegments() throws Exception {
        CompletableFuture<StreamSegments> streamSegments;
        streamSegments = controllerClient.getCurrentSegments("scope1", "stream1");
        assertTrue(streamSegments.get().getSegments().size() == 2);
        assertEquals(new Segment("scope1", "stream1", 6), streamSegments.get().getSegmentForKey(0.2));
        assertEquals(new Segment("scope1", "stream1", 7), streamSegments.get().getSegmentForKey(0.6));

        streamSegments = controllerClient.getCurrentSegments("scope1", "stream2");
        AssertExtensions.assertThrows("Should throw Exception", streamSegments, throwable -> true);
    }

    @Test
    public void testGetSegmentsAtTime() throws Exception {
        CompletableFuture<Map<Segment, Long>> positions;
        positions = controllerClient.getSegmentsAtTime(new StreamImpl("scope1", "stream1"), 0);
        assertEquals(2, positions.get().size());
        assertEquals(10, positions.get().get(new Segment("scope1", "stream1", 0)).longValue());
        assertEquals(20, positions.get().get(new Segment("scope1", "stream1", 1)).longValue());
        positions = controllerClient.getSegmentsAtTime(new StreamImpl("scope1", "stream2"), 0);
        AssertExtensions.assertThrows("Should throw Exception", positions, throwable -> true);
    }

    @Test
    public void testGetSegmentsImmediatlyFollowing() throws Exception {
        CompletableFuture<Map<Segment, List<Integer>>> successors;
        successors = controllerClient.getSuccessors(new Segment("scope1", "stream1", 0))
                .thenApply(StreamSegmentsWithPredecessors::getSegmentToPredecessor);
        assertEquals(2, successors.get().size());
        assertEquals(20, successors.get().get(new Segment("scope1", "stream1", 2))
                .get(0).longValue());
        assertEquals(30, successors.get().get(new Segment("scope1", "stream1", 3))
                .get(0).longValue());

        successors = controllerClient.getSuccessors(new Segment("scope1", "stream2", 0))
                .thenApply(StreamSegmentsWithPredecessors::getSegmentToPredecessor);
        AssertExtensions.assertThrows("Should throw Exception", successors, throwable -> true);
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
        AssertExtensions.assertThrows("Should throw Exception", scaleStream, throwable -> true);

        scaleStream = controllerClient.scaleStream(new StreamImpl("UNKNOWN", "stream2"), new ArrayList<>(),
                new HashMap<>(), executor).getFuture();
        AssertExtensions.assertThrows("Should throw Exception", scaleStream, throwable -> true);

        scaleStream = controllerClient.scaleStream(new StreamImpl("scope1", "UNKNOWN"), new ArrayList<>(),
                new HashMap<>(), executor).getFuture();
        AssertExtensions.assertThrows("Should throw Exception", scaleStream, throwable -> true);
    }

    @Test
    public void testGetURI() throws Exception {
        CompletableFuture<PravegaNodeUri> endpointForSegment;
        endpointForSegment = controllerClient.getEndpointForSegment("scope1/stream1/0");
        assertEquals(new PravegaNodeUri("localhost", SERVICE_PORT), endpointForSegment.get());

        endpointForSegment = controllerClient.getEndpointForSegment("scope1/stream2/0");
        AssertExtensions.assertThrows("Should throw Exception", endpointForSegment, throwable -> true);
    }

    @Test
    public void testIsSegmentValid() throws Exception {
        CompletableFuture<Boolean> segmentOpen;
        segmentOpen = controllerClient.isSegmentOpen(new Segment("scope1", "stream1", 0));
        assertTrue(segmentOpen.get());

        segmentOpen = controllerClient.isSegmentOpen(new Segment("scope1", "stream2", 0));
        assertFalse(segmentOpen.get());

        segmentOpen = controllerClient.isSegmentOpen(new Segment("scope1", "stream3", 0));
        AssertExtensions.assertThrows("Should throw Exception", segmentOpen, throwable -> true);
    }

    @Test
    public void testCreateTransaction() throws Exception {
        CompletableFuture<TxnSegments> transaction;
        transaction = controllerClient.createTransaction(new StreamImpl("scope1", "stream1"), 0, 0, 0);
        assertEquals(new UUID(11L, 22L), transaction.get().getTxnId());
        assertEquals(2, transaction.get().getSteamSegments().getSegments().size());
        assertEquals(new Segment("scope1", "stream1", 0), transaction.get().getSteamSegments().getSegmentForKey(.2));
        assertEquals(new Segment("scope1", "stream1", 1), transaction.get().getSteamSegments().getSegmentForKey(.8));
        
        transaction = controllerClient.createTransaction(new StreamImpl("scope1", "stream2"), 0, 0, 0);
        assertEquals(new UUID(33L, 44L), transaction.get().getTxnId());
        assertEquals(1, transaction.get().getSteamSegments().getSegments().size());
        assertEquals(new Segment("scope1", "stream2", 0), transaction.get().getSteamSegments().getSegmentForKey(.2));
        assertEquals(new Segment("scope1", "stream2", 0), transaction.get().getSteamSegments().getSegmentForKey(.8));
        
        transaction = controllerClient.createTransaction(new StreamImpl("scope1", "stream3"), 0, 0, 0);
        AssertExtensions.assertThrows("Should throw Exception", transaction, throwable -> true);
    }

    @Test
    public void testCommitTransaction() throws Exception {
        CompletableFuture<Void> transaction;
        transaction = controllerClient.commitTransaction(new StreamImpl("scope1", "stream1"), UUID.randomUUID());
        assertTrue(transaction.get() == null);

        transaction = controllerClient.commitTransaction(new StreamImpl("scope1", "stream2"), UUID.randomUUID());
        AssertExtensions.assertThrows("Should throw Exception", transaction, throwable -> true);

        transaction = controllerClient.commitTransaction(new StreamImpl("scope1", "stream3"), UUID.randomUUID());
        AssertExtensions.assertThrows("Should throw Exception", transaction, throwable -> true);

        transaction = controllerClient.commitTransaction(new StreamImpl("scope1", "stream4"), UUID.randomUUID());
        AssertExtensions.assertThrows("Should throw Exception", transaction, throwable -> true);

        transaction = controllerClient.commitTransaction(new StreamImpl("scope1", "stream5"), UUID.randomUUID());
        AssertExtensions.assertThrows("Should throw Exception", transaction, throwable -> true);
    }

    @Test
    public void testAbortTransaction() throws Exception {
        CompletableFuture<Void> transaction;
        transaction = controllerClient.abortTransaction(new StreamImpl("scope1", "stream1"), UUID.randomUUID());
        assertTrue(transaction.get() == null);

        transaction = controllerClient.abortTransaction(new StreamImpl("scope1", "stream2"), UUID.randomUUID());
        AssertExtensions.assertThrows("Should throw Exception", transaction, throwable -> true);

        transaction = controllerClient.abortTransaction(new StreamImpl("scope1", "stream3"), UUID.randomUUID());
        AssertExtensions.assertThrows("Should throw Exception", transaction, throwable -> true);

        transaction = controllerClient.abortTransaction(new StreamImpl("scope1", "stream4"), UUID.randomUUID());
        AssertExtensions.assertThrows("Should throw Exception", transaction, throwable -> true);

        transaction = controllerClient.abortTransaction(new StreamImpl("scope1", "stream5"), UUID.randomUUID());
        AssertExtensions.assertThrows("Should throw Exception", transaction, throwable -> true);
    }

    @Test
    public void testPingTransaction() throws Exception {
        CompletableFuture<Void> transaction;
        transaction = controllerClient.pingTransaction(new StreamImpl("scope1", "stream1"), UUID.randomUUID(), 0);
        assertTrue(transaction.get() == null);

        transaction = controllerClient.pingTransaction(new StreamImpl("scope1", "stream2"), UUID.randomUUID(), 0);
        AssertExtensions.assertThrows("Should throw Exception", transaction, throwable -> true);
    }

    @Test
    public void testChecktransactionState() throws Exception {
        CompletableFuture<Transaction.Status> transaction;
        transaction = controllerClient.checkTransactionStatus(new StreamImpl("scope1", "stream1"), UUID.randomUUID());
        assertEquals(Transaction.Status.OPEN, transaction.get());

        transaction = controllerClient.checkTransactionStatus(new StreamImpl("scope1", "stream2"), UUID.randomUUID());
        AssertExtensions.assertThrows("Should throw Exception", transaction, throwable -> true);

        transaction = controllerClient.checkTransactionStatus(new StreamImpl("scope1", "stream3"), UUID.randomUUID());
        assertEquals(Transaction.Status.COMMITTING, transaction.get());

        transaction = controllerClient.checkTransactionStatus(new StreamImpl("scope1", "stream4"), UUID.randomUUID());
        assertEquals(Transaction.Status.COMMITTED, transaction.get());

        transaction = controllerClient.checkTransactionStatus(new StreamImpl("scope1", "stream5"), UUID.randomUUID());
        assertEquals(Transaction.Status.ABORTING, transaction.get());

        transaction = controllerClient.checkTransactionStatus(new StreamImpl("scope1", "stream6"), UUID.randomUUID());
        assertEquals(Transaction.Status.ABORTED, transaction.get());

        transaction = controllerClient.checkTransactionStatus(new StreamImpl("scope1", "stream7"), UUID.randomUUID());
        AssertExtensions.assertThrows("Should throw Exception", transaction, throwable -> true);
    }

    @Test
    public void testCreateScope() throws Exception {
        CompletableFuture<Boolean> scopeStatus;
        scopeStatus = controllerClient.createScope("scope1");
        assertTrue(scopeStatus.get());

        scopeStatus = controllerClient.createScope("scope2");
        AssertExtensions.assertThrows("Server should throw exception", scopeStatus, Throwable -> true);

        scopeStatus = controllerClient.createScope("scope3");
        AssertExtensions.assertThrows("Server should throw exception", scopeStatus, Throwable -> true);

        scopeStatus = controllerClient.createScope("scope4");
        assertFalse(scopeStatus.get());

        scopeStatus = controllerClient.createScope("scope5");
        AssertExtensions.assertThrows("Should throw Exception", scopeStatus, throwable -> true);
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
        AssertExtensions.assertThrows("Server should throw exception", deleteStatus, Throwable -> true);

        deleteStatus = controllerClient.deleteScope(scope3);
        AssertExtensions.assertThrows("Server should throw exception", deleteStatus, Throwable -> true);

        deleteStatus = controllerClient.deleteScope(scope4);
        assertFalse(deleteStatus.join());

        deleteStatus = controllerClient.deleteScope(scope5);
        AssertExtensions.assertThrows("Server should throw exception", deleteStatus, Throwable -> true);
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
                        createStreamStatus = controllerClient.createStream(
                                StreamConfiguration.builder()
                                        .streamName("streamparallel")
                                        .scope("scope1")
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
        executorService.shutdownNow();
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
        executorService.shutdownNow();
        assertTrue(success.get());
    }
    
    @Test
    public void testCutpointSuccessors() throws Exception {
        PravegaNodeUri pravegaNodeUri = new PravegaNodeUri("localhost", SERVICE_PORT);
        String scope = "scope1";
        String stream = "stream1";
        Stream s = new StreamImpl(scope, stream);
        Map<Segment, Long> segments = new HashMap<>();
        segments.put(new Segment(scope, stream, 0), 4L);
        segments.put(new Segment(scope, stream, 1), 6L);
        StreamCut cut = new StreamCut(s, segments);
        Set<Segment> successors = controllerClient.getSuccessors(cut).get();
        assertEquals(ImmutableSet.of(new Segment(scope, stream, 0), new Segment(scope, stream, 1),
                                     new Segment(scope, stream, 2), new Segment(scope, stream, 3),
                                     new Segment(scope, stream, 4), new Segment(scope, stream, 5),
                                     new Segment(scope, stream, 6), new Segment(scope, stream, 7)),
                     successors);
    }
}
