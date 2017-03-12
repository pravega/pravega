/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.stream.impl;

import com.emc.pravega.common.netty.PravegaNodeUri;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.CreateScopeStatus;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.CreateStreamStatus;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.CreateTxnRequest;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.DeleteScopeStatus;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.DeleteStreamStatus;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.GetPositionRequest;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.NodeUri;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.PingTxnRequest;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.PingTxnStatus;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.Position;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.Positions;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.ScaleRequest;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.ScaleResponse;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.ScopeInfo;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.SegmentId;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.SegmentRanges;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.SegmentValidityResponse;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.StreamConfig;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.StreamInfo;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.SuccessorResponse;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.TxnId;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.TxnRequest;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.TxnState;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.UpdateStreamStatus;
import com.emc.pravega.controller.stream.api.grpc.v1.ControllerServiceGrpc.ControllerServiceImplBase;
import com.emc.pravega.stream.RetentionPolicy;
import com.emc.pravega.stream.ScalingPolicy;
import com.emc.pravega.stream.Segment;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.Transaction;
import com.emc.pravega.testcommon.AssertExtensions;
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.internal.ServerImpl;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for ControllerImpl.
 *
 */
@Slf4j
public class ControllerImplTest {

    @Rule
    public final Timeout globalTimeout = new Timeout(20, TimeUnit.SECONDS);

    // Test implementation for simulating the server responses.
    private ServerImpl fakeServer = null;

    // The controller RPC client.
    private ControllerImpl controllerClient = null;

    static class MockServiceImpl extends ControllerServiceImplBase {

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
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    log.error("Unexpected interrupt");
                    responseObserver.onError(e);
                    return;
                }
                responseObserver.onNext(CreateStreamStatus.newBuilder()
                        .setStatus(CreateStreamStatus.Status.SUCCESS)
                        .build());
                responseObserver.onCompleted();
            } else {
                responseObserver.onError(Status.INTERNAL.withDescription("Server error").asRuntimeException());
            }
        }

        @Override
        public void alterStream(StreamConfig request,
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
                                                                                                     0,
                                                                                                     0.0,
                                                                                                     0.4))
                                                    .addSegmentRanges(ModelHelper.createSegmentRange("scope1",
                                                                                                     "stream1",
                                                                                                     1,
                                                                                                     0.4,
                                                                                                     1.0))
                                                    .build());
                    responseObserver.onCompleted();
                } else if (request.getStream().equals("streamparallel")) {
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException e) {
                        log.error("Unexpected interrupt");
                        responseObserver.onError(e);
                        return;
                    }
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
        public void getPositions(GetPositionRequest request, StreamObserver<Positions> responseObserver) {
            if (request.getStreamInfo().getStream().equals("stream1")) {
                responseObserver.onNext(Positions.newBuilder()
                        .addPositions(Position.newBuilder()
                                .addOwnedSegments(Position.OwnedSegmentEntry.newBuilder()
                                        .setSegmentId(
                                                ModelHelper.createSegmentId(
                                                        "scope1",
                                                        "stream1",
                                                        0))
                                        .setValue(10)
                                        .build()))
                        .addPositions(Position.newBuilder()
                                .addOwnedSegments(Position.OwnedSegmentEntry.newBuilder()
                                        .setSegmentId(
                                                ModelHelper.createSegmentId(
                                                        "scope1",
                                                        "stream1",
                                                        1))
                                        .setValue(20)
                                        .build()))
                        .build());
                responseObserver.onCompleted();
            } else {
                responseObserver.onError(Status.INTERNAL.withDescription("Server error").asRuntimeException());
            }
        }

        @Override
        public void getSegmentsImmediatlyFollowing(SegmentId request,
                                                   StreamObserver<SuccessorResponse> responseObserver) {
            if (request.getStreamInfo().getStream().equals("stream1")) {
                responseObserver.onNext(SuccessorResponse.newBuilder()
                        .addSegments(SuccessorResponse.SegmentEntry.newBuilder()
                                .setSegmentId(ModelHelper.createSegmentId(
                                        "scope1",
                                        "stream1",
                                        0))
                                .addValue(10)
                                .build())
                        .addSegments(SuccessorResponse.SegmentEntry.newBuilder()
                                .setSegmentId(ModelHelper.createSegmentId(
                                        "scope1",
                                        "stream1",
                                        1))
                                .addValue(20)
                                .build())
                        .build());
                responseObserver.onCompleted();
            } else {
                responseObserver.onError(Status.INTERNAL.withDescription("Server error").asRuntimeException());
            }
        }

        @Override
        public void scale(ScaleRequest request, StreamObserver<ScaleResponse> responseObserver) {
            if (request.getStreamInfo().getStream().equals("stream1")) {
                responseObserver.onNext(ScaleResponse.newBuilder()
                        .setStatus(ScaleResponse.ScaleStreamStatus.SUCCESS)
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
                        .build());
                responseObserver.onCompleted();
            } else {
                responseObserver.onError(Status.INTERNAL.withDescription("Server error").asRuntimeException());
            }
        }

        @Override
        public void getURI(SegmentId request, StreamObserver<NodeUri> responseObserver) {
            if (request.getStreamInfo().getStream().equals("stream1")) {
                responseObserver.onNext(NodeUri.newBuilder().setEndpoint("localhost").setPort(12345).build());
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
        public void createTransaction(CreateTxnRequest request, StreamObserver<TxnId> responseObserver) {
            if (request.getStreamInfo().getStream().equals("stream1")) {
                responseObserver.onNext(TxnId.newBuilder().setHighBits(11L).setLowBits(22L).build());
                responseObserver.onCompleted();
            } else if (request.getStreamInfo().getStream().equals("stream2")) {
                responseObserver.onNext(TxnId.newBuilder().setHighBits(33L).setLowBits(44L).build());
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
    }

    @Before
    public void setup() throws IOException {
        // Setup fake server generating different success and failure responses.
        ControllerServiceImplBase fakeServerImpl = new MockServiceImpl();
        fakeServer = InProcessServerBuilder.forName("fakeserver")
                .addService(fakeServerImpl)
                .directExecutor()
                .build()
                .start();
        controllerClient = new ControllerImpl(InProcessChannelBuilder.forName("fakeserver").directExecutor());
    }

    @After
    public void tearDown() {
        fakeServer.shutdown();
    }

    @Test
    public void testCreateStream() throws Exception {
        CompletableFuture<CreateStreamStatus> createStreamStatus;
        createStreamStatus = controllerClient.createStream(StreamConfiguration.builder()
                                                                   .streamName("stream1")
                                                                   .scope("scope1")
                                                                   .retentionPolicy(RetentionPolicy.builder()
                                                                                            .retentionTimeMillis(0)
                                                                                            .build())
                                                                   .scalingPolicy(ScalingPolicy.fixed(1))
                                                                   .build());
        assertEquals(CreateStreamStatus.Status.SUCCESS, createStreamStatus.get().getStatus());

        createStreamStatus = controllerClient.createStream(StreamConfiguration.builder()
                                                                   .streamName("stream2")
                                                                   .scope("scope1")
                                                                   .retentionPolicy(RetentionPolicy.builder()
                                                                                            .retentionTimeMillis(0)
                                                                                            .build())
                                                                   .scalingPolicy(ScalingPolicy.fixed(1))
                                                                   .build());
        assertEquals(CreateStreamStatus.Status.FAILURE, createStreamStatus.get().getStatus());

        createStreamStatus = controllerClient.createStream(StreamConfiguration.builder()
                                                                   .streamName("stream3")
                                                                   .scope("scope1")
                                                                   .retentionPolicy(RetentionPolicy.builder()
                                                                                            .retentionTimeMillis(0)
                                                                                            .build())
                                                                   .scalingPolicy(ScalingPolicy.fixed(1))
                                                                   .build());
        assertEquals(CreateStreamStatus.Status.SCOPE_NOT_FOUND, createStreamStatus.get().getStatus());

        createStreamStatus = controllerClient.createStream(StreamConfiguration.builder()
                                                                   .streamName("stream4")
                                                                   .scope("scope1")
                                                                   .retentionPolicy(RetentionPolicy.builder()
                                                                                            .retentionTimeMillis(0)
                                                                                            .build())
                                                                   .scalingPolicy(ScalingPolicy.fixed(1))
                                                                   .build());
        assertEquals(CreateStreamStatus.Status.STREAM_EXISTS, createStreamStatus.get().getStatus());

        createStreamStatus = controllerClient.createStream(StreamConfiguration.builder()
                                                                   .streamName("stream5")
                                                                   .scope("scope1")
                                                                   .retentionPolicy(RetentionPolicy.builder()
                                                                                            .retentionTimeMillis(0)
                                                                                            .build())
                                                                   .scalingPolicy(ScalingPolicy.fixed(1))
                                                                   .build());
        assertEquals(CreateStreamStatus.Status.INVALID_STREAM_NAME, createStreamStatus.get().getStatus());

        createStreamStatus = controllerClient.createStream(StreamConfiguration.builder()
                                                                   .streamName("stream6")
                                                                   .scope("scope1")
                                                                   .retentionPolicy(RetentionPolicy.builder()
                                                                                            .retentionTimeMillis(0)
                                                                                            .build())
                                                                   .scalingPolicy(ScalingPolicy.fixed(1))
                                                                   .build());
        AssertExtensions.assertThrows("Should throw Exception", createStreamStatus, throwable -> true);
    }

    @Test
    public void testAlterStream() throws Exception {
        CompletableFuture<UpdateStreamStatus> updateStreamStatus;
        updateStreamStatus = controllerClient.alterStream(StreamConfiguration.builder()
                                                                  .streamName("stream1")
                                                                  .scope("scope1")
                                                                  .retentionPolicy(RetentionPolicy.builder()
                                                                                           .retentionTimeMillis(0)
                                                                                           .build())
                                                                  .scalingPolicy(ScalingPolicy.fixed(1))
                                                                  .build());
        assertEquals(UpdateStreamStatus.Status.SUCCESS, updateStreamStatus.get().getStatus());

        updateStreamStatus = controllerClient.alterStream(StreamConfiguration.builder()
                                                                  .streamName("stream2")
                                                                  .scope("scope1")
                                                                  .retentionPolicy(RetentionPolicy.builder()
                                                                                           .retentionTimeMillis(0)
                                                                                           .build())
                                                                  .scalingPolicy(ScalingPolicy.fixed(1))
                                                                  .build());
        assertEquals(UpdateStreamStatus.Status.FAILURE, updateStreamStatus.get().getStatus());

        updateStreamStatus = controllerClient.alterStream(StreamConfiguration.builder()
                                                                  .streamName("stream3")
                                                                  .scope("scope1")
                                                                  .retentionPolicy(RetentionPolicy.builder()
                                                                                           .retentionTimeMillis(0)
                                                                                           .build())
                                                                  .scalingPolicy(ScalingPolicy.fixed(1))
                                                                  .build());
        assertEquals(UpdateStreamStatus.Status.SCOPE_NOT_FOUND, updateStreamStatus.get().getStatus());

        updateStreamStatus = controllerClient.alterStream(StreamConfiguration.builder()
                                                                  .streamName("stream4")
                                                                  .scope("scope1")
                                                                  .retentionPolicy(RetentionPolicy.builder()
                                                                                           .retentionTimeMillis(0)
                                                                                           .build())
                                                                  .scalingPolicy(ScalingPolicy.fixed(1))
                                                                  .build());
        assertEquals(UpdateStreamStatus.Status.STREAM_NOT_FOUND, updateStreamStatus.get().getStatus());

        updateStreamStatus = controllerClient.alterStream(StreamConfiguration.builder()
                                                                  .streamName("stream5")
                                                                  .scope("scope1")
                                                                  .retentionPolicy(RetentionPolicy.builder()
                                                                                           .retentionTimeMillis(0)
                                                                                           .build())
                                                                  .scalingPolicy(ScalingPolicy.fixed(1))
                                                                  .build());
        AssertExtensions.assertThrows("Should throw Exception", updateStreamStatus, throwable -> true);
    }

    @Test
    public void testSealStream() throws Exception {
        CompletableFuture<UpdateStreamStatus> updateStreamStatus;
        updateStreamStatus = controllerClient.sealStream("scope1", "stream1");
        assertEquals(UpdateStreamStatus.Status.SUCCESS, updateStreamStatus.get().getStatus());

        updateStreamStatus = controllerClient.sealStream("scope1", "stream2");
        assertEquals(UpdateStreamStatus.Status.FAILURE, updateStreamStatus.get().getStatus());

        updateStreamStatus = controllerClient.sealStream("scope1", "stream3");
        assertEquals(UpdateStreamStatus.Status.SCOPE_NOT_FOUND, updateStreamStatus.get().getStatus());

        updateStreamStatus = controllerClient.sealStream("scope1", "stream4");
        assertEquals(UpdateStreamStatus.Status.STREAM_NOT_FOUND, updateStreamStatus.get().getStatus());

        updateStreamStatus = controllerClient.sealStream("scope1", "stream5");
        AssertExtensions.assertThrows("Should throw Exception", updateStreamStatus, throwable -> true);
    }

    @Test
    public void testDeleteStream() {
        CompletableFuture<DeleteStreamStatus> deleteStreamStatus;
        deleteStreamStatus = controllerClient.deleteStream("scope1", "stream1");
        assertEquals(DeleteStreamStatus.Status.SUCCESS, deleteStreamStatus.join().getStatus());

        deleteStreamStatus = controllerClient.deleteStream("scope1", "stream2");
        assertEquals(DeleteStreamStatus.Status.FAILURE, deleteStreamStatus.join().getStatus());

        deleteStreamStatus = controllerClient.deleteStream("scope1", "stream3");
        assertEquals(DeleteStreamStatus.Status.STREAM_NOT_FOUND, deleteStreamStatus.join().getStatus());

        deleteStreamStatus = controllerClient.deleteStream("scope1", "stream4");
        assertEquals(DeleteStreamStatus.Status.STREAM_NOT_SEALED, deleteStreamStatus.join().getStatus());

        deleteStreamStatus = controllerClient.deleteStream("scope1", "stream5");
        AssertExtensions.assertThrows("Should throw Exception", deleteStreamStatus, throwable -> true);
    }

    @Test
    public void testGetCurrentSegments() throws Exception {
        CompletableFuture<StreamSegments> streamSegments;
        streamSegments = controllerClient.getCurrentSegments("scope1", "stream1");
        assertTrue(streamSegments.get().getSegments().size() == 2);
        assertEquals(new Segment("scope1", "stream1", 0), streamSegments.get().getSegmentForKey(0.2));
        assertEquals(new Segment("scope1", "stream1", 1), streamSegments.get().getSegmentForKey(0.6));

        streamSegments = controllerClient.getCurrentSegments("scope1", "stream2");
        AssertExtensions.assertThrows("Should throw Exception", streamSegments, throwable -> true);
    }

    @Test
    public void testGetPositions() throws Exception {
        CompletableFuture<List<PositionInternal>> positions;
        positions = controllerClient.getPositions(new StreamImpl("scope1", "stream1"), 0, 0);
        assertEquals(2, positions.get().size());
        assertEquals(10,
                     positions.get().get(0).getOffsetForOwnedSegment(new Segment("scope1", "stream1", 0)).longValue());
        assertEquals(20,
                     positions.get().get(1).getOffsetForOwnedSegment(new Segment("scope1", "stream1", 1)).longValue());

        positions = controllerClient.getPositions(new StreamImpl("scope1", "stream2"), 0, 0);
        AssertExtensions.assertThrows("Should throw Exception", positions, throwable -> true);
    }

    @Test
    public void testGetSegmentsImmediatlyFollowing() throws Exception {
        CompletableFuture<Map<Segment, List<Integer>>> successors;
        successors = controllerClient.getSuccessors(new Segment("scope1", "stream1", 0));
        assertEquals(2, successors.get().size());
        assertEquals(10, successors.get().get(new Segment("scope1", "stream1", 0)).get(0).longValue());
        assertEquals(20, successors.get().get(new Segment("scope1", "stream1", 1)).get(0).longValue());

        successors = controllerClient.getSuccessors(new Segment("scope1", "stream2", 0));
        AssertExtensions.assertThrows("Should throw Exception", successors, throwable -> true);
    }

    @Test
    public void testScale() throws Exception {
        CompletableFuture<ScaleResponse> scaleStream;
        scaleStream = controllerClient.scaleStream(new StreamImpl("scope1", "stream1"), new ArrayList<>(),
                                                   new HashMap<>());
        assertTrue(scaleStream.get().getStatus() == ScaleResponse.ScaleStreamStatus.SUCCESS);
        assertEquals(2, scaleStream.get().getSegmentsCount());
        assertEquals(ModelHelper.createSegmentRange("scope1", "stream1", 0, 0.0, 0.5),
                     scaleStream.get().getSegments(0));
        assertEquals(ModelHelper.createSegmentRange("scope1", "stream1", 1, 0.5, 1.0),
                     scaleStream.get().getSegments(1));

        scaleStream = controllerClient.scaleStream(new StreamImpl("scope1", "stream2"), new ArrayList<>(),
                                                   new HashMap<>());
        AssertExtensions.assertThrows("Should throw Exception", scaleStream, throwable -> true);
    }

    @Test
    public void testGetURI() throws Exception {
        CompletableFuture<PravegaNodeUri> endpointForSegment;
        endpointForSegment = controllerClient.getEndpointForSegment("scope1/stream1/0");
        assertEquals(new PravegaNodeUri("localhost", 12345), endpointForSegment.get());

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
        CompletableFuture<UUID> transaction;
        transaction = controllerClient.createTransaction(new StreamImpl("scope1", "stream1"), 0, 0, 0);
        assertEquals(new UUID(11L, 22L), transaction.get());

        transaction = controllerClient.createTransaction(new StreamImpl("scope1", "stream2"), 0, 0, 0);
        assertEquals(new UUID(33L, 44L), transaction.get());

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
        CompletableFuture<CreateScopeStatus> scopeStatus;
        scopeStatus = controllerClient.createScope("scope1");
        assertEquals(CreateScopeStatus.Status.SUCCESS, scopeStatus.get().getStatus());

        scopeStatus = controllerClient.createScope("scope2");
        assertEquals(CreateScopeStatus.Status.FAILURE, scopeStatus.get().getStatus());

        scopeStatus = controllerClient.createScope("scope3");
        assertEquals(CreateScopeStatus.Status.INVALID_SCOPE_NAME, scopeStatus.get().getStatus());

        scopeStatus = controllerClient.createScope("scope4");
        assertEquals(CreateScopeStatus.Status.SCOPE_EXISTS, scopeStatus.get().getStatus());

        scopeStatus = controllerClient.createScope("scope5");
        AssertExtensions.assertThrows("Should throw Exception", scopeStatus, throwable -> true);
    }

    @Test
    public void testDeleteScope() {
        CompletableFuture<DeleteScopeStatus> deleteStatus;
        String scope1 = "scope1";
        String scope2 = "scope2";
        String scope3 = "scope3";
        String scope4 = "scope4";
        String scope5 = "scope5";

        deleteStatus = controllerClient.deleteScope(scope1);
        assertEquals(DeleteScopeStatus.Status.SUCCESS, deleteStatus.join().getStatus());

        deleteStatus = controllerClient.deleteScope(scope2);
        assertEquals(DeleteScopeStatus.Status.FAILURE, deleteStatus.join().getStatus());

        deleteStatus = controllerClient.deleteScope(scope3);
        assertEquals(DeleteScopeStatus.Status.SCOPE_NOT_EMPTY, deleteStatus.join().getStatus());

        deleteStatus = controllerClient.deleteScope(scope4);
        assertEquals(DeleteScopeStatus.Status.SCOPE_NOT_FOUND, deleteStatus.join().getStatus());

        deleteStatus = controllerClient.deleteScope(scope5);
        AssertExtensions.assertThrows("Server should throw exception", deleteStatus, Throwable -> true);
    }

    @Test
    public void testParallelCreateStream() throws Exception {
        final ExecutorService executorService = Executors.newFixedThreadPool(10);
        Semaphore createCount = new Semaphore(-19);
        AtomicBoolean success = new AtomicBoolean(true);
        for (int i = 0; i < 10; i++) {
            executorService.submit(() -> {
                for (int j = 0; j < 2; j++) {
                    try {
                        CompletableFuture<CreateStreamStatus> createStreamStatus;
                        createStreamStatus = controllerClient.createStream(
                                StreamConfiguration.builder()
                                        .streamName("streamparallel")
                                        .scope("scope1")
                                        .retentionPolicy(RetentionPolicy.builder().retentionTimeMillis(0).build())
                                        .scalingPolicy(ScalingPolicy.fixed(1))
                                        .build());
                        log.info("{}", createStreamStatus.get().getStatus());
                        assertEquals(CreateStreamStatus.Status.SUCCESS, createStreamStatus.get().getStatus());
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
        final ExecutorService executorService = Executors.newFixedThreadPool(10);
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
}
