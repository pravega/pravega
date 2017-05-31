/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.server.v1;

import io.pravega.controller.stream.api.grpc.v1.Controller.CreateScopeStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.CreateStreamStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.CreateTxnRequest;
import io.pravega.controller.stream.api.grpc.v1.Controller.CreateTxnResponse;
import io.pravega.controller.stream.api.grpc.v1.Controller.DeleteScopeStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.DeleteStreamStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.GetSegmentsRequest;
import io.pravega.controller.stream.api.grpc.v1.Controller.NodeUri;
import io.pravega.controller.stream.api.grpc.v1.Controller.ScaleRequest;
import io.pravega.controller.stream.api.grpc.v1.Controller.ScaleResponse;
import io.pravega.controller.stream.api.grpc.v1.Controller.ScopeInfo;
import io.pravega.controller.stream.api.grpc.v1.Controller.SegmentRanges;
import io.pravega.controller.stream.api.grpc.v1.Controller.SegmentValidityResponse;
import io.pravega.controller.stream.api.grpc.v1.Controller.SegmentsAtTime;
import io.pravega.controller.stream.api.grpc.v1.Controller.ServerRequest;
import io.pravega.controller.stream.api.grpc.v1.Controller.ServerResponse;
import io.pravega.controller.stream.api.grpc.v1.Controller.StreamInfo;
import io.pravega.controller.stream.api.grpc.v1.Controller.SuccessorResponse;
import io.pravega.controller.stream.api.grpc.v1.Controller.UpdateStreamStatus;
import io.pravega.shared.NameUtils;
import io.pravega.controller.server.rpc.grpc.v1.ControllerServiceImpl;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.ModelHelper;
import io.pravega.test.common.AssertExtensions;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

import lombok.SneakyThrows;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;

/**
 * Controller Service Implementation tests.
 * <p>
 * Every test is run twice for both streamStore (Zookeeper and InMemory) types.
 */
public abstract class ControllerServiceImplTest {

    protected static final String SCOPE1 = "scope1";
    protected static final String SCOPE2 = "scope2";
    protected static final String SCOPE3 = "scope3";
    protected static final String STREAM1 = "stream1";
    protected static final String STREAM2 = "stream2";

    //Ensure each test completes within 10 seconds.
    @Rule
    public final Timeout globalTimeout = new Timeout(10, TimeUnit.SECONDS);

    ControllerServiceImpl controllerService;

    @Before
    public abstract void setup() throws Exception;

    @After
    public abstract void tearDown() throws Exception;

    @Test
    public void getControllerServersTest() {
        ResultObserver<ServerResponse> result = new ResultObserver<>();
        this.controllerService.getControllerServerList(ServerRequest.getDefaultInstance(), result);
        assertEquals(1, result.get().getNodeURICount());
        assertEquals("localhost", result.get().getNodeURI(0).getEndpoint());
        assertEquals(9090, result.get().getNodeURI(0).getPort());
    }

    @Test
    public void createScopeTests() {
        CreateScopeStatus status;

        // region createScope
        ResultObserver<CreateScopeStatus> result1 = new ResultObserver<>();
        this.controllerService.createScope(ModelHelper.createScopeInfo(SCOPE1), result1);
        status = result1.get();
        assertEquals(status.getStatus(), CreateScopeStatus.Status.SUCCESS);

        ResultObserver<CreateScopeStatus> result2 = new ResultObserver<>();
        this.controllerService.createScope(ModelHelper.createScopeInfo(SCOPE2), result2);
        status = result2.get();
        assertEquals(status.getStatus(), CreateScopeStatus.Status.SUCCESS);
        // endregion

        // region duplicate create scope
        ResultObserver<CreateScopeStatus> result3 = new ResultObserver<>();
        this.controllerService.createScope(ModelHelper.createScopeInfo(SCOPE2), result3);
        status = result3.get();
        assertEquals(status.getStatus(), CreateScopeStatus.Status.SCOPE_EXISTS);
        // endregion

        // region with invalid scope with name "abc/def'
        ResultObserver<CreateScopeStatus> result4 = new ResultObserver<>();
        this.controllerService.createScope(ModelHelper.createScopeInfo("abc/def"), result4);
        status = result4.get();
        assertEquals(status.getStatus(), CreateScopeStatus.Status.INVALID_SCOPE_NAME);
        // endregion
    }

    @Test
    public void deleteScopeTests() {
        CreateScopeStatus createScopeStatus;
        DeleteScopeStatus deleteScopeStatus;
        CreateStreamStatus createStreamStatus;

        // Delete empty scope (containing no streams) SCOPE3
        ResultObserver<CreateScopeStatus> result1 = new ResultObserver<>();
        this.controllerService.createScope(ModelHelper.createScopeInfo(SCOPE3), result1);
        createScopeStatus = result1.get();
        assertEquals("Create Scope", CreateScopeStatus.Status.SUCCESS, createScopeStatus.getStatus());

        ResultObserver<DeleteScopeStatus> result2 = new ResultObserver<>();
        this.controllerService.deleteScope(ModelHelper.createScopeInfo(SCOPE3), result2);
        deleteScopeStatus = result2.get();
        assertEquals("Delete Empty scope", DeleteScopeStatus.Status.SUCCESS, deleteScopeStatus.getStatus());

        // To verify that SCOPE3 is infact deleted in above delete call
        ResultObserver<DeleteScopeStatus> result7 = new ResultObserver<>();
        this.controllerService.deleteScope(ModelHelper.createScopeInfo(SCOPE3), result7);
        deleteScopeStatus = result7.get();
        assertEquals("Verify that Scope3 is infact deleted", DeleteScopeStatus.Status.SCOPE_NOT_FOUND,
                     deleteScopeStatus.getStatus());

        // Delete Non-empty Scope SCOPE2
        ResultObserver<CreateScopeStatus> result3 = new ResultObserver<>();
        this.controllerService.createScope(ModelHelper.createScopeInfo(SCOPE2), result3);
        createScopeStatus = result3.get();
        assertEquals("Create Scope", CreateScopeStatus.Status.SUCCESS, createScopeStatus.getStatus());

        final ScalingPolicy policy1 = ScalingPolicy.fixed(2);
        final StreamConfiguration configuration1 =
                StreamConfiguration.builder().scope(SCOPE2).streamName(STREAM1).scalingPolicy(policy1).build();
        ResultObserver<CreateStreamStatus> result4 = new ResultObserver<>();
        this.controllerService.createStream(ModelHelper.decode(configuration1), result4);
        createStreamStatus = result4.get();
        assertEquals(createStreamStatus.getStatus(), CreateStreamStatus.Status.SUCCESS);

        ResultObserver<DeleteScopeStatus> result5 = new ResultObserver<>();
        this.controllerService.deleteScope(ModelHelper.createScopeInfo(SCOPE2), result5);
        deleteScopeStatus = result5.get();
        assertEquals("Delete non empty scope", DeleteScopeStatus.Status.SCOPE_NOT_EMPTY, deleteScopeStatus.getStatus());

        // Delete Non-existent scope SCOPE3
        ResultObserver<DeleteScopeStatus> result6 = new ResultObserver<>();
        this.controllerService.deleteScope(ModelHelper.createScopeInfo("SCOPE3"), result6);
        deleteScopeStatus = result6.get();
        assertEquals("Delete non existent scope", DeleteScopeStatus.Status.SCOPE_NOT_FOUND,
                     deleteScopeStatus.getStatus());
    }

    @Test
    public void createStreamTests() {
        final ScalingPolicy policy1 = ScalingPolicy.fixed(2);
        final ScalingPolicy policy2 = ScalingPolicy.fixed(3);
        final StreamConfiguration configuration1 =
                StreamConfiguration.builder().scope(SCOPE1).streamName(STREAM1).scalingPolicy(policy1).build();
        final StreamConfiguration configuration2 =
                StreamConfiguration.builder().scope(SCOPE1).streamName(STREAM2).scalingPolicy(policy2).build();
        final StreamConfiguration configuration3 =
                StreamConfiguration.builder().scope("SCOPE3").streamName(STREAM2).scalingPolicy(policy2).build();

        CreateStreamStatus status;

        // region checkStream
        ResultObserver<CreateScopeStatus> result = new ResultObserver<>();
        this.controllerService.createScope(ScopeInfo.newBuilder().setScope(SCOPE1).build(), result);
        Assert.assertEquals(result.get().getStatus(), CreateScopeStatus.Status.SUCCESS);

        ResultObserver<CreateStreamStatus> result1 = new ResultObserver<>();
        this.controllerService.createStream(ModelHelper.decode(configuration1), result1);
        status = result1.get();
        Assert.assertEquals(status.getStatus(), CreateStreamStatus.Status.SUCCESS);

        ResultObserver<CreateStreamStatus> result2 = new ResultObserver<>();
        this.controllerService.createStream(ModelHelper.decode(configuration2), result2);
        status = result2.get();
        Assert.assertEquals(status.getStatus(), CreateStreamStatus.Status.SUCCESS);
        // endregion

        // region duplicate create stream
        ResultObserver<CreateStreamStatus> result3 = new ResultObserver<>();
        this.controllerService.createStream(ModelHelper.decode(configuration1), result3);
        status = result3.get();
        Assert.assertEquals(status.getStatus(), CreateStreamStatus.Status.STREAM_EXISTS);
        // endregion

        // create stream for non-existent scope
        ResultObserver<CreateStreamStatus> result4 = new ResultObserver<>();
        this.controllerService.createStream(ModelHelper.decode(configuration3), result4);
        status = result4.get();
        Assert.assertEquals(status.getStatus(), CreateStreamStatus.Status.SCOPE_NOT_FOUND);

        //create stream with invalid stream name "abc/def"
        ResultObserver<CreateStreamStatus> result5 = new ResultObserver<>();
        final StreamConfiguration configuration4 =
                StreamConfiguration.builder().scope("SCOPE3").streamName("abc/def").scalingPolicy(policy2).build();
        this.controllerService.createStream(ModelHelper.decode(configuration4), result5);
        status = result5.get();
        assertEquals(status.getStatus(), CreateStreamStatus.Status.INVALID_STREAM_NAME);

        // Create stream with an internal stream name.
        ResultObserver<CreateStreamStatus> result6 = new ResultObserver<>();
        final StreamConfiguration configuration6 =
                StreamConfiguration.builder().scope(SCOPE1).streamName(
                        NameUtils.getInternalNameForStream("abcdef")).scalingPolicy(policy2).build();
        this.controllerService.createStream(ModelHelper.decode(configuration6), result6);
        status = result6.get();
        assertEquals(status.getStatus(), CreateStreamStatus.Status.SUCCESS);
    }

    @Test
    public void alterStreamTests() {
        createScopeAndStream(SCOPE1, STREAM1, ScalingPolicy.fixed(2));

        final StreamConfiguration configuration2 = StreamConfiguration.builder().scope(SCOPE1).streamName(STREAM1)
                .scalingPolicy(ScalingPolicy.fixed(3)).build();
        ResultObserver<UpdateStreamStatus> result2 = new ResultObserver<>();
        this.controllerService.alterStream(ModelHelper.decode(configuration2), result2);
        UpdateStreamStatus updateStreamStatus = result2.get();
        Assert.assertEquals(updateStreamStatus.getStatus(), UpdateStreamStatus.Status.SUCCESS);

        // Alter stream for non-existent stream.
        ResultObserver<UpdateStreamStatus> result3 = new ResultObserver<>();
        final StreamConfiguration configuration3 = StreamConfiguration.builder().scope(SCOPE1)
                .streamName("unknownstream").scalingPolicy(ScalingPolicy.fixed(1)).build();
        this.controllerService.alterStream(ModelHelper.decode(configuration3), result3);
        updateStreamStatus = result3.get();
        Assert.assertEquals(UpdateStreamStatus.Status.STREAM_NOT_FOUND, updateStreamStatus.getStatus());
    }

    @Test
    public void deleteStreamTests() {
        CreateScopeStatus createScopeStatus;
        CreateStreamStatus createStreamStatus;
        DeleteStreamStatus deleteStreamStatus;
        final StreamConfiguration configuration1 =
                StreamConfiguration.builder().scope(SCOPE1).streamName(STREAM1).scalingPolicy(ScalingPolicy.fixed(4))
                        .build();

        // Create a test scope.
        ResultObserver<CreateScopeStatus> result1 = new ResultObserver<>();
        this.controllerService.createScope(ModelHelper.createScopeInfo(SCOPE1), result1);
        createScopeStatus = result1.get();
        assertEquals("Create Scope", CreateScopeStatus.Status.SUCCESS, createScopeStatus.getStatus());

        // Try deleting a non-existent stream.
        ResultObserver<DeleteStreamStatus> result2 = new ResultObserver<>();
        this.controllerService.deleteStream(ModelHelper.createStreamInfo(SCOPE3, "dummyStream"), result2);
        deleteStreamStatus = result2.get();
        assertEquals("Delete Non-existent stream",
                DeleteStreamStatus.Status.STREAM_NOT_FOUND, deleteStreamStatus.getStatus());

        // Try deleting a non-existent stream.
        ResultObserver<DeleteStreamStatus> result3 = new ResultObserver<>();
        this.controllerService.deleteStream(ModelHelper.createStreamInfo("dummyScope", "dummyStream"), result3);
        deleteStreamStatus = result3.get();
        assertEquals("Delete Non-existent stream",
                DeleteStreamStatus.Status.STREAM_NOT_FOUND, deleteStreamStatus.getStatus());

        // Create a test stream.
        ResultObserver<CreateStreamStatus> result4 = new ResultObserver<>();
        this.controllerService.createStream(ModelHelper.decode(configuration1), result4);
        createStreamStatus = result4.get();
        Assert.assertEquals("Create stream",
                CreateStreamStatus.Status.SUCCESS, createStreamStatus.getStatus());

        // Try deleting the test stream without sealing it first.
        ResultObserver<DeleteStreamStatus> result5 = new ResultObserver<>();
        this.controllerService.deleteStream(ModelHelper.createStreamInfo(SCOPE1, STREAM1), result5);
        deleteStreamStatus = result5.get();
        assertEquals("Delete non-sealed stream",
                DeleteStreamStatus.Status.STREAM_NOT_SEALED, deleteStreamStatus.getStatus());

        // Seal the test stream.
        ResultObserver<UpdateStreamStatus> result6 = new ResultObserver<>();
        this.controllerService.sealStream(ModelHelper.createStreamInfo(SCOPE1, STREAM1), result6);
        UpdateStreamStatus updateStreamStatus = result6.get();
        assertEquals("Seal stream", UpdateStreamStatus.Status.SUCCESS, updateStreamStatus.getStatus());

        // Delete the sealed stream.
        ResultObserver<DeleteStreamStatus> result7 = new ResultObserver<>();
        this.controllerService.deleteStream(ModelHelper.createStreamInfo(SCOPE1, STREAM1), result7);
        deleteStreamStatus = result7.get();
        assertEquals("Delete sealed stream", DeleteStreamStatus.Status.SUCCESS, deleteStreamStatus.getStatus());
    }

    @Test
    public void sealStreamTests() {
        CreateScopeStatus createScopeStatus;
        CreateStreamStatus createStreamStatus;
        UpdateStreamStatus updateStreamStatus;

        final StreamConfiguration configuration1 =
                StreamConfiguration.builder().scope(SCOPE1).streamName(STREAM1).scalingPolicy(ScalingPolicy.fixed(4))
                        .build();

        // Create a test scope.
        ResultObserver<CreateScopeStatus> result1 = new ResultObserver<>();
        this.controllerService.createScope(ModelHelper.createScopeInfo(SCOPE1), result1);
        createScopeStatus = result1.get();
        assertEquals("Create Scope", CreateScopeStatus.Status.SUCCESS, createScopeStatus.getStatus());

        // Create a test stream.
        ResultObserver<CreateStreamStatus> result2 = new ResultObserver<>();
        this.controllerService.createStream(ModelHelper.decode(configuration1), result2);
        createStreamStatus = result2.get();
        assertEquals("Create stream", CreateStreamStatus.Status.SUCCESS, createStreamStatus.getStatus());

        // Seal a test stream.
        ResultObserver<UpdateStreamStatus> result3 = new ResultObserver<>();
        this.controllerService.sealStream(ModelHelper.createStreamInfo(SCOPE1, STREAM1), result3);
        updateStreamStatus = result3.get();
        assertEquals("Seal Stream", UpdateStreamStatus.Status.SUCCESS, updateStreamStatus.getStatus());

        // Seal a non-existent stream.
        ResultObserver<UpdateStreamStatus> result4 = new ResultObserver<>();
        this.controllerService.sealStream(ModelHelper.createStreamInfo(SCOPE1, "dummyStream"), result4);
        updateStreamStatus = result4.get();
        assertEquals("Seal non-existent stream",
                UpdateStreamStatus.Status.STREAM_NOT_FOUND, updateStreamStatus.getStatus());

        // Seal a non-existent stream.
        ResultObserver<UpdateStreamStatus> result5 = new ResultObserver<>();
        this.controllerService.sealStream(ModelHelper.createStreamInfo("dummyScope", STREAM1), result5);
        updateStreamStatus = result5.get();
        assertEquals("Seal non-existent stream",
                UpdateStreamStatus.Status.STREAM_NOT_FOUND, updateStreamStatus.getStatus());
    }

    @Test
    public void getCurrentSegmentsTest() {
        createScopeAndStream(SCOPE1, STREAM1, ScalingPolicy.fixed(2));

        ResultObserver<SegmentRanges> result2 = new ResultObserver<>();
        this.controllerService.getCurrentSegments(ModelHelper.createStreamInfo(SCOPE1, STREAM1), result2);
        final SegmentRanges segmentRanges = result2.get();
        Assert.assertEquals(2, segmentRanges.getSegmentRangesCount());
    }

    @Test
    public void getSegmentsTest() {
        createScopeAndStream(SCOPE1, STREAM1, ScalingPolicy.fixed(2));

        ResultObserver<SegmentsAtTime> result2 = new ResultObserver<>();
        this.controllerService.getSegments(GetSegmentsRequest.newBuilder()
                        .setStreamInfo(ModelHelper.createStreamInfo(SCOPE1, STREAM1))
                        .setTimestamp(0L)
                        .build(),
                result2);
        final SegmentsAtTime segmentRanges = result2.get();
        Assert.assertEquals(2, segmentRanges.getSegmentsCount());
    }

    @Test
    public void getSegmentsImmediatlyFollowingTest() {
        scaleTest();
        ResultObserver<SuccessorResponse> result = new ResultObserver<>();
        this.controllerService.getSegmentsImmediatlyFollowing(ModelHelper.createSegmentId(SCOPE1, STREAM1, 1), result);
        final SuccessorResponse successorResponse = result.get();
        Assert.assertEquals(2, successorResponse.getSegmentsCount());

        ResultObserver<SuccessorResponse> result2 = new ResultObserver<>();
        this.controllerService.getSegmentsImmediatlyFollowing(ModelHelper.createSegmentId(SCOPE1, STREAM1, 0),
                result2);
        final SuccessorResponse successorResponse2 = result2.get();
        Assert.assertEquals(0, successorResponse2.getSegmentsCount());
    }

    @Test
    public void scaleTest() {
        long createTime = System.currentTimeMillis();
        createScopeAndStream(SCOPE1, STREAM1, ScalingPolicy.fixed(2));

        // Scale segment 1 which has key range from 0.5 to 1.0 at time: createTime + 20.
        Map<Double, Double> keyRanges = new HashMap<>(2);
        keyRanges.put(0.5, 0.75);
        keyRanges.put(0.75, 1.0);

        final ScaleRequest scaleRequest = ScaleRequest.newBuilder()
                .setStreamInfo(ModelHelper.createStreamInfo(SCOPE1, STREAM1))
                .setScaleTimestamp(createTime + 20)
                .addSealedSegments(1)
                .addNewKeyRanges(ScaleRequest.KeyRangeEntry.newBuilder().setStart(0.5).setEnd(0.75).build())
                .addNewKeyRanges(ScaleRequest.KeyRangeEntry.newBuilder().setStart(0.75).setEnd(1.0).build())
                .build();
        ResultObserver<ScaleResponse> result2 = new ResultObserver<>();
        this.controllerService.scale(scaleRequest, result2);
        final ScaleResponse scaleResponse = result2.get();
        Assert.assertEquals(ScaleResponse.ScaleStreamStatus.SUCCESS, scaleResponse.getStatus());
        Assert.assertEquals(2, scaleResponse.getSegmentsCount());

        ResultObserver<SegmentRanges> result3 = new ResultObserver<>();
        this.controllerService.getCurrentSegments(ModelHelper.createStreamInfo(SCOPE1, STREAM1), result3);
        final SegmentRanges segmentRanges = result3.get();
        Assert.assertEquals(3, segmentRanges.getSegmentRangesCount());
        Assert.assertEquals(0, segmentRanges.getSegmentRanges(0).getSegmentId().getSegmentNumber());
        Assert.assertEquals(2, segmentRanges.getSegmentRanges(1).getSegmentId().getSegmentNumber());
        Assert.assertEquals(3, segmentRanges.getSegmentRanges(2).getSegmentId().getSegmentNumber());
    }

    @Test
    public void getURITest() {
        createScopeAndStream(SCOPE1, STREAM1, ScalingPolicy.fixed(2));

        ResultObserver<NodeUri> result1 = new ResultObserver<>();
        this.controllerService.getURI(ModelHelper.createSegmentId(SCOPE1, STREAM1, 0), result1);
        NodeUri nodeUri = result1.get();
        Assert.assertEquals("localhost", nodeUri.getEndpoint());
        Assert.assertEquals(12345, nodeUri.getPort());
    }

    @Test
    public void isSegmentValidTest() {
        createScopeAndStream(SCOPE1, STREAM1, ScalingPolicy.fixed(2));

        ResultObserver<SegmentValidityResponse> result1 = new ResultObserver<>();
        this.controllerService.isSegmentValid(ModelHelper.createSegmentId(SCOPE1, STREAM1, 0), result1);
        final SegmentValidityResponse isValid = result1.get();
        Assert.assertEquals(true, isValid.getResponse());

        ResultObserver<SegmentValidityResponse> result2 = new ResultObserver<>();
        this.controllerService.isSegmentValid(ModelHelper.createSegmentId(SCOPE1, STREAM1, 3), result2);
        final SegmentValidityResponse isValid2 = result2.get();
        Assert.assertEquals(false, isValid2.getResponse());
    }

    @Test
    public void createTransactionFailureTest() {
        createScopeAndStream(SCOPE1, STREAM1, ScalingPolicy.fixed(4));

        StreamInfo streamInfo = ModelHelper.createStreamInfo(SCOPE1, STREAM1);

        // Invalid lease
        CreateTxnRequest request = CreateTxnRequest.newBuilder()
                .setStreamInfo(streamInfo)
                .setLease(-1)
                .setMaxExecutionTime(10000)
                .setScaleGracePeriod(10000).build();
        ResultObserver<CreateTxnResponse> resultObserver = new ResultObserver<>();
        this.controllerService.createTransaction(request, resultObserver);
        AssertExtensions.assertThrows("Lease lower bound violated ",
                resultObserver::get,
                e -> checkGRPCException(e, IllegalArgumentException.class));

        // Invalid maxExecutionTime
        request = CreateTxnRequest.newBuilder()
                .setStreamInfo(streamInfo)
                .setLease(10000)
                .setMaxExecutionTime(-1)
                .setScaleGracePeriod(10000).build();
        ResultObserver<CreateTxnResponse> resultObserver2 = new ResultObserver<>();
        this.controllerService.createTransaction(request, resultObserver2);
        AssertExtensions.assertThrows("Lease lower bound violated ",
                resultObserver2::get,
                e -> checkGRPCException(e, IllegalArgumentException.class));

        // Invalid ScaleGracePeriod
        request = CreateTxnRequest.newBuilder()
                .setStreamInfo(streamInfo)
                .setLease(10000)
                .setMaxExecutionTime(10000)
                .setScaleGracePeriod(-1).build();
        ResultObserver<CreateTxnResponse> resultObserver3 = new ResultObserver<>();
        this.controllerService.createTransaction(request, resultObserver3);
        AssertExtensions.assertThrows("Lease lower bound violated ",
                resultObserver3::get,
                e -> checkGRPCException(e, IllegalArgumentException.class));
    }

    protected void createScopeAndStream(String scope, String stream, ScalingPolicy scalingPolicy) {
        final StreamConfiguration configuration1 =
                StreamConfiguration.builder().scope(scope).streamName(stream).scalingPolicy(scalingPolicy).build();

        // Create a test scope.
        ResultObserver<CreateScopeStatus> result1 = new ResultObserver<>();
        this.controllerService.createScope(ModelHelper.createScopeInfo(scope), result1);
        CreateScopeStatus createScopeStatus = result1.get();
        assertEquals("Create Scope", CreateScopeStatus.Status.SUCCESS, createScopeStatus.getStatus());

        // Create a test stream.
        ResultObserver<CreateStreamStatus> result2 = new ResultObserver<>();
        this.controllerService.createStream(ModelHelper.decode(configuration1), result2);
        CreateStreamStatus createStreamStatus = result2.get();
        assertEquals("Create stream", CreateStreamStatus.Status.SUCCESS, createStreamStatus.getStatus());
    }

    private boolean checkGRPCException(Throwable e, Class expectedCause) {
        return e instanceof StatusRuntimeException && e.getCause().getClass() == expectedCause;
    }

    static class ResultObserver<T> implements StreamObserver<T> {
        private T result = null;
        private Throwable error;
        private final AtomicBoolean completed = new AtomicBoolean(false);

        @Override
        public void onNext(T value) {
            result = value;
        }

        @Override
        public void onError(Throwable t) {
            synchronized (this) {
                error = t;
                completed.set(true);
                this.notifyAll();
            }
        }

        @Override
        public void onCompleted() {
            synchronized (this) {
                completed.set(true);
                this.notifyAll();
            }
        }

        @SneakyThrows
        public T get() {
            synchronized (this) {
                while (!completed.get()) {
                    try {
                        this.wait();
                    } catch (InterruptedException e) {
                        return null;
                    }
                }
            }
            if (error != null) {
                throw error;
            } else {
                return result;
            }
        }
    }
}
