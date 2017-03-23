/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.server.v1;

import com.emc.pravega.controller.server.rpc.grpc.v1.ControllerServiceImpl;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.CreateScopeStatus;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.CreateStreamStatus;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.DeleteScopeStatus;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.DeleteStreamStatus;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.UpdateStreamStatus;
import com.emc.pravega.stream.ScalingPolicy;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.impl.ModelHelper;
import io.grpc.stub.StreamObserver;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;

/**
 * Controller Service Implementation tests.
 * <p>
 * Every test is run twice for both streamStore (Zookeeper and InMemory) types.
 */
public abstract class ControllerServiceImplTest {

    private static final String SCOPE1 = "scope1";
    private static final String SCOPE2 = "scope2";
    private static final String SCOPE3 = "scope3";
    private static final String STREAM1 = "stream1";
    private static final String STREAM2 = "stream2";

    //Ensure each test completes within 10 seconds.
    @Rule
    public final Timeout globalTimeout = new Timeout(10, TimeUnit.SECONDS);

    ControllerServiceImpl controllerService;

    @Before
    public abstract void setup() throws Exception;

    @After
    public abstract void tearDown() throws Exception;

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
        this.controllerService.createScope(Controller.ScopeInfo.newBuilder().setScope(SCOPE1).build(), result);
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

    private static class ResultObserver<T> implements StreamObserver<T> {
        private T result = null;
        private final AtomicBoolean completed = new AtomicBoolean(false);

        @Override
        public void onNext(T value) {
            result = value;
        }

        @Override
        public void onError(Throwable t) {
        }

        @Override
        public void onCompleted() {
            synchronized (this) {
                completed.set(true);
                this.notifyAll();
            }
        }

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
            return result;
        }
    }
}
