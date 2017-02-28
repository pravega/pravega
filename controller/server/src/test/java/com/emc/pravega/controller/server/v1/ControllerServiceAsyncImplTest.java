/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.server.v1;

import com.emc.pravega.controller.server.rpc.v1.ControllerService;
import com.emc.pravega.controller.server.rpc.v1.ControllerServiceAsyncImpl;
import com.emc.pravega.controller.store.StoreClient;
import com.emc.pravega.controller.store.ZKStoreClient;
import com.emc.pravega.controller.store.host.HostControllerStore;
import com.emc.pravega.controller.store.host.HostStoreFactory;
import com.emc.pravega.controller.store.stream.StreamMetadataStore;
import com.emc.pravega.controller.store.stream.StreamStoreFactory;
import com.emc.pravega.controller.store.stream.ZKStreamMetadataStore;
import com.emc.pravega.controller.store.task.TaskMetadataStore;
import com.emc.pravega.controller.store.task.TaskStoreFactory;
import com.emc.pravega.controller.stream.api.v1.CreateScopeStatus;
import com.emc.pravega.controller.stream.api.v1.CreateStreamStatus;
import com.emc.pravega.controller.stream.api.v1.DeleteScopeStatus;
import com.emc.pravega.controller.task.Stream.StreamMetadataTasks;
import com.emc.pravega.controller.task.Stream.StreamTransactionMetadataTasks;
import com.emc.pravega.controller.util.ThriftAsyncCallback;
import com.emc.pravega.stream.ScalingPolicy;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.impl.ModelHelper;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.thrift.TException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.assertEquals;

/**
 * Async Controller Service Implementation tests.
 *
 * Every Test is run for both streamStore (Zookeeper and InMemory) types.
 * checkstyle:StaticVariableName is suppressed because the structure requires a static method to pass parameters
 * to constructor, and this methods initializes the static class members.
 */
@RunWith(Parameterized.class)
public class ControllerServiceAsyncImplTest {

    private final String scope1 = "scope1";
    private final String scope2 = "scope2";
    private final String scope3 = "scope3";
    private final String stream1 = "stream1";
    private final String stream2 = "stream2";
    private final ControllerServiceAsyncImpl controllerService;
    private final TaskMetadataStore taskMetadataStore;
    private final HostControllerStore hostStore;
    private final StreamMetadataTasks streamMetadataTasks;
    private final StoreClient storeClient;
    private final StreamTransactionMetadataTasks streamTransactionMetadataTasks;
    private final StreamMetadataStore streamStore;

    public ControllerServiceAsyncImplTest(ScheduledExecutorService executor,
                                          CuratorFramework zkClient,
                                          StreamMetadataStore streamStoreType)
            throws Exception {
        streamStore = streamStoreType;
        storeClient = new ZKStoreClient(zkClient);

        taskMetadataStore = TaskStoreFactory.createStore(storeClient, executor);
        hostStore = HostStoreFactory.createStore(HostStoreFactory.StoreType.InMemory);
        streamMetadataTasks = new StreamMetadataTasks(streamStore, hostStore, taskMetadataStore,
                executor, "host");

        streamTransactionMetadataTasks =
                new StreamTransactionMetadataTasks(streamStore, hostStore, taskMetadataStore, executor, "host");

        controllerService = new ControllerServiceAsyncImpl(
                new ControllerService(streamStore, hostStore, streamMetadataTasks, streamTransactionMetadataTasks));
    }

    /**
     * Different parameters for streamStore to be sent to constructor.
     *
     * @return Collection of Arrays, each array being a parameter to the test.
     * @throws Exception Exception thrown by test method.
     */
    @Parameterized.Parameters
    public static Collection storeType() throws Exception {

        final TestingServer zkServer = new TestingServer();
        zkServer.start();
        final CuratorFramework zkClient = CuratorFrameworkFactory.newClient(zkServer.getConnectString(),
                new ExponentialBackoffRetry(200, 10, 5000));
        zkClient.start();

        final ScheduledExecutorService executor = Executors.newScheduledThreadPool(20,
                new ThreadFactoryBuilder().setNameFormat("testpool-%d").build());

        return Arrays.asList(new Object[][]{
                {executor, zkClient, StreamStoreFactory.createStore(StreamStoreFactory.StoreType.InMemory, executor)},
                {executor, zkClient, new ZKStreamMetadataStore(zkClient, executor)}
        });
    }

    @Test
    public void createScopeTests() throws TException, ExecutionException, InterruptedException {
        CreateScopeStatus status;
        final String scope1 = "Scope1";
        final String scope2 = "Scope2";

        // region createScope
        ThriftAsyncCallback<CreateScopeStatus> result1 = new ThriftAsyncCallback<>();
        this.controllerService.createScope(scope1, result1);
        status = result1.getResult().get();
        assertEquals(status, CreateScopeStatus.SUCCESS);

        ThriftAsyncCallback<CreateScopeStatus> result2 = new ThriftAsyncCallback<>();
        this.controllerService.createScope(scope2, result2);
        status = result2.getResult().get();
        assertEquals(status, CreateScopeStatus.SUCCESS);
        // endregion

        // region duplicate create scope
        ThriftAsyncCallback<CreateScopeStatus> result3 = new ThriftAsyncCallback<>();
        this.controllerService.createScope(scope2, result3);
        status = result3.getResult().get();
        assertEquals(status, CreateScopeStatus.SCOPE_EXISTS);
        // endregion

        // region with invalid scope with name "abc/def'
        ThriftAsyncCallback<CreateScopeStatus> result4 = new ThriftAsyncCallback<>();
        this.controllerService.createScope("abc/def", result4);
        status = result4.getResult().get();
        assertEquals(status, CreateScopeStatus.INVALID_SCOPE_NAME);
        // endregion
    }

    @Test
    public void deleteScopeTests() throws TException, ExecutionException, InterruptedException {
        CreateScopeStatus createScopeStatus;
        DeleteScopeStatus deleteScopeStatus;
        CreateStreamStatus createStreamStatus;

        // Delete empty scope (containing no streams) SCOPE3
        ThriftAsyncCallback<CreateScopeStatus> result1 = new ThriftAsyncCallback<>();
        this.controllerService.createScope(scope3, result1);
        createScopeStatus = result1.getResult().get();
        assertEquals("Create Scope", CreateScopeStatus.SUCCESS, createScopeStatus);

        ThriftAsyncCallback<DeleteScopeStatus> result2 = new ThriftAsyncCallback<>();
        this.controllerService.deleteScope(scope3, result2);
        deleteScopeStatus = result2.getResult().get();
        assertEquals("Delete Empty scope", DeleteScopeStatus.SUCCESS, deleteScopeStatus);

        // To verify that SCOPE3 is infact deleted in above delete call
        ThriftAsyncCallback<DeleteScopeStatus> result7 = new ThriftAsyncCallback<>();
        this.controllerService.deleteScope(scope3, result7);
        deleteScopeStatus = result7.getResult().get();
        assertEquals("Verify that Scope3 is infact deleted", DeleteScopeStatus.SCOPE_NOT_FOUND, deleteScopeStatus);

        // Delete Non-empty Scope SCOPE2
        ThriftAsyncCallback<CreateScopeStatus> result3 = new ThriftAsyncCallback<>();
        this.controllerService.createScope(scope2, result3);
        createScopeStatus = result3.getResult().get();
        assertEquals("Create Scope", CreateScopeStatus.SUCCESS, createScopeStatus);

        final ScalingPolicy policy1 = new ScalingPolicy(ScalingPolicy.Type.FIXED_NUM_SEGMENTS, 100L, 2, 2);
        final StreamConfiguration configuration1 =
                StreamConfiguration.builder().scope(scope2).streamName(stream1).scalingPolicy(policy1).build();
        ThriftAsyncCallback<CreateStreamStatus> result4 = new ThriftAsyncCallback<>();
        this.controllerService.createStream(ModelHelper.decode(configuration1), result4);
        createStreamStatus = result4.getResult().get();
        assertEquals(createStreamStatus, CreateStreamStatus.SUCCESS);

        ThriftAsyncCallback<DeleteScopeStatus> result5 = new ThriftAsyncCallback<>();
        this.controllerService.deleteScope(scope2, result5);
        deleteScopeStatus = result5.getResult().get();
        assertEquals("Delete non empty scope", DeleteScopeStatus.SCOPE_NOT_EMPTY, deleteScopeStatus);

        // Delete Non-existent scope SCOPE3
        ThriftAsyncCallback<DeleteScopeStatus> result6 = new ThriftAsyncCallback<>();
        this.controllerService.deleteScope("SCOPE3", result6);
        deleteScopeStatus = result6.getResult().get();
        assertEquals("Delete non existent scope", DeleteScopeStatus.SCOPE_NOT_FOUND, deleteScopeStatus);
    }

    @Test
    public void createStreamTests() throws TException, ExecutionException, InterruptedException {
        final ScalingPolicy policy1 = new ScalingPolicy(ScalingPolicy.Type.FIXED_NUM_SEGMENTS, 100L, 2, 2);
        final ScalingPolicy policy2 = new ScalingPolicy(ScalingPolicy.Type.FIXED_NUM_SEGMENTS, 100L, 2, 3);
        final StreamConfiguration configuration1 =
                StreamConfiguration.builder().scope(scope1).streamName(stream1).scalingPolicy(policy1).build();
        final StreamConfiguration configuration2 =
                StreamConfiguration.builder().scope(scope1).streamName(stream2).scalingPolicy(policy2).build();
        final StreamConfiguration configuration3 =
                StreamConfiguration.builder().scope("SCOPE3").streamName(stream2).scalingPolicy(policy2).build();

        CreateStreamStatus status;

        // region checkStream
        ThriftAsyncCallback<CreateStreamStatus> result1 = new ThriftAsyncCallback<>();
        ThriftAsyncCallback<CreateScopeStatus> result = new ThriftAsyncCallback<>();
        this.controllerService.createScope(scope1, result);
        result.getResult().get();
        this.controllerService.createStream(ModelHelper.decode(configuration1), result1);
        status = result1.getResult().get();
        assertEquals(status, CreateStreamStatus.SUCCESS);

        ThriftAsyncCallback<CreateStreamStatus> result2 = new ThriftAsyncCallback<>();
        this.controllerService.createStream(ModelHelper.decode(configuration2), result2);
        status = result2.getResult().get();
        assertEquals(status, CreateStreamStatus.SUCCESS);
        // endregion

        // region duplicate create stream
        ThriftAsyncCallback<CreateStreamStatus> result3 = new ThriftAsyncCallback<>();
        this.controllerService.createStream(ModelHelper.decode(configuration1), result3);
        status = result3.getResult().get();
        assertEquals(status, CreateStreamStatus.STREAM_EXISTS);
        // endregion

        // create stream for non-existent scope
        ThriftAsyncCallback<CreateStreamStatus> result4 = new ThriftAsyncCallback<>();
        this.controllerService.createStream(ModelHelper.decode(configuration3), result4);
        status = result4.getResult().get();
        assertEquals(status, CreateStreamStatus.SCOPE_NOT_FOUND);

        //create stream with invalid stream name "abc/def"
        ThriftAsyncCallback<CreateStreamStatus> result5 = new ThriftAsyncCallback<>();
        final StreamConfiguration configuration4 =
                StreamConfiguration.builder().scope("SCOPE3").streamName("abc/def").scalingPolicy(policy2).build();
        this.controllerService.createStream(ModelHelper.decode(configuration4), result5);
        status = result5.getResult().get();
        assertEquals(status, CreateStreamStatus.INVALID_STREAM_NAME);
    }
}
