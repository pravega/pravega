/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.server.v1;

import com.emc.pravega.controller.server.rpc.v1.ControllerService;
import com.emc.pravega.controller.server.rpc.v1.ControllerServiceAsyncImpl;
import com.emc.pravega.controller.server.rpc.v1.SegmentHelperMock;
import com.emc.pravega.controller.store.StoreClient;
import com.emc.pravega.controller.store.ZKStoreClient;
import com.emc.pravega.controller.store.host.HostControllerStore;
import com.emc.pravega.controller.store.host.HostStoreFactory;
import com.emc.pravega.controller.store.stream.StreamMetadataStore;
import com.emc.pravega.controller.store.stream.StreamStoreFactory;
import com.emc.pravega.controller.store.task.TaskMetadataStore;
import com.emc.pravega.controller.store.task.TaskStoreFactory;
import com.emc.pravega.controller.stream.api.v1.CreateStreamStatus;
import com.emc.pravega.controller.task.Stream.StreamMetadataTasks;
import com.emc.pravega.controller.task.Stream.StreamTransactionMetadataTasks;
import com.emc.pravega.controller.util.ThriftAsyncCallback;
import com.emc.pravega.stream.ScalingPolicy;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.impl.ModelHelper;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.thrift.TException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Async Controller Service Impl tests.
 */
public class ControllerServiceAsyncImplTest {

    private static final String SCOPE = "scope";
    private final String stream1 = "stream1";
    private final String stream2 = "stream2";
    private final ControllerServiceAsyncImpl controllerService;

    private final TestingServer zkServer;

    public ControllerServiceAsyncImplTest() throws Exception {
        zkServer = new TestingServer();
        zkServer.start();

        SegmentHelperMock.init();
        CuratorFramework zkClient = CuratorFrameworkFactory.newClient(zkServer.getConnectString(),
                new ExponentialBackoffRetry(200, 10, 5000));
        zkClient.start();

        final ScheduledExecutorService executor = Executors.newScheduledThreadPool(20,
                new ThreadFactoryBuilder().setNameFormat("testpool-%d").build());

        StoreClient storeClient = new ZKStoreClient(zkClient);

        final StreamMetadataStore streamStore = StreamStoreFactory.createStore(StreamStoreFactory.StoreType.InMemory,
                executor);

        final TaskMetadataStore taskMetadataStore = TaskStoreFactory.createStore(storeClient, executor);

        final HostControllerStore hostStore = HostStoreFactory.createStore(HostStoreFactory.StoreType.InMemory);

        StreamMetadataTasks streamMetadataTasks = new StreamMetadataTasks(streamStore, hostStore, taskMetadataStore,
                executor, "host");
        StreamTransactionMetadataTasks streamTransactionMetadataTasks =
                new StreamTransactionMetadataTasks(streamStore, hostStore, taskMetadataStore, executor, "host");

        this.controllerService = new ControllerServiceAsyncImpl(new ControllerService(streamStore, hostStore, streamMetadataTasks,
                streamTransactionMetadataTasks, executor));
    }

    @Test
    public void createStreamTests() throws TException, ExecutionException, InterruptedException {
        final ScalingPolicy policy1 = new ScalingPolicy(ScalingPolicy.Type.FIXED_NUM_SEGMENTS, 100, 2, 2);
        final ScalingPolicy policy2 = new ScalingPolicy(ScalingPolicy.Type.FIXED_NUM_SEGMENTS, 100, 2, 3);
        final StreamConfiguration configuration1 = StreamConfiguration.builder().scope(SCOPE).streamName(stream1).scalingPolicy(policy1).build();
        final StreamConfiguration configuration2 = StreamConfiguration.builder().scope(SCOPE).streamName(stream2).scalingPolicy(policy2).build();

        CreateStreamStatus status;

        // region checkStream
        ThriftAsyncCallback<CreateStreamStatus> result1 = new ThriftAsyncCallback<>();
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
    }
}
