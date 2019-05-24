package io.pravega.controller.server.bucket;

import com.google.common.collect.ImmutableMap;
import io.pravega.client.ClientConfig;
import io.pravega.client.netty.impl.ConnectionFactoryImpl;
import io.pravega.client.stream.Stream;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.controller.mocks.SegmentHelperMock;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.server.rpc.auth.AuthHelper;
import io.pravega.controller.store.stream.BucketStore;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.StreamStoreFactory;
import io.pravega.controller.store.task.TaskMetadataStore;
import io.pravega.controller.store.task.TaskStoreFactory;
import io.pravega.controller.task.Stream.StreamMetadataTasks;
import io.pravega.test.common.TestingServerStarter;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;
import java.util.function.Supplier;

public class WatermarkWorkflowTest {
    TestingServer zkServer;
    CuratorFramework zkClient;

    StreamMetadataStore streamMetadataStore;
    BucketStore bucketStore;
    ScheduledExecutorService executor;
    PeriodicWatermarking periodicWatermarking;
    
    @Before
    public void setUp() throws Exception {
        zkServer = new TestingServerStarter().start();

        zkClient = CuratorFrameworkFactory.newClient(zkServer.getConnectString(), 10000, 1000,
                (r, e, s) -> false);

        zkClient.start();

        executor = Executors.newScheduledThreadPool(10);

        streamMetadataStore = StreamStoreFactory.createPravegaTablesStore(SegmentHelperMock.getSegmentHelperMockForTables(executor),
                AuthHelper.getDisabledAuthHelper(), zkClient, executor);
        ImmutableMap<BucketStore.ServiceType, Integer> map = ImmutableMap.of(BucketStore.ServiceType.RetentionService, 3,
                BucketStore.ServiceType.WatermarkingService, 3);
        bucketStore = StreamStoreFactory.createZKBucketStore(map, zkClient, executor);

        Function<Stream, PeriodicWatermarking.WatermarkClient> supplier = null;
        periodicWatermarking = new PeriodicWatermarking(streamMetadataStore, bucketStore, supplier, executor);
    }
    
    @After
    public void tearDown() throws Exception {
        streamMetadataStore.close();
        ExecutorServiceHelpers.shutdown(executor);

        streamMetadataStore.close();
        zkClient.close();
        zkServer.close();
    }
    
    // test
    // 
    
}
