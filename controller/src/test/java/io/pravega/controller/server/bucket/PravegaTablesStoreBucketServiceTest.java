/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.controller.server.bucket;

import com.google.common.collect.ImmutableMap;
import io.pravega.common.cluster.Cluster;
import io.pravega.common.cluster.ClusterType;
import io.pravega.common.cluster.Host;
import io.pravega.common.cluster.zkImpl.ClusterZKImpl;
import io.pravega.controller.mocks.SegmentHelperMock;
import io.pravega.controller.server.security.auth.GrpcAuthHelper;
import io.pravega.controller.store.stream.BucketStore;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.StreamStoreFactory;
import io.pravega.test.common.TestingServerStarter;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static io.pravega.test.common.AssertExtensions.assertEventuallyEquals;

public class PravegaTablesStoreBucketServiceTest extends BucketServiceTest {
    private TestingServer zkServer;
    private CuratorFramework zkClient;
    private Cluster cluster;

    @Override
    @Before
    public void setup() throws Exception {
        zkServer = new TestingServerStarter().start();
        zkServer.start();

        zkClient = CuratorFrameworkFactory.newClient(zkServer.getConnectString(), 10000, 1000,
                (r, e, s) -> false);

        zkClient.start();
        cluster = new ClusterZKImpl(zkClient, ClusterType.CONTROLLER);
        super.setup();
    }

    @Override
    protected void addEntryToZkCluster(Host host) {
        addControllerToZkCluster(host, cluster);
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
        cluster.close();
        streamMetadataStore.close();
        zkClient.close();
        zkServer.close();
    }

    @Override
    StreamMetadataStore createStreamStore(ScheduledExecutorService executor) {
        return StreamStoreFactory.createPravegaTablesStore(SegmentHelperMock.getSegmentHelperMockForTables(executor), 
                                                           GrpcAuthHelper.getDisabledAuthHelper(), zkClient, executor);
    }

    @Override
    BucketStore createBucketStore(int bucketCount) {
        ImmutableMap<BucketStore.ServiceType, Integer> map = ImmutableMap.of(BucketStore.ServiceType.RetentionService, bucketCount,
                BucketStore.ServiceType.WatermarkingService, bucketCount);
        return StreamStoreFactory.createZKBucketStore(map, zkClient, executor);
    }



    @Test(timeout = 30000)
    public void testFailover() throws Exception {
        addEntryToZkCluster(controller);
        assertEventuallyEquals(3, () -> watermarkingService.getBucketServices().size(), 10000);
        assertEventuallyEquals(3, () -> retentionService.getBucketServices().size(), 10000);

        //add new controller instance in pravgea cluster.
        Host controller1 = new Host(UUID.randomUUID().toString(), 9090, null);
        addEntryToZkCluster(controller1);
        assertEventuallyEquals(2, () -> retentionService.getBucketServices().size(), 10000);
        assertEventuallyEquals(2, () -> watermarkingService.getBucketServices().size(), 10000);

        //add new controller instance in pravgea cluster.
        Host controller2 = new Host(UUID.randomUUID().toString(), 9090, null);
        addEntryToZkCluster(controller2);
        assertEventuallyEquals(1, () -> retentionService.getBucketServices().size(), 10000);
        assertEventuallyEquals(1, () -> watermarkingService.getBucketServices().size(), 10000);

        //remove controller instances from pravega cluster.
        removeControllerFromZkCluster(controller1, cluster);
        removeControllerFromZkCluster(controller2, cluster);
        assertEventuallyEquals(3, () -> retentionService.getBucketServices().size(), 10000);
        assertEventuallyEquals(3, () -> watermarkingService.getBucketServices().size(), 10000);
    }
}
