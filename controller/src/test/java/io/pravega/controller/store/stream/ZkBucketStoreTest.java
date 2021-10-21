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
package io.pravega.controller.store.stream;

import com.google.common.collect.ImmutableMap;
import io.pravega.test.common.TestingServerStarter;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class ZkBucketStoreTest extends BucketStoreTest {
    private TestingServer zkServer;
    private CuratorFramework cli;

    @Before
    @Override
    public void setUp() throws Exception {
        zkServer = new TestingServerStarter().start();
        zkServer.start();
        cli = CuratorFrameworkFactory.newClient(zkServer.getConnectString(), 10000, 10000,
                (r, e, s) -> false);
        cli.start();
        super.setUp();
    }

    @After
    @Override
    public void tearDown() throws Exception {
        cli.close();
        zkServer.close();
        super.tearDown();
    }

    @Override
    public BucketStore getBucketStore(ImmutableMap<BucketStore.ServiceType, Integer> map) {
        ZookeeperBucketStore store = new ZookeeperBucketStore(map, cli, executorService);
        map.forEach((service, bucketCount) -> {
            store.createBucketsRoot(service).join();
            for (int bucket = 0; bucket < bucketCount; bucket++) {
                store.createBucket(service, bucket).join();
            }
        });
        
        return store;
    }
    
    @Test(timeout = 10000L)
    public void testCheckExists() {
        ImmutableMap<BucketStore.ServiceType, Integer> map =
                ImmutableMap.of(BucketStore.ServiceType.RetentionService, 1, BucketStore.ServiceType.WatermarkingService, 1);

        CuratorFramework spied = spy(cli);
        ZookeeperBucketStore store = new ZookeeperBucketStore(map, spied, executorService);
        store.addStreamToBucketStore(BucketStore.ServiceType.RetentionService, "scope", "stream", executorService).join();
        verify(spied, times(1)).create();
        verify(spied, times(1)).checkExists();

        store.addStreamToBucketStore(BucketStore.ServiceType.RetentionService, "scope", "stream", executorService).join();
        verify(spied, times(1)).create();
        verify(spied, times(2)).checkExists();
    }
}
