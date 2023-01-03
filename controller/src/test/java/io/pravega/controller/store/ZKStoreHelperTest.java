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
package io.pravega.controller.store;

import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.concurrent.Futures;
import io.pravega.controller.store.stream.StoreException;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.TestingServerStarter;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for ZKStoreHelper.
 */
public class ZKStoreHelperTest {

    private TestingServer zkServer;
    private CuratorFramework cli;
    private final ScheduledExecutorService executor = ExecutorServiceHelpers.newScheduledThreadPool(1, "test");
    private ZKStoreHelper zkStoreHelper;

    @Before
    public void setup() throws Exception {
        zkServer = new TestingServerStarter().start();
        cli = CuratorFrameworkFactory.newClient(zkServer.getConnectString(), 10000, 10000, new RetryNTimes(0, 0));
        cli.start();
        zkStoreHelper = new ZKStoreHelper(cli, executor);
    }

    @After
    public void tearDown() throws IOException {
        ExecutorServiceHelpers.shutdown(executor);
        cli.close();
        zkServer.close();
    }

    @Test(timeout = 30000)
    public void testAddNode() throws ExecutionException, InterruptedException, IOException {
        Assert.assertNull(zkStoreHelper.addNode("/test/test1").get());
        AssertExtensions.assertFutureThrows("Should throw NodeExistsException", zkStoreHelper.addNode("/test/test1"),
                e -> e instanceof StoreException.DataExistsException);
        zkServer.stop();
        AssertExtensions.assertFutureThrows("Should throw UnknownException", zkStoreHelper.addNode("/test/test2"),
                e -> e instanceof StoreException.StoreConnectionException);
    }

    @Test(timeout = 30000)
    public void testDeleteNode() throws ExecutionException, InterruptedException, IOException {
        Assert.assertNull(zkStoreHelper.addNode("/test/test1").get());

        Assert.assertNull(zkStoreHelper.addNode("/test/test1/test2").get());
        AssertExtensions.assertFutureThrows("Should throw NodeNotEmptyException", zkStoreHelper.deleteNode("/test/test1"),
                e -> e instanceof StoreException.DataNotEmptyException);

        Assert.assertNull(zkStoreHelper.deleteNode("/test/test1/test2").get());

        Assert.assertNull(zkStoreHelper.deleteNode("/test/test1").get());
        AssertExtensions.assertFutureThrows("Should throw NodeNotFoundException", zkStoreHelper.deleteNode("/test/test1"),
                e -> e instanceof StoreException.DataNotFoundException);
        zkServer.stop();
        AssertExtensions.assertFutureThrows("Should throw UnknownException", zkStoreHelper.deleteNode("/test/test1"),
                e -> e instanceof StoreException.StoreConnectionException);
    }
    
    @Test(timeout = 30000)
    public void testDeleteConditionally() {
        String path = "/test/test/deleteConditionally";
        zkStoreHelper.createZNode(path, new byte[0]).join();
        VersionedMetadata<byte[]> data = zkStoreHelper.getData(path, x -> x).join();
        zkStoreHelper.setData(path, data.getObject(), data.getVersion()).join();
        AssertExtensions.assertFutureThrows("delete version mismatch",
                zkStoreHelper.deleteNode(path, data.getVersion()),
                e -> Exceptions.unwrap(e) instanceof StoreException.WriteConflictException);
    }

    @Test(timeout = 30000)
    public void testEphemeralNode() {
        CuratorFramework cli2 = CuratorFrameworkFactory.newClient(zkServer.getConnectString(), new RetryNTimes(0, 0));
        cli2.start();
        ZKStoreHelper zkStoreHelper2 = new ZKStoreHelper(cli2, executor);

        assertTrue(zkStoreHelper2.createEphemeralZNode("/testEphemeral", new byte[0]).join());
        Assert.assertNotNull(zkStoreHelper2.getData("/testEphemeral", x -> x).join());
        zkStoreHelper2.getClient().getZookeeperClient().close();
        zkStoreHelper2.getClient().close();
        // let session get expired.
        // now read the data again. Verify that node no longer exists
        AssertExtensions.assertFutureThrows("", Futures.delayedFuture(() -> zkStoreHelper.getData("/testEphemeral", x -> x), 1000, executor),
                e -> e instanceof StoreException.DataNotFoundException);
    }
    
    @Test(timeout = 30000)
    public void testGetChildren() {
        zkStoreHelper.createZNodeIfNotExist("/1").join();
        zkStoreHelper.createZNodeIfNotExist("/1/1").join();
        zkStoreHelper.createZNodeIfNotExist("/1/2").join();
        zkStoreHelper.createZNodeIfNotExist("/1/3").join();
        zkStoreHelper.createZNodeIfNotExist("/1/4").join();
        zkStoreHelper.createZNodeIfNotExist("/1/1/1").join();
        zkStoreHelper.createZNodeIfNotExist("/1/1/2").join();
        assertEquals(zkStoreHelper.getChildren("/1").join().size(), 4);
        assertEquals(zkStoreHelper.getChildren("/1/1").join().size(), 2);
        assertEquals(zkStoreHelper.getChildren("/1/1/2").join().size(), 0);
        assertEquals(zkStoreHelper.getChildren("/112").join().size(), 0);
        AssertExtensions.assertFutureThrows("data not found",
                zkStoreHelper.getChildren("/112", false),
                e -> e instanceof StoreException.DataNotFoundException);
    }
    
    @Test(timeout = 30000)
    public void testSync() {
        String path = "/path";
        byte[] entry = new byte[1];
        zkStoreHelper.createZNode(path, entry).join();
        zkStoreHelper.sync(path).join();
        byte[] data = zkStoreHelper.getData(path, x -> x).join().getObject();
        assertEquals(1, data.length);
    }
}
