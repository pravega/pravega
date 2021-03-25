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
package io.pravega.controller.store.kvtable;


import io.pravega.controller.store.stream.StreamStoreFactory;
import io.pravega.controller.store.stream.StoreException;
import io.pravega.controller.stream.api.grpc.v1.Controller;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.TestingServerStarter;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.junit.Test;


/**
 * Zookeeper based stream metadata store tests.
 */
public class ZookeeperKVTMetadataStoreTest extends KVTableMetadataStoreTest {

    private TestingServer zkServer;
    private CuratorFramework cli;

    @Override
    public void setupStore() throws Exception {
        zkServer = new TestingServerStarter().start();
        zkServer.start();
        int sessionTimeout = 8000;
        int connectionTimeout = 5000;
        cli = CuratorFrameworkFactory.newClient(zkServer.getConnectString(), sessionTimeout, connectionTimeout, new RetryOneTime(2000));
        cli.start();
        // setup Stream Store, needed for creating scopes
        streamStore = StreamStoreFactory.createZKStore(cli, executor);
        store = KVTableStoreFactory.createZKStore(cli, executor);
    }

    @Override
    public void cleanupStore() throws Exception {
        store.close();
        streamStore.close();
        cli.close();
        zkServer.close();
    }

    @Override
    Controller.CreateScopeStatus createScope(String scopeName) throws Exception {
        return streamStore.createScope(scopeName).get();
    }

    @Test
    public void testInvalidOperation() throws Exception {
        // Test operation when stream is not in active state
        streamStore.createScope(scope).get();
        byte[] newUUID = store.newScope(scope).newId();
        store.createEntryForKVTable(scope, kvtable1, newUUID, executor);
        store.createKeyValueTable(scope, kvtable1, configuration1, System.currentTimeMillis(), null, executor).get();
        store.setState(scope, kvtable1, KVTableState.CREATING, null, executor).get();

        AssertExtensions.assertFutureThrows("Should throw IllegalStateException",
                store.getActiveSegments(scope, kvtable1, null, executor),
                (Throwable t) -> t instanceof StoreException.IllegalStateException);
    }


}
