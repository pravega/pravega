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

import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.controller.store.ZKStoreHelper;
import io.pravega.test.common.TestingServerStarter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class ZkOrderedStoreTest {

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
    public void testStore() {
        String test = "test";
        String scope = "test";
        String stream = "test";
        // 2 entities per collection.. rollover after = 1
        ZkOrderedStore store = new ZkOrderedStore(test, zkStoreHelper, executor, 1);
        
        // add 5 entities
        long position1 = store.addEntity(scope, stream, test + 1, 0L).join();
        assertEquals(0L, position1);
        // verify that set 0 is not sealed
        assertFalse(store.isSealed(scope, stream, 0).join());
        
        long position2 = store.addEntity(scope, stream, test + 2, 0L).join();
        assertEquals(1L, position2);
        // verify that set 0 is still not sealed
        assertFalse(store.isSealed(scope, stream, 0).join());

        long position3 = store.addEntity(scope, stream, test + 3, 0L).join();
        assertEquals(ZkOrderedStore.Position.toLong(1, 0), position3);
        // verify that set 0 is sealed
        assertTrue(store.isSealed(scope, stream, 0).join());

        long position4 = store.addEntity(scope, stream, test + 4, 0L).join();
        assertEquals(ZkOrderedStore.Position.toLong(1, 1), position4);
        
        long position5 = store.addEntity(scope, stream, test + 5, 0L).join();
        assertEquals(ZkOrderedStore.Position.toLong(2, 0), position5);
        // verify that set 1 is sealed
        assertTrue(store.isSealed(scope, stream, 1).join());

        // get entries and verify that we are able to get all 5 entries. 
        Map<Long, String> entities = store.getEntitiesWithPosition(scope, stream).join();
        assertEquals(5, entities.size());
        assertTrue(entities.containsKey(position1) && entities.get(position1).equals(test + 1));
        assertTrue(entities.containsKey(position2) && entities.get(position2).equals(test + 2));
        assertTrue(entities.containsKey(position3) && entities.get(position3).equals(test + 3));
        assertTrue(entities.containsKey(position4) && entities.get(position4).equals(test + 4));
        assertTrue(entities.containsKey(position5) && entities.get(position5).equals(test + 5));
        
        // remove entities such that queue 1 becomes a candidate for removal.
        store.removeEntities(scope, stream, Arrays.asList(position1, position3, position4)).join();
        // verify that only position 2 and position 5 are present in the ordered set.
        entities = store.getEntitiesWithPosition(scope, stream).join();
        assertEquals(2, entities.size());
        assertTrue(entities.containsKey(position2));
        assertTrue(entities.containsKey(position5));
        assertFalse(store.positionExists(scope, stream, position1).join());
        assertTrue(store.positionExists(scope, stream, position2).join());
        assertFalse(store.positionExists(scope, stream, position3).join());
        assertFalse(store.positionExists(scope, stream, position4).join());
        assertTrue(store.positionExists(scope, stream, position5).join());
        
        // verify that set 1 is removed
        assertTrue(store.isDeleted(scope, stream, 1).join());
        assertFalse(store.isDeleted(scope, stream, 0).join());
        assertFalse(store.isDeleted(scope, stream, 2).join());

        // remove entities such that queue 0 becomes a candidate for removal.
        store.removeEntities(scope, stream, Collections.singletonList(position2)).join();

        assertTrue(store.isDeleted(scope, stream, 0).join());
        assertFalse(store.isDeleted(scope, stream, 2).join());

        entities = store.getEntitiesWithPosition(scope, stream).join();
        assertEquals(1, entities.size());
        assertTrue(entities.containsKey(position5));

        store.removeEntities(scope, stream, Collections.singletonList(position5)).join();
        // verify that collection 2 is not deleted as it is not sealed
        assertFalse(store.isDeleted(scope, stream, 2).join());
    }

    @Test(timeout = 30000)
    public void testSync() {
        String test = "test";
        String scope = "test";
        String stream = "test";
        ZKStoreHelper zkStoreHelper = spy(this.zkStoreHelper);
        ZkOrderedStore store = new ZkOrderedStore(test, zkStoreHelper, executor, 1);
        
        store.addEntity(scope, stream, test + 1, 0L).join();
        store.getEntitiesWithPosition(scope, stream).join();
        
        verify(zkStoreHelper, times(1)).sync(any());
    }
}
