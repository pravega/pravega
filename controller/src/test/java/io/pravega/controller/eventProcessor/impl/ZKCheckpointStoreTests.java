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
package io.pravega.controller.eventProcessor.impl;

import io.pravega.client.stream.Position;
import io.pravega.client.stream.impl.PositionImpl;
import io.pravega.controller.store.checkpoint.CheckpointStoreException;
import io.pravega.controller.store.checkpoint.CheckpointStoreFactory;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.TestingServerStarter;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Predicate;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.KeeperException;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for Zookeeper based checkpoint store.
 */
public class ZKCheckpointStoreTests extends CheckpointStoreTests {
    private TestingServer zkServer;
    private CuratorFramework cli;

    @Override
    public void setupCheckpointStore() throws Exception {
        zkServer = new TestingServerStarter().start();
        zkServer.start();
        cli = CuratorFrameworkFactory.newClient(zkServer.getConnectString(), 10000, 10000, new RetryOneTime(10000));
        cli.start();
        checkpointStore = CheckpointStoreFactory.createZKStore(cli);
    }

    @Override
    public void cleanupCheckpointStore() throws IOException {
        cli.close();
        zkServer.close();
    }

    @Test(timeout = 30000)
    public void failingTests() {
        final String process1 = UUID.randomUUID().toString();
        final String readerGroup1 = UUID.randomUUID().toString();
        final String readerGroup2 = UUID.randomUUID().toString();
        final String reader1 = UUID.randomUUID().toString();
        cli.close();

        Predicate<Throwable> predicate = e -> e instanceof CheckpointStoreException && e.getCause() instanceof IllegalStateException;
        AssertExtensions.assertThrows("failed getProcesses", () -> checkpointStore.getProcesses(), predicate);

        AssertExtensions.assertThrows("failed addReaderGroup",
                () -> checkpointStore.addReaderGroup(process1, readerGroup1), predicate);

        AssertExtensions.assertThrows("failed getReaderGroups",
                () -> checkpointStore.getReaderGroups(process1), predicate);

        AssertExtensions.assertThrows("failed addReader",
                () -> checkpointStore.addReader(process1, readerGroup1, reader1), predicate);

        Position position = new PositionImpl(Collections.emptyMap());
        AssertExtensions.assertThrows("failed setPosition",
                () -> checkpointStore.setPosition(process1, readerGroup1, reader1, position), predicate);

        AssertExtensions.assertThrows("failed getPositions",
                () -> checkpointStore.getPositions(process1, readerGroup1), predicate);

        AssertExtensions.assertThrows("failed sealReaderGroup",
                () -> checkpointStore.sealReaderGroup(process1, readerGroup2), predicate);

        AssertExtensions.assertThrows("failed removeReader",
                () -> checkpointStore.removeReader(process1, readerGroup1, reader1), predicate);

        AssertExtensions.assertThrows("failed removeReaderGroup",
                () -> checkpointStore.removeReaderGroup(process1, readerGroup1), predicate);
    }

    @Test(timeout = 30000)
    public void testRemoveProcess() throws Exception {
        final String process1 = "process1";
        final String readerGroup1 = "rg1";
        final String reader1 = "reader1";
        final String reader2 = "reader2";

        Set<String> processes = checkpointStore.getProcesses();
        Assert.assertEquals(0, processes.size());

        checkpointStore.addReaderGroup(process1, readerGroup1);
        List<String> result = checkpointStore.getReaderGroups(process1);
        Assert.assertNotNull(result);
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(readerGroup1, result.get(0));

        processes = checkpointStore.getProcesses();
        Assert.assertEquals(1, processes.size());

        checkpointStore.addReader(process1, readerGroup1, reader1);
        Map<String, Position> resultMap = checkpointStore.getPositions(process1, readerGroup1);
        Assert.assertNotNull(resultMap);
        Assert.assertEquals(1, resultMap.size());
        Assert.assertNull(resultMap.get(reader1));
        
        checkpointStore.addReader(process1, readerGroup1, reader2);
        resultMap = checkpointStore.getPositions(process1, readerGroup1);
        Assert.assertNotNull(resultMap);
        Assert.assertEquals(2, resultMap.size());

        Map<String, Position> readerPositions = checkpointStore.sealReaderGroup(process1, readerGroup1);
        Assert.assertEquals(readerPositions.size(), 2);
        Assert.assertTrue(readerPositions.containsKey(reader1));
        Assert.assertNull(readerPositions.get(reader1));
        Assert.assertTrue(readerPositions.containsKey(reader2));
        Assert.assertNull(readerPositions.get(reader2));

        for (Map.Entry<String, Position> entry : readerPositions.entrySet()) {
            // 2. Remove reader from Checkpoint Store
            checkpointStore.removeReader(process1, readerGroup1, entry.getKey());
        }

        // Finally, remove reader group from the checkpoint store
        checkpointStore.removeReaderGroup(process1, readerGroup1);
        AssertExtensions.assertThrows(KeeperException.NoNodeException.class, () -> {            
            cli.getData().forPath(String.format("/%s/%s/%s/%s", "eventProcessors", process1, readerGroup1, reader1));
        });
    }
    
    @Test(timeout = 30000)
    public void readerWithoutCheckpointTest() throws Exception {
        final String process1 = "process1";
        final String readerGroup1 = "rg1";
        final String reader1 = "reader1";
        final String reader2 = "reader2";

        Assert.assertFalse(checkpointStore.isHealthy());
        Set<String> processes = checkpointStore.getProcesses();
        Assert.assertEquals(0, processes.size());

        checkpointStore.addReaderGroup(process1, readerGroup1);
        List<String> result = checkpointStore.getReaderGroups(process1);
        Assert.assertNotNull(result);
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(readerGroup1, result.get(0));

        processes = checkpointStore.getProcesses();
        Assert.assertEquals(1, processes.size());

        checkpointStore.addReader(process1, readerGroup1, reader1);
        Map<String, Position> resultMap = checkpointStore.getPositions(process1, readerGroup1);
        Assert.assertNotNull(resultMap);
        Assert.assertEquals(1, resultMap.size());
        Assert.assertNull(resultMap.get(reader1));
        
        checkpointStore.addReader(process1, readerGroup1, reader2);
        resultMap = checkpointStore.getPositions(process1, readerGroup1);
        Assert.assertNotNull(resultMap);
        Assert.assertEquals(2, resultMap.size());

        // delete the node for the reader. 
        cli.delete().forPath(String.format("/%s/%s/%s/%s", "eventProcessors", process1, readerGroup1, reader1));

        Map<String, Position> map = checkpointStore.sealReaderGroup(process1, readerGroup1);
        Assert.assertEquals(map.size(), 2);
        Assert.assertTrue(map.containsKey(reader1));
        Assert.assertNull(map.get(reader1));
        Assert.assertTrue(map.containsKey(reader2));
        Assert.assertNull(map.get(reader2));
        map.keySet().forEach(x -> {
            try {
                checkpointStore.removeReader(process1, readerGroup1, x);
            } catch (CheckpointStoreException e) {
                throw new RuntimeException(e);
            }
        });
        checkpointStore.removeReaderGroup(process1, readerGroup1);
    }
}

