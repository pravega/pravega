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
import io.pravega.controller.store.checkpoint.CheckpointStore;
import io.pravega.controller.store.checkpoint.CheckpointStoreException;
import io.pravega.controller.store.checkpoint.CheckpointStoreFactory;
import io.pravega.test.common.AssertExtensions;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.io.IOException;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

public class ZkCheckpointStoreConnectivityTest {
    @Rule
    public Timeout globalTimeout = new Timeout(30, TimeUnit.SECONDS);

    private CuratorFramework cli;
    private CheckpointStore checkpointStore;

    @Before
    public void setup() throws Exception {
        cli = CuratorFrameworkFactory.newClient("localhost:0000", 10000, 1, (r, e, s) -> false);
        cli.start();
        checkpointStore = CheckpointStoreFactory.createZKStore(cli);
    }

    @After
    public void tearDown() throws IOException {
        cli.close();
    }

    @Test
    public void connectivityFailureTests() throws IOException {
        final String process1 = UUID.randomUUID().toString();
        final String readerGroup1 = UUID.randomUUID().toString();
        final String reader1 = UUID.randomUUID().toString();

        Predicate<Throwable> predicate = e -> e instanceof CheckpointStoreException &&
                ((CheckpointStoreException) e).getType().equals(CheckpointStoreException.Type.Connectivity);
        AssertExtensions.assertThrows("failed getProcesses", () -> checkpointStore.getProcesses(), predicate);

        AssertExtensions.assertThrows("failed addReaderGroup",
                () -> checkpointStore.addReaderGroup(process1, readerGroup1), predicate);

        AssertExtensions.assertThrows("failed addReader",
                () -> checkpointStore.addReader(process1, readerGroup1, reader1), predicate);

        AssertExtensions.assertThrows("failed sealReaderGroup",
                () -> checkpointStore.sealReaderGroup(process1, readerGroup1), predicate);

        AssertExtensions.assertThrows("failed removeReader",
                () -> checkpointStore.removeReader(process1, readerGroup1, reader1), predicate);

        AssertExtensions.assertThrows("failed getPositions",
                () -> checkpointStore.getPositions(process1, readerGroup1), predicate);

        Position position = new PositionImpl(Collections.emptyMap());
        AssertExtensions.assertThrows("failed setPosition",
                () -> checkpointStore.setPosition(process1, readerGroup1, reader1, position), predicate);

        AssertExtensions.assertThrows("failed removeReader",
                () -> checkpointStore.removeReader(process1, readerGroup1, reader1), predicate);

        AssertExtensions.assertThrows("failed removeReaderGroup",
                () -> checkpointStore.removeReaderGroup(process1, readerGroup1), predicate);
    }
}
