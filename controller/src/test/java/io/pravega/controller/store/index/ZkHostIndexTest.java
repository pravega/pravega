/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
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
package io.pravega.controller.store.index;

import io.pravega.test.common.TestingServerStarter;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class ZkHostIndexTest {
    protected final ScheduledExecutorService executor = Executors.newScheduledThreadPool(10);
    protected CuratorFramework cli;
    private TestingServer zkServer;

    @Before
    public void setUp() throws Exception {
        zkServer = new TestingServerStarter().start();
        zkServer.start();

        cli = CuratorFrameworkFactory.newClient(zkServer.getConnectString(), new RetryOneTime(2000));
        cli.start();
    }
    
    @After
    public void tearDown() throws IOException {
        cli.close();
        zkServer.stop();
        zkServer.close();
        executor.shutdown();
    }
    
    @Test
    public void testSync() {
        ZKHostIndex index = spy(new ZKHostIndex(cli, "/hostRequestIndex", executor));
        String hostId = "hostId";
        index.addEntity(hostId, "entity").join();
        List<String> entities = index.getEntities(hostId).join();
        verify(index, times(1)).sync(any());
        assertEquals(entities.size(), 1);
    }
}
