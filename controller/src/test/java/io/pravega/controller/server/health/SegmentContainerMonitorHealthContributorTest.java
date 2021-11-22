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
package io.pravega.controller.server.health;

import io.pravega.controller.fault.ContainerBalancer;
import io.pravega.controller.fault.SegmentContainerMonitor;
import io.pravega.controller.store.host.HostControllerStore;
import io.pravega.shared.health.Health;
import io.pravega.shared.health.Status;
import org.apache.curator.CuratorZookeeperClient;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.listen.Listenable;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.Assert;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

/**
 * Unit tests for SegmentContainerMonitorHealthContributor
 */
public class SegmentContainerMonitorHealthContributorTest {
    private SegmentContainerMonitor monitor;
    private SegmentContainerMonitorHealthContributor contributor;
    private Health.HealthBuilder builder;

    @Before
    public void setup() {
        HostControllerStore hostStore = mock(HostControllerStore.class);
        CuratorFramework client = mock(CuratorFramework.class);
        ContainerBalancer balancer = mock(ContainerBalancer.class);
        CuratorZookeeperClient curatorZKClientMock = mock(CuratorZookeeperClient.class);
        Listenable listen = mock(Listenable.class);
        doNothing().when(listen).addListener(any(ConnectionStateListener.class));
        doReturn(listen).when(client).getConnectionStateListenable();
        doReturn(curatorZKClientMock).when(client).getZookeeperClient();
        doReturn(true).when(curatorZKClientMock).isConnected();
        monitor = spy(new SegmentContainerMonitor(hostStore, client, balancer, 1));
        contributor = new SegmentContainerMonitorHealthContributor("segmentcontainermonitor", monitor);
        builder = Health.builder().name("monitor");
    }

    @After
    public void tearDown() {
        contributor.close();
    }

    @Test
    public void testHealthCheck() throws Exception {
        monitor.startAsync();
        monitor.awaitRunning();
        Status status = contributor.doHealthCheck(builder);
        Assert.assertEquals(Status.UP, status);
        monitor.stopAsync();
        monitor.awaitTerminated();
        status = contributor.doHealthCheck(builder);
        Assert.assertEquals(Status.DOWN, status);
    }
}