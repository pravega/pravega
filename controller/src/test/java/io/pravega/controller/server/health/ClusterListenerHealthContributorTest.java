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

import io.pravega.common.cluster.Cluster;
import io.pravega.common.cluster.Host;
import io.pravega.controller.fault.ControllerClusterListener;
import io.pravega.controller.fault.FailoverSweeper;
import io.pravega.controller.server.ControllerServiceConfig;
import io.pravega.controller.server.impl.ControllerServiceConfigImpl;
import io.pravega.controller.task.Stream.TxnSweeper;
import io.pravega.controller.task.TaskSweeper;
import io.pravega.shared.health.Health;
import io.pravega.shared.health.Status;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

/**
 * Unit tests for ClusterListenerHealthContributor
 */
public class ClusterListenerHealthContributorTest {
    private ControllerClusterListener  clusterListener;
    private ClusterListenerHealthContributor contributor;
    private Health.HealthBuilder builder;

    @Before
    public void setup() {
        Host host = mock(Host.class);
        Cluster cluster = mock(Cluster.class);
        ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
        ControllerServiceConfig serviceConfig = mock(ControllerServiceConfigImpl.class);
        TaskSweeper taskSweeper = mock(TaskSweeper.class);
        TxnSweeper txnSweeper = mock(TxnSweeper.class);
        List<FailoverSweeper> failoverSweepers = new ArrayList<>();
        failoverSweepers.add(taskSweeper);
        failoverSweepers.add(txnSweeper);

        doReturn(true).when(serviceConfig).isControllerClusterListenerEnabled();
        clusterListener = spy(new ControllerClusterListener(host, cluster, executor, failoverSweepers));
        doReturn(true).when(clusterListener).isReady();
        contributor = new ClusterListenerHealthContributor("clusterlistener", clusterListener);
        builder = Health.builder().name("clusterlistener");
    }

    @After
    public void tearDown() {
        contributor.close();
    }

    @Test
    public void testHealthCheck() throws Exception {
        clusterListener.startAsync();
        clusterListener.awaitRunning();
        Status status = contributor.doHealthCheck(builder);
        Assert.assertEquals(Status.UP, status);
        clusterListener.stopAsync();
        clusterListener.awaitTerminated();
        status = contributor.doHealthCheck(builder);
        Assert.assertEquals(Status.DOWN, status);
    }
}
