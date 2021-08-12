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

import io.pravega.common.tracing.RequestTracker;
import io.pravega.controller.server.ControllerService;
import io.pravega.controller.server.rpc.grpc.GRPCServer;
import io.pravega.controller.server.rpc.grpc.GRPCServerConfig;
import io.pravega.shared.health.Health;
import io.pravega.shared.health.Status;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

/**
 * Unit tests for GRPCServiceHealthContibutor
 */
public class GRPCServerHealthContributorTest {
    private GRPCServer grpcServer;
    private GRPCServerHealthContributor contributor;
    private Health.HealthBuilder builder;

    @Before
    public void setup() {
        ControllerService service = mock(ControllerService.class);
        GRPCServerConfig config = mock(GRPCServerConfig.class);
        RequestTracker requestTracker = new RequestTracker(config.isRequestTracingEnabled());
        grpcServer = spy(new GRPCServer(service, config, requestTracker));
        contributor = new GRPCServerHealthContributor("grpc", grpcServer);
        builder = Health.builder().name("grpc");
    }

    @After
    public void tearDown() {
        contributor.close();
    }

    @Test
    public void testHealthCheck() throws Exception {
        grpcServer.startAsync();
        grpcServer.awaitRunning();
        Status status = contributor.doHealthCheck(builder);
        Assert.assertEquals(Status.UP, status);
        grpcServer.stopAsync();
        grpcServer.awaitTerminated();
        status = contributor.doHealthCheck(builder);
        Assert.assertEquals(Status.DOWN, status);
    }
}
