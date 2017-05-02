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
package io.pravega.stream.impl;

import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.controller.stream.api.grpc.v1.Controller.NodeUri;
import io.pravega.controller.stream.api.grpc.v1.Controller.SegmentId;
import io.pravega.controller.stream.api.grpc.v1.Controller.ServerRequest;
import io.pravega.controller.stream.api.grpc.v1.Controller.ServerResponse;
import io.pravega.controller.stream.api.grpc.v1.ControllerServiceGrpc.ControllerServiceImplBase;
import io.pravega.test.common.TestUtils;
import io.grpc.Server;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Tests for ControllerImpl with Service Discovery, Loadbalancing and Failover support.
 */
@Slf4j
public class ControllerImplLBTest {
    @Rule
    public final Timeout globalTimeout = new Timeout(30, TimeUnit.SECONDS);

    private Server testRPCServer1 = null;
    private Server testRPCServer2 = null;
    private Server testRPCServer3 = null;

    @Before
    public void setup() throws IOException {
        final int serverPort1 = TestUtils.getAvailableListenPort();
        final int serverPort2 = TestUtils.getAvailableListenPort();
        final int serverPort3 = TestUtils.getAvailableListenPort();

        // Setup fake servers for simulating multiple controllers with discovery info.
        ControllerServiceImplBase fakeServerImpl1 = new ControllerServiceImplBase() {
            @Override
            public void getControllerServerList(ServerRequest request, StreamObserver<ServerResponse> responseObserver) {
                responseObserver.onNext(ServerResponse.newBuilder()
                        .addNodeURI(NodeUri.newBuilder().setEndpoint("localhost").setPort(serverPort1).build())
                        .addNodeURI(NodeUri.newBuilder().setEndpoint("localhost").setPort(serverPort2).build())
                        .addNodeURI(NodeUri.newBuilder().setEndpoint("localhost").setPort(serverPort3).build())
                        .build());
                responseObserver.onCompleted();
            }

            @Override
            public void getURI(SegmentId request, StreamObserver<NodeUri> responseObserver) {
                responseObserver.onNext(NodeUri.newBuilder().setEndpoint("localhost1").setPort(1).build());
                responseObserver.onCompleted();
            }
        };

        ControllerServiceImplBase fakeServerImpl2 = new ControllerServiceImplBase() {
            @Override
            public void getControllerServerList(ServerRequest request, StreamObserver<ServerResponse> responseObserver) {
                responseObserver.onNext(ServerResponse.newBuilder()
                        .addNodeURI(NodeUri.newBuilder().setEndpoint("localhost").setPort(serverPort1).build())
                        .addNodeURI(NodeUri.newBuilder().setEndpoint("localhost").setPort(serverPort2).build())
                        .addNodeURI(NodeUri.newBuilder().setEndpoint("localhost").setPort(serverPort3).build())
                        .build());
                responseObserver.onCompleted();
            }

            @Override
            public void getURI(SegmentId request, StreamObserver<NodeUri> responseObserver) {
                responseObserver.onNext(NodeUri.newBuilder().setEndpoint("localhost2").setPort(2).build());
                responseObserver.onCompleted();
            }
        };

        ControllerServiceImplBase fakeServerImpl3 = new ControllerServiceImplBase() {
            @Override
            public void getURI(SegmentId request, StreamObserver<NodeUri> responseObserver) {
                responseObserver.onNext(NodeUri.newBuilder().setEndpoint("localhost3").setPort(3).build());
                responseObserver.onCompleted();
            }
        };

        testRPCServer1 = InProcessServerBuilder.forPort(serverPort1).addService(fakeServerImpl1).build().start();
        testRPCServer2 = InProcessServerBuilder.forPort(serverPort2).addService(fakeServerImpl2).build().start();
        testRPCServer3 = InProcessServerBuilder.forPort(serverPort3).addService(fakeServerImpl3).build().start();
    }

    @After
    public void tearDown() throws IOException {
        testRPCServer1.shutdownNow();
        testRPCServer2.shutdownNow();
        testRPCServer3.shutdownNow();
    }

    @Test
    public void testDiscoverySuccess() throws Exception {
        final int serverPort1 = testRPCServer1.getPort();
        final int serverPort2 = testRPCServer2.getPort();

        // Use 2 servers to discover all the servers.
        ControllerImpl controllerClient = new ControllerImpl(
                URI.create("pravega://localhost:" + serverPort1 + ",localhost:" + serverPort2));
        final Set<PravegaNodeUri> uris = fetchFromServers(controllerClient);

        // Verify we could reach all 3 controllers.
        Assert.assertEquals(3, uris.size());
    }

    @Test
    public void testDiscoveryFailover() throws Exception {
        final int serverPort1 = testRPCServer1.getPort();
        final int serverPort2 = testRPCServer2.getPort();

        // Use 2 servers for discovery. Bring down the first server and ensure discovery happens using the other one.
        testRPCServer1.shutdownNow();
        testRPCServer1.awaitTermination();
        Assert.assertTrue(testRPCServer1.isTerminated());
        ControllerImpl controllerClient = new ControllerImpl(
                URI.create("pravega://localhost:" + serverPort1 + ",localhost:" + serverPort2));

        // Verify that we can read from the 2 live servers.
        Set<PravegaNodeUri> uris = fetchFromServers(controllerClient);
        Assert.assertEquals(2, uris.size());
        Assert.assertFalse(uris.contains(new PravegaNodeUri("localhost1", 1)));

        // Bring down another one and verify.
        testRPCServer2.shutdownNow();
        testRPCServer2.awaitTermination();
        Assert.assertTrue(testRPCServer2.isTerminated());
        uris = fetchFromServers(controllerClient);
        Assert.assertEquals(1, uris.size());
        Assert.assertTrue(uris.contains(new PravegaNodeUri("localhost3", 3)));

        // Bring down all and verify.
        testRPCServer3.shutdownNow();
        testRPCServer3.awaitTermination();
        Assert.assertTrue(testRPCServer3.isTerminated());
        controllerClient = new ControllerImpl(
                URI.create("pravega://localhost:" + serverPort1 + ",localhost:" + serverPort2));
        uris = fetchFromServers(controllerClient);
        Assert.assertEquals(0, uris.size());
    }

    @Test
    public void testDirectSuccess() throws Exception {
        final int serverPort1 = testRPCServer1.getPort();
        final int serverPort2 = testRPCServer2.getPort();
        final int serverPort3 = testRPCServer3.getPort();

        // Directly use all 3 servers and verify.
        ControllerImpl controllerClient = new ControllerImpl(URI.create(
                "tcp://localhost:" + serverPort1 + ",localhost:" + serverPort2 + ",localhost:" + serverPort3));
        final Set<PravegaNodeUri> uris = fetchFromServers(controllerClient);
        Assert.assertEquals(3, uris.size());
    }

    @Test
    public void testDirectFailover() throws Exception {
        final int serverPort1 = testRPCServer1.getPort();
        final int serverPort2 = testRPCServer2.getPort();
        final int serverPort3 = testRPCServer3.getPort();

        // Bring down the first server and verify we can fallback to the remaining 2 servers.
        testRPCServer1.shutdownNow();
        testRPCServer1.awaitTermination();
        Assert.assertTrue(testRPCServer1.isTerminated());
        ControllerImpl controllerClient = new ControllerImpl(URI.create(
                "tcp://localhost:" + serverPort1 + ",localhost:" + serverPort2 + ",localhost:" + serverPort3));
        Set<PravegaNodeUri> uris = fetchFromServers(controllerClient);
        Assert.assertEquals(2, uris.size());
        Assert.assertFalse(uris.contains(new PravegaNodeUri("localhost1", 1)));

        // Bring down another one and verify.
        testRPCServer2.shutdownNow();
        testRPCServer2.awaitTermination();
        Assert.assertTrue(testRPCServer2.isTerminated());
        uris = fetchFromServers(controllerClient);
        Assert.assertEquals(1, uris.size());
        Assert.assertTrue(uris.contains(new PravegaNodeUri("localhost3", 3)));

        // Bring down all and verify.
        testRPCServer3.shutdownNow();
        testRPCServer3.awaitTermination();
        Assert.assertTrue(testRPCServer3.isTerminated());
        uris = fetchFromServers(controllerClient);
        Assert.assertEquals(0, uris.size());
    }

    private Set<PravegaNodeUri> fetchFromServers(ControllerImpl client) {
        Set<PravegaNodeUri> uris = new HashSet<>();

        // Reading multiple times to ensure round robin policy gets a chance to read from all available servers.
        // Reading more than the number of servers since on failover request might fail intermittently due to
        // client-server connection timing issues.
        for (int i = 0; i < 6; i++) {
            try {
                uris.add(client.getEndpointForSegment("a/b/0").get());
            } catch (Exception e) {
                // Ignore temporary exceptions which happens due to failover.
            }
        }
        return uris;
    }
}
