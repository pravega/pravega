/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.stream.impl;

import com.emc.pravega.common.netty.PravegaNodeUri;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.NodeUri;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.SegmentId;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.ServerRequest;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.ServerResponse;
import com.emc.pravega.controller.stream.api.grpc.v1.ControllerServiceGrpc.ControllerServiceImplBase;
import com.emc.pravega.shared.TestUtils;
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
    private static final int SERVER_PORT1 = TestUtils.getAvailableListenPort();
    private static final int SERVER_PORT2 = TestUtils.getAvailableListenPort();
    private static final int SERVER_PORT3 = TestUtils.getAvailableListenPort();

    @Rule
    public final Timeout globalTimeout = new Timeout(10, TimeUnit.SECONDS);

    private Server fakeServer1 = null;
    private Server fakeServer2 = null;
    private Server fakeServer3 = null;

    @Before
    public void setup() throws IOException {

        // Setup fake servers for simulating multiple controllers with discovery info.
        ControllerServiceImplBase fakeServerImpl1 = new ControllerServiceImplBase() {
            @Override
            public void getControllerServerList(ServerRequest request, StreamObserver<ServerResponse> responseObserver) {
                responseObserver.onNext(ServerResponse.newBuilder()
                        .addNodeURI(NodeUri.newBuilder().setEndpoint("localhost").setPort(SERVER_PORT1).build())
                        .addNodeURI(NodeUri.newBuilder().setEndpoint("localhost").setPort(SERVER_PORT2).build())
                        .addNodeURI(NodeUri.newBuilder().setEndpoint("localhost").setPort(SERVER_PORT3).build())
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
                        .addNodeURI(NodeUri.newBuilder().setEndpoint("localhost").setPort(SERVER_PORT1).build())
                        .addNodeURI(NodeUri.newBuilder().setEndpoint("localhost").setPort(SERVER_PORT2).build())
                        .addNodeURI(NodeUri.newBuilder().setEndpoint("localhost").setPort(SERVER_PORT3).build())
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

        fakeServer1 = InProcessServerBuilder.forPort(SERVER_PORT1).addService(fakeServerImpl1).build().start();
        fakeServer2 = InProcessServerBuilder.forPort(SERVER_PORT2).addService(fakeServerImpl2).build().start();
        fakeServer3 = InProcessServerBuilder.forPort(SERVER_PORT3).addService(fakeServerImpl3).build().start();
    }

    @After
    public void tearDown() throws IOException {
        fakeServer1.shutdownNow();
        fakeServer2.shutdownNow();
        fakeServer3.shutdownNow();
    }

    @Test
    public void testDiscoverySuccess() throws Exception {

        // Use 2 servers to discover all the servers.
        ControllerImpl controllerClient = new ControllerImpl(
                URI.create("pravega://localhost:" + SERVER_PORT1 + ",localhost:" + SERVER_PORT2));
        final Set<PravegaNodeUri> uris = fetchFromServers(controllerClient);

        // Verify we could reach all 3 controllers.
        Assert.assertEquals(3, uris.size());
    }

    @Test
    public void testDiscoveryFailover() throws Exception {

        // Use 2 servers for discovery. Bring down the first server and ensure discovery happens using the other one.
        fakeServer1.shutdownNow();
        Assert.assertTrue(fakeServer1.isShutdown());
        ControllerImpl controllerClient = new ControllerImpl(
                URI.create("pravega://localhost:" + SERVER_PORT1 + ",localhost:" + SERVER_PORT2));

        // Verify that we can read from the 2 live servers.
        Set<PravegaNodeUri> uris = fetchFromServers(controllerClient);
        Assert.assertEquals(2, uris.size());
        Assert.assertFalse(uris.contains(new PravegaNodeUri("localhost1", 1)));

        // Bring down another one and verify.
        fakeServer2.shutdownNow();
        Assert.assertTrue(fakeServer2.isShutdown());
        uris = fetchFromServers(controllerClient);
        Assert.assertEquals(1, uris.size());
        Assert.assertTrue(uris.contains(new PravegaNodeUri("localhost3", 3)));
    }

    @Test
    public void testDirectSuccess() throws Exception {

        // Directly use all 3 servers and verify.
        ControllerImpl controllerClient = new ControllerImpl(URI.create(
                "tcp://localhost:" + SERVER_PORT1 + ",localhost:" + SERVER_PORT2 + ",localhost:" + SERVER_PORT3));
        final Set<PravegaNodeUri> uris = fetchFromServers(controllerClient);
        Assert.assertEquals(3, uris.size());
    }

    @Test
    public void testDirectFailover() throws Exception {

        // Bring down the first server and verify we can fallback to the remaining 2 servers.
        fakeServer1.shutdownNow();
        Assert.assertTrue(fakeServer1.isShutdown());
        ControllerImpl controllerClient = new ControllerImpl(URI.create(
                "tcp://localhost:" + SERVER_PORT1 + ",localhost:" + SERVER_PORT2 + ",localhost:" + SERVER_PORT3));
        Set<PravegaNodeUri> uris = fetchFromServers(controllerClient);
        Assert.assertEquals(2, uris.size());
        Assert.assertFalse(uris.contains(new PravegaNodeUri("localhost1", 1)));

        // Bring down another one and verify.
        fakeServer2.shutdownNow();
        Assert.assertTrue(fakeServer2.isShutdown());
        uris = fetchFromServers(controllerClient);
        Assert.assertEquals(1, uris.size());
        Assert.assertTrue(uris.contains(new PravegaNodeUri("localhost3", 3)));
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
