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
package io.pravega.client.control.impl;

import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import io.pravega.client.ClientConfig;
import io.pravega.client.segment.impl.Segment;
import io.pravega.common.Exceptions;
import io.pravega.common.util.RetriesExhaustedException;
import io.pravega.controller.stream.api.grpc.v1.Controller.NodeUri;
import io.pravega.controller.stream.api.grpc.v1.Controller.SegmentId;
import io.pravega.controller.stream.api.grpc.v1.Controller.ServerRequest;
import io.pravega.controller.stream.api.grpc.v1.Controller.ServerResponse;
import io.pravega.controller.stream.api.grpc.v1.ControllerServiceGrpc.ControllerServiceImplBase;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.InlineExecutor;
import io.pravega.test.common.TestUtils;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.mockito.Mockito;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;


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

        testRPCServer1 = NettyServerBuilder.forPort(serverPort1).addService(fakeServerImpl1).build().start();
        testRPCServer2 = NettyServerBuilder.forPort(serverPort2).addService(fakeServerImpl2).build().start();
        testRPCServer3 = NettyServerBuilder.forPort(serverPort3).addService(fakeServerImpl3).build().start();
    }

    @After
    public void tearDown() {
        testRPCServer1.shutdownNow();
        testRPCServer2.shutdownNow();
        testRPCServer3.shutdownNow();
    }

    @Test
    public void testDiscoverySuccess() throws Exception {
        final int serverPort1 = testRPCServer1.getPort();
        final int serverPort2 = testRPCServer2.getPort();

        // Use 2 servers to discover all the servers.
        @Cleanup("shutdown")
        InlineExecutor executor = new InlineExecutor();
        final ControllerImpl controllerClient = new ControllerImpl(
                ControllerImplConfig.builder()
                                    .clientConfig(ClientConfig.builder().controllerURI(URI.create("pravega://localhost:" + serverPort1 + ",localhost:" + serverPort2)).build())
                                    .retryAttempts(1).build(), executor);
        final Set<PravegaNodeUri> uris = fetchFromServers(controllerClient, 3);

        // Verify we could reach all 3 controllers.
        Assert.assertEquals(3, uris.size());
    }

    @Test
    public void testDiscoverySuccessUsingIPAddress() throws Exception {
        final int serverPort1 = testRPCServer1.getPort();
        final int serverPort2 = testRPCServer2.getPort();

        // Use 2 servers to discover all the servers.
        String localIP = InetAddress.getLoopbackAddress().getHostAddress();
        @Cleanup("shutdown")
        InlineExecutor executor = new InlineExecutor();
        final ControllerImpl controllerClient = new ControllerImpl(
                ControllerImplConfig.builder()
                                    .clientConfig(ClientConfig.builder().controllerURI(URI.create("pravega://" + localIP + ":" + serverPort1 + "," + localIP + ":" + serverPort2)).build())
                                    .retryAttempts(1).build(), executor);
        final Set<PravegaNodeUri> uris = fetchFromServers(controllerClient, 3);

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
        @Cleanup("shutdown")
        InlineExecutor executor = new InlineExecutor();
        final ControllerImpl controllerClient = new ControllerImpl( ControllerImplConfig.builder()
                .clientConfig(ClientConfig.builder().controllerURI(URI.create("pravega://localhost:" + serverPort1 + ",localhost:" + serverPort2)).build())
                .retryAttempts(1).build(),
                executor);

        // Verify that we can read from the 2 live servers.
        Set<PravegaNodeUri> uris = fetchFromServers(controllerClient, 2);
        Assert.assertEquals(2, uris.size());
        Assert.assertFalse(uris.contains(new PravegaNodeUri("localhost1", 1)));

        // Verify no RPC requests fail due to the failed servers.
        Assert.assertTrue(verifyNoFailures(controllerClient));

        // Bring down another one and verify.
        testRPCServer2.shutdownNow();
        testRPCServer2.awaitTermination();
        Assert.assertTrue(testRPCServer2.isTerminated());
        uris = fetchFromServers(controllerClient, 1);
        Assert.assertEquals(1, uris.size());
        Assert.assertTrue(uris.contains(new PravegaNodeUri("localhost3", 3)));

        // Verify no RPC requests fail due to the failed servers.
        Assert.assertTrue(verifyNoFailures(controllerClient));

        // Bring down all and verify.
        testRPCServer3.shutdownNow();
        testRPCServer3.awaitTermination();
        Assert.assertTrue(testRPCServer3.isTerminated());
        @Cleanup
        final ControllerImpl client = new ControllerImpl( ControllerImplConfig.builder()
                .clientConfig(ClientConfig.builder().controllerURI(URI.create("pravega://localhost:" + serverPort1 + ",localhost:" + serverPort2)).build())
                .retryAttempts(1).build(), executor);
        AssertExtensions.assertThrows(RetriesExhaustedException.class, () -> client.getEndpointForSegment("a/b/0").get());
    }

    @Test
    public void testDirectSuccess() throws Exception {
        final int serverPort1 = testRPCServer1.getPort();
        final int serverPort2 = testRPCServer2.getPort();
        final int serverPort3 = testRPCServer3.getPort();

        // Directly use all 3 servers and verify.
        @Cleanup("shutdown")
        InlineExecutor executor = new InlineExecutor();
        final ControllerImpl controllerClient = new ControllerImpl( ControllerImplConfig.builder()
                .clientConfig(ClientConfig.builder().controllerURI(URI.create("tcp://localhost:" + serverPort1 + ",localhost:" + serverPort2 + ",localhost:" + serverPort3)).build())
                .retryAttempts(1).build(), executor);
        final Set<PravegaNodeUri> uris = fetchFromServers(controllerClient, 3);
        Assert.assertEquals(3, uris.size());
    }

    @Test
    public void testDirectSuccessUsingIPAddress() throws Exception {
        final int serverPort1 = testRPCServer1.getPort();
        final int serverPort2 = testRPCServer2.getPort();
        final int serverPort3 = testRPCServer3.getPort();

        // Directly use all 3 servers and verify.
        String localIP = InetAddress.getLoopbackAddress().getHostAddress();
        @Cleanup("shutdown")
        InlineExecutor executor = new InlineExecutor();
        final ControllerImpl controllerClient = new ControllerImpl( ControllerImplConfig.builder()
                .clientConfig(ClientConfig.builder().controllerURI(URI.create("tcp://" + localIP + ":" + serverPort1 + "," + localIP + ":" + serverPort2 + "," + localIP + ":" + serverPort3)).build())
                .retryAttempts(1).build(), executor);
        final Set<PravegaNodeUri> uris = fetchFromServers(controllerClient, 3);
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

        @Cleanup("shutdown")
        InlineExecutor executor = new InlineExecutor();
        final ControllerImpl controllerClient = new ControllerImpl( ControllerImplConfig.builder()
                .clientConfig(ClientConfig.builder().controllerURI(URI.create("tcp://localhost:" + serverPort1 + ",localhost:" + serverPort2 + ",localhost:" + serverPort3)).build())
                .retryAttempts(1).build(), executor);
        Set<PravegaNodeUri> uris = fetchFromServers(controllerClient, 2);
        Assert.assertEquals(2, uris.size());
        Assert.assertFalse(uris.contains(new PravegaNodeUri("localhost1", 1)));

        // Verify no RPC requests fail due to the failed servers.
        Assert.assertTrue(verifyNoFailures(controllerClient));

        // Bring down another one and verify.
        testRPCServer2.shutdownNow();
        testRPCServer2.awaitTermination();
        Assert.assertTrue(testRPCServer2.isTerminated());

        uris = fetchFromServers(controllerClient, 1);
        Assert.assertEquals(1, uris.size());
        Assert.assertTrue(uris.contains(new PravegaNodeUri("localhost3", 3)));

        // Verify no RPC requests fail due to the failed servers.
        Assert.assertTrue(verifyNoFailures(controllerClient));

        // Bring down all and verify.
        testRPCServer3.shutdownNow();
        testRPCServer3.awaitTermination();
        Assert.assertTrue(testRPCServer3.isTerminated());

        AssertExtensions.assertThrows(RetriesExhaustedException.class,
                () -> controllerClient.getPravegaNodeUri(Segment.fromScopedName("a/b/0")).get());
    }

    private Set<PravegaNodeUri> fetchFromServers(ControllerImpl client, int numServers) {
        Set<PravegaNodeUri> uris = new HashSet<>();

        // Reading multiple times to ensure round robin policy gets a chance to read from all available servers.
        // Reading more than the number of servers since on failover request might fail intermittently due to
        // client-server connection timing issues.
        while (uris.size() < numServers) {
            try {
                // We have added cache support in getEndPointForSegment API.
                // Since it will always return the same value from cache for ID 0, so the condition in while loop can never be met and will eventually timeout and fail.
                // hence we did change the call to getPravegaNodeUriForTesting.
                uris.add(client.getPravegaNodeUri(Segment.fromScopedName("a/b/0")).get());
            } catch (Exception e) {
                // Ignore temporary exceptions which happens due to failover.
            }
            // Adding a small delay to avoid busy cpu loop.
            Exceptions.handleInterrupted(() -> Thread.sleep(10));
        }
        return uris;
    }

    private boolean verifyNoFailures(ControllerImpl client) {
        for (int i = 0; i < 100; i++) {
            try {
                client.getEndpointForSegment("a/b/0").get();
            } catch (Exception e) {
                return false;
            }
        }
        return true;
    }

    @Test
    public void testDefaultUpdateStaleValueInCache() {
            Controller connection = Mockito.spy(Controller.class);
            connection.updateStaleValueInCache("dummySegment", new PravegaNodeUri("dummyhost", 12345));
            verify(connection, times(1)).updateStaleValueInCache(anyString(), any());
    }
}
