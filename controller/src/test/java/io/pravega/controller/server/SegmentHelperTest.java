/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.server;

import io.pravega.auth.AuthenticationException;
import io.pravega.client.netty.impl.ClientConnection;
import io.pravega.client.netty.impl.ConnectionFactory;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.common.cluster.Host;
import io.pravega.controller.store.host.HostControllerStore;
import io.pravega.controller.stream.api.grpc.v1.Controller;
import io.pravega.shared.protocol.netty.Append;
import io.pravega.shared.protocol.netty.ConnectionFailedException;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.shared.protocol.netty.ReplyProcessor;
import io.pravega.shared.protocol.netty.WireCommand;
import io.pravega.shared.protocol.netty.WireCommands;
import io.pravega.test.common.AssertExtensions;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import lombok.Getter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SegmentHelperTest {

    private SegmentHelper helper;

    @Before
    public void setUp() throws Exception {
        helper = new SegmentHelper();
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void getSegmentUri() {
        helper.getSegmentUri("", "", 0, new MockHostControllerStore());
    }

    @Test
    public void createSegment() {
        MockConnectionFactory factory = new MockConnectionFactory();
        CompletableFuture<Boolean> retVal = helper.createSegment("", "",
                0, ScalingPolicy.fixed(2), new MockHostControllerStore(), factory, "", Long.MIN_VALUE);
        factory.rp.authTokenCheckFailed(new WireCommands.AuthTokenCheckFailed(0, "SomeException"));
        AssertExtensions.assertThrows("",
                () -> retVal.join(),
                ex -> ex instanceof WireCommandFailedException
                 && ex.getCause() instanceof AuthenticationException
                );
    }

    @Test
    public void truncateSegment() {
        MockConnectionFactory factory = new MockConnectionFactory();
        CompletableFuture<Boolean> retVal = helper.truncateSegment("", "", 0L, 0L,
                new MockHostControllerStore(), factory, "", System.nanoTime());
        factory.rp.authTokenCheckFailed(new WireCommands.AuthTokenCheckFailed(0, "SomeException"));
        AssertExtensions.assertThrows("",
                () -> retVal.join(),
                ex -> ex instanceof WireCommandFailedException
                        && ex.getCause() instanceof AuthenticationException
        );
    }


    @Test
    public void deleteSegment() {
        MockConnectionFactory factory = new MockConnectionFactory();
        CompletableFuture<Boolean> retVal = helper.deleteSegment("", "", 0L, new MockHostControllerStore(),
                factory, "", System.nanoTime());
        factory.rp.authTokenCheckFailed(new WireCommands.AuthTokenCheckFailed(0, "SomeException"));
        AssertExtensions.assertThrows("",
                () -> retVal.join(),
                ex -> ex instanceof WireCommandFailedException
                        && ex.getCause() instanceof AuthenticationException
        );
    }

    @Test
    public void sealSegment() {
        MockConnectionFactory factory = new MockConnectionFactory();
        CompletableFuture<Boolean> retVal = helper.sealSegment("", "", 0L,
                new MockHostControllerStore(), factory, "", System.nanoTime());
        factory.rp.authTokenCheckFailed(new WireCommands.AuthTokenCheckFailed(0, "SomeException"));
        AssertExtensions.assertThrows("",
                () -> retVal.join(),
                ex -> ex instanceof WireCommandFailedException
                        && ex.getCause() instanceof AuthenticationException
        );
    }

    @Test
    public void createTransaction() {
        MockConnectionFactory factory = new MockConnectionFactory();
        CompletableFuture<UUID> retVal = helper.createTransaction("", "", 0L, new UUID(0, 0L),
                new MockHostControllerStore(), factory, "");
        factory.rp.authTokenCheckFailed(new WireCommands.AuthTokenCheckFailed(0, "SomeException"));
        AssertExtensions.assertThrows("",
                () -> retVal.join(),
                ex -> ex instanceof WireCommandFailedException
                        && ex.getCause() instanceof AuthenticationException
        );
    }

    @Test
    public void commitTransaction() {
        MockConnectionFactory factory = new MockConnectionFactory();
        CompletableFuture<Controller.TxnStatus> retVal = helper.commitTransaction("", "", 0L, 0L, new UUID(0, 0L),
                new MockHostControllerStore(), factory, "", Long.MIN_VALUE);
        factory.rp.authTokenCheckFailed(new WireCommands.AuthTokenCheckFailed(0, "SomeException"));
        AssertExtensions.assertThrows("",
                () -> retVal.join(),
                ex -> ex instanceof WireCommandFailedException
                        && ex.getCause() instanceof AuthenticationException
        );
    }

    @Test
    public void abortTransaction() {
        MockConnectionFactory factory = new MockConnectionFactory();
        CompletableFuture<Controller.TxnStatus> retVal = helper.abortTransaction("", "", 0L, new UUID(0, 0L),
                new MockHostControllerStore(), factory, "", Long.MIN_VALUE);
        factory.rp.authTokenCheckFailed(new WireCommands.AuthTokenCheckFailed(0, "SomeException"));
        AssertExtensions.assertThrows("",
                () -> retVal.join(),
                ex -> ex instanceof WireCommandFailedException
                        && ex.getCause() instanceof AuthenticationException
        );
    }

    @Test
    public void updatePolicy() {
        MockConnectionFactory factory = new MockConnectionFactory();
        CompletableFuture<Void> retVal = helper.updatePolicy("", "", ScalingPolicy.fixed(1), 0L,
                new MockHostControllerStore(), factory, "", System.nanoTime());
        factory.rp.authTokenCheckFailed(new WireCommands.AuthTokenCheckFailed(0, "SomeException"));
        AssertExtensions.assertThrows("",
                () -> retVal.join(),
                ex -> ex instanceof WireCommandFailedException
                        && ex.getCause() instanceof AuthenticationException
        );
    }

    @Test
    public void getSegmentInfo() {
        MockConnectionFactory factory = new MockConnectionFactory();
        CompletableFuture<WireCommands.StreamSegmentInfo> retVal = helper.getSegmentInfo("", "", 0L,
                new MockHostControllerStore(), factory, "");
        factory.rp.authTokenCheckFailed(new WireCommands.AuthTokenCheckFailed(0, "SomeException"));
        AssertExtensions.assertThrows("",
                () -> retVal.join(),
                ex -> ex instanceof WireCommandFailedException
                        && ex.getCause() instanceof AuthenticationException
        );
    }

    private static class MockHostControllerStore implements HostControllerStore {

        @Override
        public Map<Host, Set<Integer>> getHostContainersMap() {
            return null;
        }

        @Override
        public void updateHostContainersMap(Map<Host, Set<Integer>> newMapping) {

        }

        @Override
        public int getContainerCount() {
            return 0;
        }

        @Override
        public Host getHostForSegment(String scope, String stream, long segmentId) {
            return new Host("localhost", 1000, "");
        }
    }

    private class MockConnectionFactory implements ConnectionFactory {
        @Getter
        private ReplyProcessor rp;

        @Override
        public CompletableFuture<ClientConnection> establishConnection(PravegaNodeUri endpoint, ReplyProcessor rp) {
            this.rp = rp;
            ClientConnection connection = new MockConnection(rp);
            return CompletableFuture.completedFuture(connection);
        }

        @Override
        public ScheduledExecutorService getInternalExecutor() {
            return null;
        }

        @Override
        public void close() {

        }
    }

    private class MockConnection implements ClientConnection {
        @Getter
        private final ReplyProcessor rp;

        public MockConnection(ReplyProcessor rp) {
            this.rp = rp;
        }

        @Override
        public void send(WireCommand cmd) throws ConnectionFailedException {

        }

        @Override
        public void send(Append append) throws ConnectionFailedException {

        }

        @Override
        public void sendAsync(WireCommand cmd) throws ConnectionFailedException {

        }

        @Override
        public void sendAsync(List<Append> appends, CompletedCallback callback) {

        }

        @Override
        public void close() {

        }
    }
}