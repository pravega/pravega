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
import io.pravega.client.tables.impl.KeyVersion;
import io.pravega.client.tables.impl.KeyVersionImpl;
import io.pravega.client.tables.impl.TableEntry;
import io.pravega.client.tables.impl.TableEntryImpl;
import io.pravega.client.tables.impl.TableKey;
import io.pravega.client.tables.impl.TableKeyImpl;
import io.pravega.common.cluster.Host;
import io.pravega.common.concurrent.Futures;
import io.pravega.controller.store.host.HostControllerStore;
import io.pravega.controller.stream.api.grpc.v1.Controller;
import io.pravega.shared.protocol.netty.Append;
import io.pravega.shared.protocol.netty.ConnectionFailedException;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.shared.protocol.netty.ReplyProcessor;
import io.pravega.shared.protocol.netty.WireCommand;
import io.pravega.shared.protocol.netty.WireCommands;
import io.pravega.shared.segment.ScalingPolicy;
import io.pravega.test.common.AssertExtensions;
import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.val;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static io.pravega.shared.segment.StreamSegmentNameUtils.getQualifiedStreamSegmentName;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
                new MockHostControllerStore(), factory, "");
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
                new MockHostControllerStore(), factory, "");
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

    @Test
    public void testCreateTableSegment() {
        MockConnectionFactory factory = new MockConnectionFactory();

        // On receiving SegmentAlreadyExists true should be returned.
        CompletableFuture<Boolean> result = helper.createTableSegment("", "", new MockHostControllerStore(), factory, "", Long.MIN_VALUE);
        factory.rp.segmentAlreadyExists(new WireCommands.SegmentAlreadyExists(0, getQualifiedStreamSegmentName("", "", 0L), ""));
        assertTrue(result.join());

        // On Receiving SegmentCreated true should be returned.
        result = helper.createTableSegment("", "", new MockHostControllerStore(), factory, "", Long.MIN_VALUE);
        factory.rp.segmentCreated(new WireCommands.SegmentCreated(0, getQualifiedStreamSegmentName("", "", 0L)));
        assertTrue(result.join());

        // Validate failure conditions.
        Supplier<CompletableFuture<?>> futureSupplier = () -> helper.createTableSegment("", "", new MockHostControllerStore(), factory, "", Long.MIN_VALUE);
        validateAuthTokenCheckFailed(factory, futureSupplier);
        validateWrongHost(factory, futureSupplier);
        validateConnectionDropped(factory, futureSupplier);
        validateProcessingFailure(factory, futureSupplier);

    }

    @Test
    public void testDeleteTableSegment() {
        MockConnectionFactory factory = new MockConnectionFactory();
        // On receiving NoSuchSegment true should be returned.
        CompletableFuture<Boolean> result = helper.deleteTableSegment("", "", true, new MockHostControllerStore(),
                                                                      factory, "", System.nanoTime());
        factory.rp.noSuchSegment(new WireCommands.NoSuchSegment(0, getQualifiedStreamSegmentName("", "", 0L), ""));
        assertTrue(result.join());

        // On receiving SegmentDeleted true should be returned.
        result = helper.deleteTableSegment("", "", true, new MockHostControllerStore(),
                                                                      factory, "", System.nanoTime());
        factory.rp.segmentDeleted(new WireCommands.SegmentDeleted(0, getQualifiedStreamSegmentName("", "", 0L)));
        assertTrue(result.join());

        // On receiving TableSegmentNotEmpty WireCommandFailedException is thrown.
        result = helper.deleteTableSegment("", "", true, new MockHostControllerStore(),
                                           factory, "", System.nanoTime());
        factory.rp.tableSegmentNotEmpty(new WireCommands.TableSegmentNotEmpty(0, getQualifiedStreamSegmentName("", "", 0L), ""));
        AssertExtensions.assertThrows("", result::join,
                                      ex -> ex instanceof WireCommandFailedException &&
                                              (((WireCommandFailedException) ex).getReason() == WireCommandFailedException.Reason.TableSegmentNotEmpty));

        Supplier<CompletableFuture<?>> futureSupplier = () -> helper.deleteTableSegment("", "", true, new MockHostControllerStore(), factory, "", System.nanoTime());
        validateAuthTokenCheckFailed(factory, futureSupplier);
        validateWrongHost(factory, futureSupplier);
        validateConnectionDropped(factory, futureSupplier);
        validateProcessingFailure(factory, futureSupplier);

    }

    @Test
    public void testUpdateTableEntries() {
        MockConnectionFactory factory = new MockConnectionFactory();
        List<TableEntry<byte[], byte[]>> entries = Arrays.asList(new TableEntryImpl<>(new TableKeyImpl<>("k".getBytes(), KeyVersion.NOT_EXISTS), "v".getBytes()),
                                                                 new TableEntryImpl<>(new TableKeyImpl<>("k1".getBytes(), null), "v".getBytes()),
                                                                 new TableEntryImpl<>(new TableKeyImpl<>("k2".getBytes(), new KeyVersionImpl(10L)),
                                                                                      "v".getBytes()));

        List<KeyVersion> expectedVersions = Arrays.asList(new KeyVersionImpl(0L),
                                                          new KeyVersionImpl(1L),
                                                          new KeyVersionImpl(11L));

        // On receiving TableEntriesUpdated.
        CompletableFuture<List<KeyVersion>> result = helper.updateTableEntries("", "", entries, new MockHostControllerStore(), factory, "", System.nanoTime());
        factory.rp.tableEntriesUpdated(new WireCommands.TableEntriesUpdated(0, Arrays.asList(0L, 1L, 11L)));
        assertEquals(expectedVersions, result.join());

        // On receiving TableKeyDoesNotExist.
        result = helper.updateTableEntries("", "", entries, new MockHostControllerStore(), factory, "", System.nanoTime());
        factory.rp.tableKeyDoesNotExist(new WireCommands.TableKeyDoesNotExist(0, getQualifiedStreamSegmentName("", "", 0L), ""));
        AssertExtensions.assertThrows("", result::join,
                                      ex -> ex instanceof WireCommandFailedException &&
                                              (((WireCommandFailedException) ex).getReason() == WireCommandFailedException.Reason.TableKeyDoesNotExist));

        // On receiving TableKeyBadVersion.
        result = helper.updateTableEntries("", "", entries, new MockHostControllerStore(), factory, "", System.nanoTime());
        factory.rp.tableKeyBadVersion(new WireCommands.TableKeyBadVersion(0, getQualifiedStreamSegmentName("", "", 0L), ""));
        AssertExtensions.assertThrows("", result::join,
                                      ex -> ex instanceof WireCommandFailedException &&
                                              (((WireCommandFailedException) ex).getReason() == WireCommandFailedException.Reason.TableKeyBadVersion));

        Supplier<CompletableFuture<?>> futureSupplier = () -> helper.updateTableEntries("", "", entries, new MockHostControllerStore(), factory, "", System.nanoTime());
        validateAuthTokenCheckFailed(factory, futureSupplier);
        validateWrongHost(factory, futureSupplier);
        validateConnectionDropped(factory, futureSupplier);
        validateProcessingFailure(factory, futureSupplier);
        validateNoSuchSegment(factory, futureSupplier);
    }

    @Test
    public void testRemoveTableKeys() {
        MockConnectionFactory factory = new MockConnectionFactory();
        List<TableKey<byte[]>> keys = Arrays.asList(new TableKeyImpl<>("k".getBytes(), KeyVersion.NOT_EXISTS),
                                                    new TableKeyImpl<>("k1".getBytes(), KeyVersion.NOT_EXISTS));

        // On receiving TableKeysRemoved.
        CompletableFuture<Void> result = helper.removeTableKeys("", "", keys, new MockHostControllerStore(), factory, "",
                                                                            System.nanoTime());
        factory.rp.tableKeysRemoved(new WireCommands.TableKeysRemoved(0, getQualifiedStreamSegmentName("", "", 0L) ));
        assertTrue(Futures.await(result));

        // On receiving TableKeyDoesNotExist.
        result = helper.removeTableKeys("", "", keys, new MockHostControllerStore(), factory, "", System.nanoTime());
        factory.rp.tableKeyDoesNotExist(new WireCommands.TableKeyDoesNotExist(0, getQualifiedStreamSegmentName("", "", 0L), ""));
        assertTrue(Futures.await(result));

        // On receiving TableKeyBadVersion.
        result = helper.removeTableKeys("", "", keys, new MockHostControllerStore(), factory, "", System.nanoTime());
        factory.rp.tableKeyBadVersion(new WireCommands.TableKeyBadVersion(0, getQualifiedStreamSegmentName("", "", 0L), ""));
        AssertExtensions.assertThrows("", result::join,
                                      ex -> ex instanceof WireCommandFailedException &&
                                              (((WireCommandFailedException) ex).getReason() == WireCommandFailedException.Reason.TableKeyBadVersion));

        Supplier<CompletableFuture<?>> futureSupplier = () -> helper.removeTableKeys("", "", keys, new MockHostControllerStore(),
                                                                                        factory, "", System.nanoTime());
        validateAuthTokenCheckFailed(factory, futureSupplier);
        validateWrongHost(factory, futureSupplier);
        validateConnectionDropped(factory, futureSupplier);
        validateProcessingFailure(factory, futureSupplier);
        validateNoSuchSegment(factory, futureSupplier);
    }

    @Test
    public void testReadTable() {
        MockConnectionFactory factory = new MockConnectionFactory();
        List<TableKey<byte[]>> keys = Arrays.asList(new TableKeyImpl<>("k".getBytes(), KeyVersion.NOT_EXISTS),
                                                    new TableKeyImpl<>("k1".getBytes(), KeyVersion.NOT_EXISTS));

        List<TableEntry<byte[], byte[]>> entries = Arrays.asList(new TableEntryImpl<>(new TableKeyImpl<>("k".getBytes(), new KeyVersionImpl(10L)), "v".getBytes()),
                                                                 new TableEntryImpl<>(new TableKeyImpl<>("k1".getBytes(), new KeyVersionImpl(10L)), "v".getBytes()));

        WireCommands.TableEntries resultData = new WireCommands.TableEntries(entries.stream().map(e -> {
            val k = new WireCommands.TableKey(ByteBuffer.wrap(e.getKey().getKey()), e.getKey().getVersion().getSegmentVersion());
            val v = new WireCommands.TableValue(ByteBuffer.wrap(e.getValue()));
            return new AbstractMap.SimpleImmutableEntry<>(k, v);
        }).collect(Collectors.toList()));

        // On receiving TableKeysRemoved.
        CompletableFuture<List<TableEntry<byte[], byte[]>>> result = helper.readTable("", "", keys, new MockHostControllerStore(),
                                                                                      factory, "", System.nanoTime());
        factory.rp.tableRead(new WireCommands.TableRead(0, getQualifiedStreamSegmentName("", "", 0L), resultData));
        assertEquals(entries, result.join());

        // On receiving TableKeyDoesNotExist.
        result = helper.readTable("", "", keys, new MockHostControllerStore(), factory, "", System.nanoTime());
        factory.rp.tableKeyDoesNotExist(new WireCommands.TableKeyDoesNotExist(0, getQualifiedStreamSegmentName("", "", 0L), ""));
        AssertExtensions.assertThrows("", result::join,
                                      ex -> ex instanceof WireCommandFailedException &&
                                              (((WireCommandFailedException) ex).getReason() == WireCommandFailedException.Reason.TableKeyDoesNotExist));

        Supplier<CompletableFuture<?>> futureSupplier = () -> helper.readTable("", "", keys, new MockHostControllerStore(),
                                                                               factory, "", System.nanoTime());
        validateAuthTokenCheckFailed(factory, futureSupplier);
        validateWrongHost(factory, futureSupplier);
        validateConnectionDropped(factory, futureSupplier);
        validateProcessingFailure(factory, futureSupplier);
        validateNoSuchSegment(factory, futureSupplier);
    }

    private void validateAuthTokenCheckFailed(MockConnectionFactory factory, Supplier<CompletableFuture<?>> futureSupplier) {
        CompletableFuture<?> future = futureSupplier.get();
        factory.rp.authTokenCheckFailed(new WireCommands.AuthTokenCheckFailed(0, "SomeException"));
        AssertExtensions.assertThrows("", future::join,
                                      ex -> ex instanceof WireCommandFailedException && ex.getCause() instanceof AuthenticationException);
    }

    private void validateNoSuchSegment(MockConnectionFactory factory, Supplier<CompletableFuture<?>> futureSupplier) {
        CompletableFuture<?> future = futureSupplier.get();
        factory.rp.noSuchSegment(new WireCommands.NoSuchSegment(0, "segment", "SomeException"));
        AssertExtensions.assertThrows("", future::join,
                                      ex -> ex instanceof WireCommandFailedException &&
                                              (((WireCommandFailedException) ex).getReason() == WireCommandFailedException.Reason.SegmentDoesNotExist));
    }

    private void validateWrongHost(MockConnectionFactory factory, Supplier<CompletableFuture<?>> futureSupplier) {
        CompletableFuture<?> future = futureSupplier.get();
        factory.rp.wrongHost(new WireCommands.WrongHost(0, "segment", "correctHost", "SomeException"));
        AssertExtensions.assertThrows("", future::join,
                                      ex -> ex instanceof WireCommandFailedException &&
                                              (((WireCommandFailedException) ex).getReason() == WireCommandFailedException.Reason.UnknownHost));
    }

    private void validateConnectionDropped(MockConnectionFactory factory, Supplier<CompletableFuture<?>> futureSupplier) {
        CompletableFuture<?> future = futureSupplier.get();
        factory.rp.connectionDropped();
        AssertExtensions.assertThrows("", future::join,
                                      ex -> ex instanceof WireCommandFailedException &&
                                              (((WireCommandFailedException) ex).getReason() == WireCommandFailedException.Reason.ConnectionDropped));
    }

    private void validateProcessingFailure(MockConnectionFactory factory, Supplier<CompletableFuture<?>> futureSupplier) {
        CompletableFuture<?> future = futureSupplier.get();
        factory.rp.processingFailure(new RuntimeException());
        AssertExtensions.assertThrows("", future::join, ex -> ex instanceof RuntimeException);
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
        public void sendAsync(WireCommand cmd, CompletedCallback callback) {

        }

        @Override
        public void sendAsync(List<Append> appends, CompletedCallback callback) {

        }

        @Override
        public void close() {

        }
    }
}