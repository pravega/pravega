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

import io.netty.buffer.ByteBuf;
import io.pravega.auth.AuthenticationException;
import io.pravega.client.netty.impl.ClientConnection;
import io.pravega.client.netty.impl.ConnectionFactory;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.tables.impl.IteratorState;
import io.pravega.client.tables.impl.IteratorStateImpl;
import io.pravega.client.tables.impl.KeyVersion;
import io.pravega.client.tables.impl.KeyVersionImpl;
import io.pravega.client.tables.impl.TableEntry;
import io.pravega.client.tables.impl.TableEntryImpl;
import io.pravega.client.tables.impl.TableKey;
import io.pravega.client.tables.impl.TableKeyImpl;
import io.pravega.client.tables.impl.TableSegment;
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
import io.pravega.test.common.AssertExtensions;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.val;
import org.junit.Test;

import static io.netty.buffer.Unpooled.wrappedBuffer;
import static io.pravega.common.Exceptions.unwrap;
import static io.pravega.controller.server.SegmentStoreConnectionManager.*;
import static io.pravega.controller.server.SegmentStoreConnectionManager.ConnectionListener.*;
import static io.pravega.shared.segment.StreamSegmentNameUtils.getQualifiedStreamSegmentName;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SegmentHelperTest {

    private final byte[] key0 = "k".getBytes();
    private final byte[] key1 = "k1".getBytes();
    private final byte[] key2 = "k2".getBytes();
    private final byte[] key3 = "k3".getBytes();
    private final byte[] value = "v".getBytes();
    private final ByteBuf token1 = wrappedBuffer(new byte[]{0x01});
    private final ByteBuf token2 = wrappedBuffer(new byte[]{0x02});

    @Test
    public void getSegmentUri() {
        MockConnectionFactory factory = new MockConnectionFactory();
                SegmentHelper helper = new SegmentHelper(factory);

        helper.getSegmentUri("", "", 0, new MockHostControllerStore());
    }

    @Test
    public void createSegment() {
        MockConnectionFactory factory = new MockConnectionFactory();
                SegmentHelper helper = new SegmentHelper(factory);
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
                SegmentHelper helper = new SegmentHelper(factory);
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
        SegmentHelper helper = new SegmentHelper(factory);
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
                SegmentHelper helper = new SegmentHelper(factory);
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
                SegmentHelper helper = new SegmentHelper(factory);
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
                SegmentHelper helper = new SegmentHelper(factory);
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
                SegmentHelper helper = new SegmentHelper(factory);
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
                SegmentHelper helper = new SegmentHelper(factory);
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
                SegmentHelper helper = new SegmentHelper(factory);
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
                SegmentHelper helper = new SegmentHelper(factory);

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
                SegmentHelper helper = new SegmentHelper(factory);
        // On receiving NoSuchSegment true should be returned.
        CompletableFuture<Boolean> result = helper.deleteTableSegment("", "", true, new MockHostControllerStore(),
                                                                      factory, "", System.nanoTime());
        factory.rp.noSuchSegment(new WireCommands.NoSuchSegment(0, getQualifiedStreamSegmentName("", "", 0L), "", -1L));
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
                SegmentHelper helper = new SegmentHelper(factory);
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
                SegmentHelper helper = new SegmentHelper(factory);
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
                SegmentHelper helper = new SegmentHelper(factory);
        List<TableKey<byte[]>> keys = Arrays.asList(new TableKeyImpl<>(key0, KeyVersion.NOT_EXISTS),
                                                    new TableKeyImpl<>(key1, KeyVersion.NOT_EXISTS));

        List<TableEntry<byte[], byte[]>> entries = Arrays.asList(new TableEntryImpl<>(new TableKeyImpl<>(key0, new KeyVersionImpl(10L)), value),
                                                                 new TableEntryImpl<>(new TableKeyImpl<>(key1, new KeyVersionImpl(10L)), value));

        // On receiving TableKeysRemoved.
        CompletableFuture<List<TableEntry<byte[], byte[]>>> result = helper.readTable("", "", keys, new MockHostControllerStore(),
                                                                                      factory, "", System.nanoTime());
        factory.rp.tableRead(new WireCommands.TableRead(0, getQualifiedStreamSegmentName("", "", 0L), getTableEntries(entries)));
        List<TableEntry<byte[], byte[]>> readResult = result.join();
        assertArrayEquals(key0, readResult.get(0).getKey().getKey());
        assertEquals(10L, readResult.get(0).getKey().getVersion().getSegmentVersion());
        assertArrayEquals(value, readResult.get(0).getValue());
        assertArrayEquals(key1, readResult.get(1).getKey().getKey());
        assertEquals(10L, readResult.get(1).getKey().getVersion().getSegmentVersion());
        assertArrayEquals(value, readResult.get(1).getValue());

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

    @Test
    public void testReadTableKeys() {
        MockConnectionFactory factory = new MockConnectionFactory();
                SegmentHelper helper = new SegmentHelper(factory);

        final List<TableKey<byte[]>> keys1 = Arrays.asList(new TableKeyImpl<>(key0, new KeyVersionImpl(2L)),
                                                           new TableKeyImpl<>(key1, new KeyVersionImpl(10L)));

        final List<TableKey<byte[]>> keys2 = Arrays.asList(new TableKeyImpl<>(key2, new KeyVersionImpl(2L)),
                                                           new TableKeyImpl<>(key3, new KeyVersionImpl(10L)));

        CompletableFuture<TableSegment.IteratorItem<TableKey<byte[]>>> result = helper.readTableKeys("", "", 3,
                                                                                                     IteratorState.EMPTY,
                                                                                                     new MockHostControllerStore(),
                                                                                                     factory, "", System.nanoTime());

        assertFalse(result.isDone());
        factory.rp.tableKeysRead(getTableKeysRead(keys1, token1));
        assertTrue(Futures.await(result));
        // Validate the results.
        List<TableKey<byte[]>> iterationResult = result.join().getItems();
        assertArrayEquals(key0, iterationResult.get(0).getKey());
        assertEquals(2L, iterationResult.get(0).getVersion().getSegmentVersion());
        assertArrayEquals(key1, iterationResult.get(1).getKey());
        assertEquals(10L, iterationResult.get(1).getVersion().getSegmentVersion());
        assertArrayEquals(token1.array(), result.join().getState().toBytes().array());

        // fetch the next value
        result = helper.readTableKeys("", "", 3, IteratorState.fromBytes(token1), new MockHostControllerStore(), factory, "",
                                      System.nanoTime());
        assertFalse(result.isDone());
        factory.rp.tableKeysRead(getTableKeysRead(keys2, token2));
        assertTrue(Futures.await(result));
        // Validate the results.
        iterationResult = result.join().getItems();
        assertArrayEquals(key2, iterationResult.get(0).getKey());
        assertEquals(2L, iterationResult.get(0).getVersion().getSegmentVersion());
        assertArrayEquals(key3, iterationResult.get(1).getKey());
        assertEquals(10L, iterationResult.get(1).getVersion().getSegmentVersion());
        assertArrayEquals(token2.array(), result.join().getState().toBytes().array());

        Supplier<CompletableFuture<?>> futureSupplier = () -> helper.readTableKeys("", "", 1,
                                                                                   new IteratorStateImpl(wrappedBuffer(new byte[0])),
                                                                                   new MockHostControllerStore(),
                                                                                   factory, "", System.nanoTime());
        validateAuthTokenCheckFailed(factory, futureSupplier);
        validateWrongHost(factory, futureSupplier);
        validateConnectionDropped(factory, futureSupplier);
        validateProcessingFailure(factory, futureSupplier);
        validateNoSuchSegment(factory, futureSupplier);
    }

    @Test
    public void testReadTableEntries() {
        MockConnectionFactory factory = new MockConnectionFactory();
                SegmentHelper helper = new SegmentHelper(factory);
        List<TableEntry<byte[], byte[]>> entries1 = Arrays.asList(new TableEntryImpl<>(new TableKeyImpl<>(key0,
                                                                                                          new KeyVersionImpl(10L)), value),
                                                                  new TableEntryImpl<>(new TableKeyImpl<>(key1, new KeyVersionImpl(10L)),
                                                                                       value));

        List<TableEntry<byte[], byte[]>> entries2 = Arrays.asList(new TableEntryImpl<>(new TableKeyImpl<>(key2,
                                                                                                          new KeyVersionImpl(10L)), value),
                                                                  new TableEntryImpl<>(new TableKeyImpl<>(key3,
                                                                                                          new KeyVersionImpl(10L)), value));

        CompletableFuture<TableSegment.IteratorItem<TableEntry<byte[], byte[]>>> result = helper.readTableEntries("", "", 3,
                                                                                                                  null,
                                                                                                                  new MockHostControllerStore(),
                                                                                                                  factory, "", System.nanoTime());
        assertFalse(result.isDone());
        factory.rp.tableEntriesRead(getTableEntriesRead(entries1, token1));
        assertTrue(Futures.await(result));
        List<TableEntry<byte[], byte[]>> iterationResult = result.join().getItems();
        assertArrayEquals(key0, iterationResult.get(0).getKey().getKey());
        assertEquals(10L, iterationResult.get(0).getKey().getVersion().getSegmentVersion());
        assertArrayEquals(value, iterationResult.get(0).getValue());
        assertArrayEquals(key1, iterationResult.get(1).getKey().getKey());
        assertEquals(10L, iterationResult.get(1).getKey().getVersion().getSegmentVersion());
        assertArrayEquals(token1.array(), result.join().getState().toBytes().array());

        result = helper.readTableEntries("", "", 3, IteratorState.fromBytes(token1), new MockHostControllerStore(), factory, "",
                                         System.nanoTime());
        assertFalse(result.isDone());
        factory.rp.tableEntriesRead(getTableEntriesRead(entries2, token2));
        assertTrue(Futures.await(result));
        iterationResult = result.join().getItems();
        assertArrayEquals(key2, iterationResult.get(0).getKey().getKey());
        assertEquals(10L, iterationResult.get(0).getKey().getVersion().getSegmentVersion());
        assertArrayEquals(value, iterationResult.get(0).getValue());
        assertArrayEquals(key3, iterationResult.get(1).getKey().getKey());
        assertEquals(10L, iterationResult.get(1).getKey().getVersion().getSegmentVersion());
        assertArrayEquals(token2.array(), result.join().getState().toBytes().array());

        Supplier<CompletableFuture<?>> futureSupplier = () -> helper.readTableEntries("", "", 1,
                                                                                      new IteratorStateImpl(wrappedBuffer(new byte[0])),
                                                                                      new MockHostControllerStore(),
                                                                                      factory, "", System.nanoTime());
        validateAuthTokenCheckFailed(factory, futureSupplier);
        validateWrongHost(factory, futureSupplier);
        validateConnectionDropped(factory, futureSupplier);
        validateProcessingFailure(factory, futureSupplier);
        validateNoSuchSegment(factory, futureSupplier);
    }

    @Test(timeout = 30000)
    public void connectionTest() throws InterruptedException {
        PravegaNodeUri uri = new PravegaNodeUri("pravega", 1234);
        ConnectionFactory cf = new MockConnectionFactory();
        LinkedBlockingQueue<ConnectionEvent> eventQueue = new LinkedBlockingQueue<>();
        SegmentStoreConnectionPool pool = 
                new SegmentStoreConnectionPool(uri, cf, 2, 1, eventQueue::offer);
        ReplyProcessor myReplyProc = getReplyProcessor();
        
        // we should be able to establish two connections safely
        ConnectionObject connection1 = pool.getConnection(myReplyProc).join();
        assertEquals(ConnectionEvent.NewConnection, eventQueue.take());
        assertEquals(pool.connectionCount(), 1);

        ConnectionObject connection2 = pool.getConnection(myReplyProc).join();
        assertEquals(ConnectionEvent.NewConnection, eventQueue.take());
        assertEquals(pool.connectionCount(), 2);
        
        // return these connections
        pool.returnConnection(connection1);
        // verify that available connections is 1
        assertEquals(pool.availableCount(), 1);
        assertEquals(pool.connectionCount(), 2);

        pool.returnConnection(connection2);
        // connection manager should only have one connection as available connection. 
        // it should have destroyed the second connection.
        // verify that available connections is still 1
        assertEquals(pool.availableCount(), 1);
        assertEquals(pool.connectionCount(), 1);

        // verify that one connection was closed
        assertEquals(ConnectionEvent.ConnectionClosed, eventQueue.take());
        assertTrue(eventQueue.isEmpty());
        
        // now create two more connections
        // 1st should be delivered from available connections. 
        connection1 = pool.getConnection(myReplyProc).join();
        // verify its delivered from available connections 
        assertEquals(pool.connectionCount(), 1);
        assertEquals(pool.availableCount(), 0);
        // verify that no new connection was established
        assertTrue(eventQueue.isEmpty());

        // 2nd request should result in creation of new connection
        connection2 = pool.getConnection(myReplyProc).join();
        assertEquals(ConnectionEvent.NewConnection, eventQueue.take());
        assertEquals(pool.availableCount(), 0);
        // verify that there are two created connections
        assertEquals(pool.connectionCount(), 2);

        // attempt to create a third connection
        CompletableFuture<ConnectionObject> connection3Future = pool.getConnection(myReplyProc);
        // this would not have completed. the waiting queue should have this entry
        assertEquals(pool.connectionCount(), 2);
        assertEquals(pool.waitingCount(), 1);
        assertEquals(pool.availableCount(), 0);
        assertFalse(connection3Future.isDone());
        assertTrue(eventQueue.isEmpty());

        CompletableFuture<ConnectionObject> connection4Future = pool.getConnection(myReplyProc);
        assertEquals(pool.connectionCount(), 2);
        assertEquals(pool.waitingCount(), 2);
        assertEquals(pool.availableCount(), 0);
        assertTrue(eventQueue.isEmpty());

        // return connection1. it should be assigned to first waiting connection (connection3)
        pool.returnConnection(connection1);
        ConnectionObject connection3 = connection3Future.join();
        assertEquals(pool.connectionCount(), 2);
        assertEquals(pool.waitingCount(), 1);
        assertEquals(pool.availableCount(), 0);
        // verify that connection 3 received a connection object
        assertTrue(connection3Future.isDone());
        assertTrue(eventQueue.isEmpty());

        // now fail connection 3 and return it.
        connection2.failConnection();
        pool.returnConnection(connection2);
        // this should not be given to the waiting request. instead a new connection should be established. 
        assertEquals(ConnectionEvent.ConnectionClosed, eventQueue.take());

        ConnectionObject connection4 = connection4Future.join();
        assertEquals(ConnectionEvent.NewConnection, eventQueue.take());
        assertEquals(pool.connectionCount(), 2);
        assertEquals(pool.waitingCount(), 0);
        assertEquals(pool.availableCount(), 0);

        // create another waiting request
        CompletableFuture<ConnectionObject> connection5Future = pool.getConnection(myReplyProc);
        assertEquals(pool.connectionCount(), 2);
        assertEquals(pool.waitingCount(), 1);
        assertEquals(pool.availableCount(), 0);
        assertFalse(connection5Future.isDone());
        assertTrue(eventQueue.isEmpty());

        // test shutdown
        pool.shutdown();
        pool.returnConnection(connection3);
        assertEquals(pool.connectionCount(), 2);
        assertEquals(pool.waitingCount(), 0);
        assertEquals(pool.availableCount(), 0);

        // connection 5 should have been returned by using connection3
        ConnectionObject connection5 = connection5Future.join();
        // since returned connection served the waiting request no new event should have been generated
        assertTrue(eventQueue.isEmpty());

        // return connection 4
        pool.returnConnection(connection4);
        assertEquals(pool.connectionCount(), 1);
        assertEquals(pool.waitingCount(), 0);
        assertEquals(pool.availableCount(), 0);
        // returned connection should be closed
        assertEquals(ConnectionEvent.ConnectionClosed, eventQueue.take());

        // we should still be able to request new connections.. request connection 6.. this should be served immediately 
        // by way of new connection
        ConnectionObject connection6 = pool.getConnection(myReplyProc).join();
        assertEquals(pool.connectionCount(), 2);
        assertEquals(pool.waitingCount(), 0);
        assertEquals(pool.availableCount(), 0);
        assertEquals(ConnectionEvent.NewConnection, eventQueue.take());

        // request connect 7. this should wait as connection could is 2. 
        CompletableFuture<ConnectionObject> connection7Future = pool.getConnection(myReplyProc);
        assertEquals(pool.connectionCount(), 2);
        assertEquals(pool.waitingCount(), 1);
        assertEquals(pool.availableCount(), 0);
        
        // return connection 5.. connection7 should get connection5's object and no new connection should be established
        pool.returnConnection(connection5);
        ConnectionObject connection7 = connection7Future.join();
        assertEquals(pool.connectionCount(), 2);
        assertEquals(pool.waitingCount(), 0);
        assertEquals(pool.availableCount(), 0);
        assertTrue(eventQueue.isEmpty());

        pool.returnConnection(connection6);
        assertEquals(pool.connectionCount(), 1);
        // verify that returned connection is not included in available connection.
        assertEquals(pool.waitingCount(), 0);
        assertEquals(pool.availableCount(), 0);
        // also the returned connection is closed
        assertEquals(ConnectionEvent.ConnectionClosed, eventQueue.take());

        pool.returnConnection(connection7);
        assertEquals(pool.connectionCount(), 0);
        assertEquals(pool.waitingCount(), 0);
        assertEquals(pool.availableCount(), 0);
        assertEquals(ConnectionEvent.ConnectionClosed, eventQueue.take());
    }

    private ReplyProcessor getReplyProcessor() {
        return new ReplyProcessor() {
            @Override
            public void hello(WireCommands.Hello hello) {
                
            }

            @Override
            public void wrongHost(WireCommands.WrongHost wrongHost) {

            }

            @Override
            public void segmentAlreadyExists(WireCommands.SegmentAlreadyExists segmentAlreadyExists) {

            }

            @Override
            public void segmentIsSealed(WireCommands.SegmentIsSealed segmentIsSealed) {

            }

            @Override
            public void segmentIsTruncated(WireCommands.SegmentIsTruncated segmentIsTruncated) {

            }

            @Override
            public void noSuchSegment(WireCommands.NoSuchSegment noSuchSegment) {

            }

            @Override
            public void tableSegmentNotEmpty(WireCommands.TableSegmentNotEmpty tableSegmentNotEmpty) {

            }

            @Override
            public void invalidEventNumber(WireCommands.InvalidEventNumber invalidEventNumber) {

            }

            @Override
            public void appendSetup(WireCommands.AppendSetup appendSetup) {

            }

            @Override
            public void dataAppended(WireCommands.DataAppended dataAppended) {

            }

            @Override
            public void conditionalCheckFailed(WireCommands.ConditionalCheckFailed dataNotAppended) {

            }

            @Override
            public void segmentRead(WireCommands.SegmentRead segmentRead) {

            }

            @Override
            public void segmentAttributeUpdated(WireCommands.SegmentAttributeUpdated segmentAttributeUpdated) {

            }

            @Override
            public void segmentAttribute(WireCommands.SegmentAttribute segmentAttribute) {

            }

            @Override
            public void streamSegmentInfo(WireCommands.StreamSegmentInfo streamInfo) {

            }

            @Override
            public void segmentCreated(WireCommands.SegmentCreated segmentCreated) {

            }

            @Override
            public void segmentsMerged(WireCommands.SegmentsMerged segmentsMerged) {

            }

            @Override
            public void segmentSealed(WireCommands.SegmentSealed segmentSealed) {

            }

            @Override
            public void segmentTruncated(WireCommands.SegmentTruncated segmentTruncated) {

            }

            @Override
            public void segmentDeleted(WireCommands.SegmentDeleted segmentDeleted) {

            }

            @Override
            public void operationUnsupported(WireCommands.OperationUnsupported operationUnsupported) {

            }

            @Override
            public void keepAlive(WireCommands.KeepAlive keepAlive) {

            }

            @Override
            public void connectionDropped() {

            }

            @Override
            public void segmentPolicyUpdated(WireCommands.SegmentPolicyUpdated segmentPolicyUpdated) {

            }

            @Override
            public void processingFailure(Exception error) {

            }

            @Override
            public void authTokenCheckFailed(WireCommands.AuthTokenCheckFailed authTokenCheckFailed) {

            }

            @Override
            public void tableEntriesUpdated(WireCommands.TableEntriesUpdated tableEntriesUpdated) {

            }

            @Override
            public void tableKeysRemoved(WireCommands.TableKeysRemoved tableKeysRemoved) {

            }

            @Override
            public void tableRead(WireCommands.TableRead tableRead) {

            }

            @Override
            public void tableKeyDoesNotExist(WireCommands.TableKeyDoesNotExist tableKeyDoesNotExist) {

            }

            @Override
            public void tableKeyBadVersion(WireCommands.TableKeyBadVersion tableKeyBadVersion) {

            }

            @Override
            public void tableKeysRead(WireCommands.TableKeysRead tableKeysRead) {

            }

            @Override
            public void tableEntriesRead(WireCommands.TableEntriesRead tableEntriesRead) {

            }
        };
    }

    private WireCommands.TableEntries getTableEntries(List<TableEntry<byte[], byte[]>> entries) {
        return new WireCommands.TableEntries(entries.stream().map(e -> {
            val k = new WireCommands.TableKey(wrappedBuffer(e.getKey().getKey()), e.getKey().getVersion().getSegmentVersion());
            val v = new WireCommands.TableValue(wrappedBuffer(e.getValue()));
            return new AbstractMap.SimpleImmutableEntry<>(k, v);
        }).collect(Collectors.toList()));
    }

    private WireCommands.TableKeysRead getTableKeysRead(List<TableKey<byte[]>> keys, ByteBuf continuationToken) {
        return new WireCommands.TableKeysRead(0L, getQualifiedStreamSegmentName("", "", 0L),
                                              keys.stream().map(e -> new WireCommands.TableKey(wrappedBuffer(e.getKey()), e.getVersion().getSegmentVersion()))
                                                  .collect(Collectors.toList()),
                                              continuationToken);
    }

    private WireCommands.TableEntriesRead getTableEntriesRead(List<TableEntry<byte[], byte[]>> entries, ByteBuf continuationToken) {
        return new WireCommands.TableEntriesRead(0L, getQualifiedStreamSegmentName("", "", 0L),
                                                 getTableEntries(entries), continuationToken);
    }

    private void validateAuthTokenCheckFailed(MockConnectionFactory factory, Supplier<CompletableFuture<?>> futureSupplier) {
        CompletableFuture<?> future = futureSupplier.get();
        factory.rp.authTokenCheckFailed(new WireCommands.AuthTokenCheckFailed(0, "SomeException"));
        AssertExtensions.assertThrows("", future::join,
                                      t -> {
                                          Throwable ex = unwrap(t);
                                          return ex instanceof WireCommandFailedException && ex.getCause() instanceof AuthenticationException;
                                      });
    }

    private void validateNoSuchSegment(MockConnectionFactory factory, Supplier<CompletableFuture<?>> futureSupplier) {
        CompletableFuture<?> future = futureSupplier.get();
        factory.rp.noSuchSegment(new WireCommands.NoSuchSegment(0, "segment", "SomeException", -1L));
        AssertExtensions.assertThrows("", future::join,
                                      t -> {
                                          Throwable ex = unwrap(t);
                                          return ex instanceof WireCommandFailedException &&
                                                  (((WireCommandFailedException) ex).getReason() == WireCommandFailedException.Reason.SegmentDoesNotExist);
                                      });
    }

    private void validateWrongHost(MockConnectionFactory factory, Supplier<CompletableFuture<?>> futureSupplier) {
        CompletableFuture<?> future = futureSupplier.get();
        factory.rp.wrongHost(new WireCommands.WrongHost(0, "segment", "correctHost", "SomeException"));
        AssertExtensions.assertThrows("", future::join,
                                      t -> {
                                          Throwable ex = unwrap(t);
                                          return ex instanceof WireCommandFailedException &&
                                                  (((WireCommandFailedException) ex).getReason() == WireCommandFailedException.Reason.UnknownHost);
                                      });
    }

    private void validateConnectionDropped(MockConnectionFactory factory, Supplier<CompletableFuture<?>> futureSupplier) {
        CompletableFuture<?> future = futureSupplier.get();
        factory.rp.connectionDropped();
        AssertExtensions.assertThrows("", future::join,
                                      t -> {
                                          Throwable ex = unwrap(t);
                                          return ex instanceof WireCommandFailedException &&
                                                  (((WireCommandFailedException) ex).getReason() == WireCommandFailedException.Reason.ConnectionDropped);
                                      });
    }

    private void validateProcessingFailure(MockConnectionFactory factory, Supplier<CompletableFuture<?>> futureSupplier) {
        CompletableFuture<?> future = futureSupplier.get();
        factory.rp.processingFailure(new RuntimeException());
        AssertExtensions.assertThrows("", future::join, ex -> unwrap(ex) instanceof RuntimeException);
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