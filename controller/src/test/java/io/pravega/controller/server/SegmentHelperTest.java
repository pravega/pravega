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
import io.pravega.common.Exceptions;
import io.pravega.common.cluster.Host;
import io.pravega.common.concurrent.Futures;
import io.pravega.controller.server.rpc.auth.AuthHelper;
import io.pravega.controller.store.host.HostControllerStore;
import io.pravega.controller.store.stream.StoreException;
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
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.val;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static io.netty.buffer.Unpooled.wrappedBuffer;
import static io.pravega.common.Exceptions.unwrap;
import static io.pravega.shared.segment.StreamSegmentNameUtils.getQualifiedStreamSegmentName;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SegmentHelperTest {

    private SegmentHelper helper;
    private final byte[] key0 = "k".getBytes();
    private final byte[] key1 = "k1".getBytes();
    private final byte[] key2 = "k2".getBytes();
    private final byte[] key3 = "k3".getBytes();
    private final byte[] value = "v".getBytes();
    private final ByteBuf token1 = wrappedBuffer(new byte[]{0x01});
    private final ByteBuf token2 = wrappedBuffer(new byte[]{0x02});

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void getSegmentUri() {
        MockConnectionFactory factory = new MockConnectionFactory(); 
        SegmentHelper helper = new SegmentHelper(new MockHostControllerStore(), factory, AuthHelper.getDisabledAuthHelper());

        helper.getSegmentUri("", "", 0);
    }

    @Test
    public void createSegment() {
        MockConnectionFactory factory = new MockConnectionFactory();
        SegmentHelper helper = new SegmentHelper(new MockHostControllerStore(), factory, AuthHelper.getDisabledAuthHelper());
        CompletableFuture<Boolean> retVal = helper.createSegment("", "",
                0, ScalingPolicy.fixed(2), Long.MIN_VALUE);
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
        SegmentHelper helper = new SegmentHelper(new MockHostControllerStore(), factory, AuthHelper.getDisabledAuthHelper());
        CompletableFuture<Boolean> retVal = helper.truncateSegment("", "", 0L, 0L,
                System.nanoTime());
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
        SegmentHelper helper = new SegmentHelper(new MockHostControllerStore(), factory, AuthHelper.getDisabledAuthHelper());
        CompletableFuture<Boolean> retVal = helper.deleteSegment("", "", 0L, System.nanoTime());
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
        SegmentHelper helper = new SegmentHelper(new MockHostControllerStore(), factory, AuthHelper.getDisabledAuthHelper());
        CompletableFuture<Boolean> retVal = helper.sealSegment("", "", 0L,
                System.nanoTime());
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
        SegmentHelper helper = new SegmentHelper(new MockHostControllerStore(), factory, AuthHelper.getDisabledAuthHelper());
        CompletableFuture<UUID> retVal = helper.createTransaction("", "", 0L, new UUID(0, 0L));
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
        SegmentHelper helper = new SegmentHelper(new MockHostControllerStore(), factory, AuthHelper.getDisabledAuthHelper());
        
        CompletableFuture<Controller.TxnStatus> retVal = helper.commitTransaction("", "", 0L, 0L, new UUID(0, 0L));
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
        SegmentHelper helper = new SegmentHelper(new MockHostControllerStore(), factory, AuthHelper.getDisabledAuthHelper());
        
        CompletableFuture<Controller.TxnStatus> retVal = helper.abortTransaction("", "", 0L, new UUID(0, 0L));
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
        SegmentHelper helper = new SegmentHelper(new MockHostControllerStore(), factory, AuthHelper.getDisabledAuthHelper());
        
        CompletableFuture<Void> retVal = helper.updatePolicy("", "", ScalingPolicy.fixed(1), 0L, System.nanoTime());
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
        SegmentHelper helper = new SegmentHelper(new MockHostControllerStore(), factory, AuthHelper.getDisabledAuthHelper());
        CompletableFuture<WireCommands.StreamSegmentInfo> retVal = helper.getSegmentInfo("", "", 0L);
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
        SegmentHelper helper = new SegmentHelper(new MockHostControllerStore(), factory, AuthHelper.getDisabledAuthHelper());
        
        // On receiving SegmentAlreadyExists true should be returned.
        CompletableFuture<Boolean> result = helper.createTableSegment("", "", Long.MIN_VALUE);
        factory.rp.segmentAlreadyExists(new WireCommands.SegmentAlreadyExists(0, getQualifiedStreamSegmentName("", "", 0L), ""));
        assertTrue(result.join());

        // On Receiving SegmentCreated true should be returned.
        result = helper.createTableSegment("", "", Long.MIN_VALUE);
        factory.rp.segmentCreated(new WireCommands.SegmentCreated(0, getQualifiedStreamSegmentName("", "", 0L)));
        assertTrue(result.join());

        // Validate failure conditions.
        Supplier<CompletableFuture<?>> futureSupplier = () -> helper.createTableSegment("", "", Long.MIN_VALUE);
        validateAuthTokenCheckFailed(factory, futureSupplier);
        validateWrongHost(factory, futureSupplier);
        validateConnectionDropped(factory, futureSupplier);
        validateProcessingFailure(factory, futureSupplier);

    }

    @Test
    public void testDeleteTableSegment() {
        MockConnectionFactory factory = new MockConnectionFactory();
        SegmentHelper helper = new SegmentHelper(new MockHostControllerStore(), factory, AuthHelper.getDisabledAuthHelper());

        // On receiving NoSuchSegment true should be returned.
        CompletableFuture<Boolean> result = helper.deleteTableSegment("", "", true, System.nanoTime());
        factory.rp.noSuchSegment(new WireCommands.NoSuchSegment(0, getQualifiedStreamSegmentName("", "", 0L), ""));
        AssertExtensions.assertFutureThrows("", result, e -> Exceptions.unwrap(e) instanceof WireCommandFailedException
                && ((WireCommandFailedException) Exceptions.unwrap(e)).getReason().equals(WireCommandFailedException.Reason.SegmentDoesNotExist));

        // On receiving SegmentDeleted true should be returned.
        result = helper.deleteTableSegment("", "", true, System.nanoTime());
        factory.rp.segmentDeleted(new WireCommands.SegmentDeleted(0, getQualifiedStreamSegmentName("", "", 0L)));
        assertTrue(result.join());

        // On receiving TableSegmentNotEmpty WireCommandFailedException is thrown.
        result = helper.deleteTableSegment("", "", true, System.nanoTime());
        factory.rp.tableSegmentNotEmpty(new WireCommands.TableSegmentNotEmpty(0, getQualifiedStreamSegmentName("", "", 0L), ""));
        AssertExtensions.assertThrows("", result::join,
                                      ex -> ex instanceof WireCommandFailedException &&
                                              (((WireCommandFailedException) ex).getReason() == WireCommandFailedException.Reason.TableSegmentNotEmpty));

        Supplier<CompletableFuture<?>> futureSupplier = () -> helper.deleteTableSegment("", "", true, System.nanoTime());
        validateAuthTokenCheckFailed(factory, futureSupplier);
        validateWrongHost(factory, futureSupplier);
        validateConnectionDropped(factory, futureSupplier);
        validateProcessingFailure(factory, futureSupplier);
    }

    @Test
    public void testUpdateTableEntries() {
        MockConnectionFactory factory = new MockConnectionFactory();
        SegmentHelper helper = new SegmentHelper(new MockHostControllerStore(), factory, AuthHelper.getDisabledAuthHelper());
        
        List<TableEntry<byte[], byte[]>> entries = Arrays.asList(new TableEntryImpl<>(new TableKeyImpl<>("k".getBytes(), KeyVersion.NOT_EXISTS), "v".getBytes()),
                                                                 new TableEntryImpl<>(new TableKeyImpl<>("k1".getBytes(), null), "v".getBytes()),
                                                                 new TableEntryImpl<>(new TableKeyImpl<>("k2".getBytes(), new KeyVersionImpl(10L)),
                                                                                      "v".getBytes()));

        List<KeyVersion> expectedVersions = Arrays.asList(new KeyVersionImpl(0L),
                                                          new KeyVersionImpl(1L),
                                                          new KeyVersionImpl(11L));

        // On receiving TableEntriesUpdated.
        CompletableFuture<List<KeyVersion>> result = helper.updateTableEntries("", "", entries, System.nanoTime());
        factory.rp.tableEntriesUpdated(new WireCommands.TableEntriesUpdated(0, Arrays.asList(0L, 1L, 11L)));
        assertEquals(expectedVersions, result.join());

        // On receiving TableKeyDoesNotExist.
        result = helper.updateTableEntries("", "", entries, System.nanoTime());
        factory.rp.tableKeyDoesNotExist(new WireCommands.TableKeyDoesNotExist(0, getQualifiedStreamSegmentName("", "", 0L), ""));
        AssertExtensions.assertThrows("", result::join,
                                      ex -> ex instanceof WireCommandFailedException &&
                                              (((WireCommandFailedException) ex).getReason() == WireCommandFailedException.Reason.TableKeyDoesNotExist));

        // On receiving TableKeyBadVersion.
        result = helper.updateTableEntries("", "", entries, System.nanoTime());
        factory.rp.tableKeyBadVersion(new WireCommands.TableKeyBadVersion(0, getQualifiedStreamSegmentName("", "", 0L), ""));
        AssertExtensions.assertThrows("", result::join,
                                      ex -> ex instanceof WireCommandFailedException &&
                                              (((WireCommandFailedException) ex).getReason() == WireCommandFailedException.Reason.TableKeyBadVersion));

        Supplier<CompletableFuture<?>> futureSupplier = () -> helper.updateTableEntries("", "", entries, System.nanoTime());
        validateAuthTokenCheckFailed(factory, futureSupplier);
        validateWrongHost(factory, futureSupplier);
        validateConnectionDropped(factory, futureSupplier);
        validateProcessingFailure(factory, futureSupplier);
        validateNoSuchSegment(factory, futureSupplier);
    }

    @Test
    public void testRemoveTableKeys() {
        MockConnectionFactory factory = new MockConnectionFactory();
        SegmentHelper helper = new SegmentHelper(new MockHostControllerStore(), factory, AuthHelper.getDisabledAuthHelper());
        
        List<TableKey<byte[]>> keys = Arrays.asList(new TableKeyImpl<>("k".getBytes(), KeyVersion.NOT_EXISTS),
                                                    new TableKeyImpl<>("k1".getBytes(), KeyVersion.NOT_EXISTS));

        // On receiving TableKeysRemoved.
        CompletableFuture<Void> result = helper.removeTableKeys("", "", keys, System.nanoTime());
        factory.rp.tableKeysRemoved(new WireCommands.TableKeysRemoved(0, getQualifiedStreamSegmentName("", "", 0L) ));
        assertTrue(Futures.await(result));

        // On receiving TableKeyDoesNotExist.
        result = helper.removeTableKeys("", "", keys, System.nanoTime());
        factory.rp.tableKeyDoesNotExist(new WireCommands.TableKeyDoesNotExist(0, getQualifiedStreamSegmentName("", "", 0L), ""));
        assertTrue(Futures.await(result));

        // On receiving TableKeyBadVersion.
        result = helper.removeTableKeys("", "", keys, System.nanoTime());
        factory.rp.tableKeyBadVersion(new WireCommands.TableKeyBadVersion(0, getQualifiedStreamSegmentName("", "", 0L), ""));
        AssertExtensions.assertThrows("", result::join,
                                      ex -> ex instanceof WireCommandFailedException &&
                                              (((WireCommandFailedException) ex).getReason() == WireCommandFailedException.Reason.TableKeyBadVersion));

        Supplier<CompletableFuture<?>> futureSupplier = () -> helper.removeTableKeys("", "", keys, System.nanoTime());
        validateAuthTokenCheckFailed(factory, futureSupplier);
        validateWrongHost(factory, futureSupplier);
        validateConnectionDropped(factory, futureSupplier);
        validateProcessingFailure(factory, futureSupplier);
        validateNoSuchSegment(factory, futureSupplier);
    }

    @Test
    public void testReadTable() {
        MockConnectionFactory factory = new MockConnectionFactory();
        SegmentHelper helper = new SegmentHelper(new MockHostControllerStore(), factory, AuthHelper.getDisabledAuthHelper());
        
        List<TableKey<byte[]>> keys = Arrays.asList(new TableKeyImpl<>("k".getBytes(), KeyVersion.NOT_EXISTS),
                                                    new TableKeyImpl<>("k1".getBytes(), KeyVersion.NOT_EXISTS));

        List<TableEntry<byte[], byte[]>> entries = Arrays.asList(new TableEntryImpl<>(new TableKeyImpl<>("k".getBytes(), new KeyVersionImpl(10L)), "v".getBytes()),
                                                                 new TableEntryImpl<>(new TableKeyImpl<>("k1".getBytes(), new KeyVersionImpl(10L)), "v".getBytes()));
        // On receiving TableKeysRemoved.
        CompletableFuture<List<TableEntry<byte[], byte[]>>> result = helper.readTable("", "", keys, System.nanoTime());
        factory.rp.tableRead(new WireCommands.TableRead(0, getQualifiedStreamSegmentName("", "", 0L), getTableEntries(entries)));
        List<TableEntry<byte[], byte[]>> readResult = result.join();
        assertArrayEquals(key0, readResult.get(0).getKey().getKey());
        assertEquals(10L, readResult.get(0).getKey().getVersion().getSegmentVersion());
        assertArrayEquals(value, readResult.get(0).getValue());
        assertArrayEquals(key1, readResult.get(1).getKey().getKey());
        assertEquals(10L, readResult.get(1).getKey().getVersion().getSegmentVersion());
        assertArrayEquals(value, readResult.get(1).getValue());

        // On receiving TableKeyDoesNotExist.
        result = helper.readTable("", "", keys, System.nanoTime());
        factory.rp.tableKeyDoesNotExist(new WireCommands.TableKeyDoesNotExist(0, getQualifiedStreamSegmentName("", "", 0L), ""));
        AssertExtensions.assertThrows("", result::join,
                                      ex -> ex instanceof WireCommandFailedException &&
                                              (((WireCommandFailedException) ex).getReason() == WireCommandFailedException.Reason.TableKeyDoesNotExist));

        Supplier<CompletableFuture<?>> futureSupplier = () -> helper.readTable("", "", keys, System.nanoTime());
        validateAuthTokenCheckFailed(factory, futureSupplier);
        validateWrongHost(factory, futureSupplier);
        validateConnectionDropped(factory, futureSupplier);
        validateProcessingFailure(factory, futureSupplier);
        validateNoSuchSegment(factory, futureSupplier);
    }

    @Test
    public void testReadTableKeys() {
        MockConnectionFactory factory = new MockConnectionFactory();

        final List<TableKey<byte[]>> keys1 = Arrays.asList(new TableKeyImpl<>(key0, new KeyVersionImpl(2L)),
                                                           new TableKeyImpl<>(key1, new KeyVersionImpl(10L)));

        final List<TableKey<byte[]>> keys2 = Arrays.asList(new TableKeyImpl<>(key2, new KeyVersionImpl(2L)),
                                                           new TableKeyImpl<>(key3, new KeyVersionImpl(10L)));

        CompletableFuture<TableSegment.IteratorItem<TableKey<byte[]>>> result = helper.readTableKeys("", "", 3,
                                                                                                     IteratorState.EMPTY, System.nanoTime());

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
        result = helper.readTableKeys("", "", 3, IteratorState.fromBytes(token1), System.nanoTime());
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
                                                                                   System.nanoTime());
        validateAuthTokenCheckFailed(factory, futureSupplier);
        validateWrongHost(factory, futureSupplier);
        validateConnectionDropped(factory, futureSupplier);
        validateProcessingFailure(factory, futureSupplier);
        validateNoSuchSegment(factory, futureSupplier);
    }

    @Test
    public void testReadTableEntries() {
        MockConnectionFactory factory = new MockConnectionFactory();
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
                                                                                                                  System.nanoTime());
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

        result = helper.readTableEntries("", "", 3, IteratorState.fromBytes(token1), System.nanoTime());
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
                                                                                      System.nanoTime());
        validateAuthTokenCheckFailed(factory, futureSupplier);
        validateWrongHost(factory, futureSupplier);
        validateConnectionDropped(factory, futureSupplier);
        validateProcessingFailure(factory, futureSupplier);
        validateNoSuchSegment(factory, futureSupplier);
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
        factory.rp.noSuchSegment(new WireCommands.NoSuchSegment(0, "segment", "SomeException"));
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