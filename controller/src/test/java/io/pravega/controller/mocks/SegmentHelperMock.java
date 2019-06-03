/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.mocks;

import io.netty.buffer.Unpooled;
import io.pravega.client.netty.impl.ConnectionFactory;
import io.pravega.client.tables.impl.IteratorState;
import io.pravega.client.tables.impl.IteratorStateImpl;
import io.pravega.client.tables.impl.KeyVersion;
import io.pravega.client.tables.impl.KeyVersionImpl;
import io.pravega.client.tables.impl.TableEntry;
import io.pravega.client.tables.impl.TableEntryImpl;
import io.pravega.client.tables.impl.TableKey;
import io.pravega.client.tables.impl.TableKeyImpl;
import io.pravega.client.tables.impl.TableSegment;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.BitConverter;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.server.WireCommandFailedException;
import io.pravega.controller.store.host.HostControllerStore;
import io.pravega.controller.stream.api.grpc.v1.Controller.NodeUri;
import io.pravega.shared.protocol.netty.WireCommandType;
import io.pravega.shared.protocol.netty.WireCommands;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

public class SegmentHelperMock {
    private static final int SERVICE_PORT = 12345;
    public static SegmentHelper getSegmentHelperMock() {
        SegmentHelper helper = spy(new SegmentHelper(mock(ConnectionFactory.class), mock(HostControllerStore.class)));

        doReturn(NodeUri.newBuilder().setEndpoint("localhost").setPort(SERVICE_PORT).build()).when(helper).getSegmentUri(
                anyString(), anyString(), anyLong());

        doReturn(CompletableFuture.completedFuture(true)).when(helper).sealSegment(
                anyString(), anyString(), anyLong(), any(), anyLong());

        doReturn(CompletableFuture.completedFuture(true)).when(helper).createSegment(
                anyString(), anyString(), anyLong(), any(), any(), anyLong());

        doReturn(CompletableFuture.completedFuture(true)).when(helper).deleteSegment(
                anyString(), anyString(), anyLong(), any(), anyLong());

        doReturn(CompletableFuture.completedFuture(true)).when(helper).createTransaction(
                anyString(), anyString(), anyLong(), any(), any());

        doReturn(CompletableFuture.completedFuture(true)).when(helper).abortTransaction(
                anyString(), anyString(), anyLong(), any(), any());

        doReturn(CompletableFuture.completedFuture(true)).when(helper).commitTransaction(
                anyString(), anyString(), anyLong(), anyLong(), any(), any());

        doReturn(CompletableFuture.completedFuture(true)).when(helper).updatePolicy(
                anyString(), anyString(), any(), anyLong(), any(), anyLong());

        doReturn(CompletableFuture.completedFuture(true)).when(helper).truncateSegment(
                anyString(), anyString(), anyLong(), anyLong(), any(), anyLong());

        doReturn(CompletableFuture.completedFuture(new WireCommands.StreamSegmentInfo(0L, "", true, true, false, 0L, 0L, 0L))).when(helper).getSegmentInfo(
                anyString(), anyString(), anyLong(), anyString());

        return helper;
    }

    public static SegmentHelper getFailingSegmentHelperMock() {
        SegmentHelper helper = spy(new SegmentHelper(mock(ConnectionFactory.class), mock(HostControllerStore.class)));

        doReturn(NodeUri.newBuilder().setEndpoint("localhost").setPort(SERVICE_PORT).build()).when(helper).getSegmentUri(
                anyString(), anyString(), anyLong());

        doReturn(Futures.failedFuture(new RuntimeException())).when(helper).sealSegment(
                anyString(), anyString(), anyLong(), any(), anyLong());

        doReturn(Futures.failedFuture(new RuntimeException())).when(helper).createSegment(
                anyString(), anyString(), anyLong(), any(), any(), anyLong());

        doReturn(Futures.failedFuture(new RuntimeException())).when(helper).deleteSegment(
                anyString(), anyString(), anyLong(), any(), anyLong());

        doReturn(Futures.failedFuture(new RuntimeException())).when(helper).createTransaction(
                anyString(), anyString(), anyLong(), any(), any());

        doReturn(Futures.failedFuture(new RuntimeException())).when(helper).abortTransaction(
                anyString(), anyString(), anyLong(), any(), any());

        doReturn(Futures.failedFuture(new RuntimeException())).when(helper).commitTransaction(
                anyString(), anyString(), anyLong(), anyLong(), any(), any());

        doReturn(Futures.failedFuture(new RuntimeException())).when(helper).updatePolicy(
                anyString(), anyString(), any(), anyLong(), any(), anyLong());

        return helper;
    }

    public static SegmentHelper getSegmentHelperMockForTables(ScheduledExecutorService executor) {
        SegmentHelper helper = getSegmentHelperMock();
        final Object lock = new Object();
        final Map<String, Map<ByteBuffer, TableEntry<byte[], byte[]>>> mapOfTables = new HashMap<>();
        final Map<String, Map<ByteBuffer, Long>> mapOfTablesPosition = new HashMap<>();

        // region create table
        doAnswer(x -> {
            String tableName = x.getArgument(0);
            return CompletableFuture.runAsync(() -> {
                synchronized (lock) {
                    mapOfTables.putIfAbsent(tableName, new HashMap<>());
                    mapOfTablesPosition.put(tableName, new HashMap<>());
                }
            }, executor);
        }).when(helper).createTableSegment(anyString(), anyString(), anyLong());
        // endregion
        
        // region delete table
        doAnswer(x -> {
            String tableName = x.getArgument(0);
            Boolean mustBeEmpty = x.getArgument(1);
            final WireCommandType type = WireCommandType.DELETE_TABLE_SEGMENT;
            return CompletableFuture.supplyAsync(() -> {
                synchronized (lock) {
                    if (!mapOfTables.containsKey(tableName)) {
                        throw new WireCommandFailedException(type,
                                WireCommandFailedException.Reason.SegmentDoesNotExist);
                    }
                    boolean empty = Optional.ofNullable(mapOfTables.get(tableName)).orElse(Collections.emptyMap()).isEmpty();
                    if (!mustBeEmpty || empty) {
                        mapOfTables.remove(tableName);
                        mapOfTablesPosition.remove(tableName);
                        return true;
                    } else {
                        throw new WireCommandFailedException(type,
                                WireCommandFailedException.Reason.TableSegmentNotEmpty);
                    }
                }
            }, executor);
        }).when(helper).deleteTableSegment(anyString(), anyBoolean(), anyString(), anyLong());
        // endregion
        
        // region update keys
        doAnswer(x -> {
            final WireCommandType type = WireCommandType.UPDATE_TABLE_ENTRIES;
            String tableName = x.getArgument(0);
            List<TableEntry<byte[], byte[]>> entries = x.getArgument(1);
            return CompletableFuture.supplyAsync(() -> {
                synchronized (lock) {
                    Map<ByteBuffer, TableEntry<byte[], byte[]>> table = mapOfTables.get(tableName);
                    Map<ByteBuffer, Long> tablePos = mapOfTablesPosition.get(tableName);
                    if (table == null) {
                        throw new WireCommandFailedException(type,
                                WireCommandFailedException.Reason.SegmentDoesNotExist);
                    } else {
                        List<KeyVersion> resultList = new LinkedList<>();
                        entries.forEach(entry -> {
                            ByteBuffer key = ByteBuffer.wrap(entry.getKey().getKey());
                            byte[] value = entry.getValue();
                            TableEntry<byte[], byte[]> existingEntry = table.get(key);
                            if (existingEntry == null) {
                                if (entry.getKey().getVersion().equals(KeyVersion.NOT_EXISTS)) {
                                    KeyVersion newVersion = new KeyVersionImpl(0);
                                    TableEntry<byte[], byte[]> newEntry = new TableEntryImpl<>(
                                            new TableKeyImpl<>(key.array(), newVersion), value);
                                    table.put(key, newEntry);
                                    tablePos.put(key, System.nanoTime());
                                    resultList.add(newVersion);
                                } else {
                                    throw new WireCommandFailedException(type,
                                            WireCommandFailedException.Reason.TableKeyDoesNotExist);
                                }
                            } else if (existingEntry.getKey().getVersion().equals(entry.getKey().getVersion())) {
                                KeyVersion newVersion = new KeyVersionImpl(
                                        existingEntry.getKey().getVersion().getSegmentVersion() + 1);
                                TableEntry<byte[], byte[]> newEntry = new TableEntryImpl<>(
                                        new TableKeyImpl<>(key.array(), newVersion), value);
                                table.put(key, newEntry);
                                tablePos.put(key, System.nanoTime());
                                resultList.add(newVersion);
                            } else {
                                throw new WireCommandFailedException(type,
                                        WireCommandFailedException.Reason.TableKeyBadVersion);
                            }
                        });
                        return resultList;
                    }
                }
            }, executor);
        }).when(helper).updateTableEntries(anyString(), any(), anyString(), anyLong());
        // endregion
    
        // region remove keys    
        doAnswer(x -> {
            final WireCommandType type = WireCommandType.REMOVE_TABLE_KEYS;
            String tableName = x.getArgument(0);
            List<TableKey<byte[]>> entries = x.getArgument(1);
            return CompletableFuture.runAsync(() -> {
                synchronized (lock) {
                    Map<ByteBuffer, TableEntry<byte[], byte[]>> table = mapOfTables.get(tableName);
                    Map<ByteBuffer, Long> tablePos = mapOfTablesPosition.get(tableName);
                    if (table == null) {
                        throw new WireCommandFailedException(type,
                                WireCommandFailedException.Reason.SegmentDoesNotExist);
                    } else {
                        entries.forEach(entry -> {
                            ByteBuffer key = ByteBuffer.wrap(entry.getKey());
                            TableEntry<byte[], byte[]> existingEntry = table.get(key);
                            if (existingEntry != null) {
                                if (existingEntry.getKey().getVersion().equals(entry.getVersion())
                                        || entry.getVersion() == null || entry.getVersion().equals(KeyVersion.NOT_EXISTS)) {
                                    table.remove(key);
                                    tablePos.remove(key);
                                } else {
                                    throw new WireCommandFailedException(type,
                                            WireCommandFailedException.Reason.TableKeyBadVersion);
                                }
                            }
                        });
                    }
                }
            }, executor);
        }).when(helper).removeTableKeys(anyString(), any(), anyString(), anyLong());
        // endregion

        // region read keys    
        doAnswer(x -> {
            final WireCommandType type = WireCommandType.READ_TABLE;
            String tableName = x.getArgument(0);
            List<TableKey<byte[]>> entries = x.getArgument(1);
            return CompletableFuture.supplyAsync(() -> {
                synchronized (lock) {
                    Map<ByteBuffer, TableEntry<byte[], byte[]>> table = mapOfTables.get(tableName);
                    if (table == null) {
                        throw new WireCommandFailedException(type,
                                WireCommandFailedException.Reason.SegmentDoesNotExist);
                    } else {
                        List<TableEntry<byte[], byte[]>> resultList = new LinkedList<>();

                        entries.forEach(entry -> {
                            ByteBuffer key = ByteBuffer.wrap(entry.getKey());
                            TableEntry<byte[], byte[]> existingEntry = table.get(key);
                            if (existingEntry == null) {
                                throw new WireCommandFailedException(type, WireCommandFailedException.Reason.TableKeyDoesNotExist);
                            } else if (existingEntry.getKey().getVersion().equals(entry.getVersion())
                                    || entry.getVersion() == null || entry.getVersion().equals(KeyVersion.NOT_EXISTS)) {
                                resultList.add(table.get(key));
                            } else {
                                throw new WireCommandFailedException(type,
                                        WireCommandFailedException.Reason.TableKeyBadVersion);
                            }
                        });

                        return resultList;
                    }
                }
            }, executor);
        }).when(helper).readTable(anyString(), any(), anyString(), anyLong());
        // endregion
        
        // region readTableKeys
        doAnswer(x -> {
            String tableName = x.getArgument(0);
            int limit = x.getArgument(1);
            IteratorState state = x.getArgument(2);
            final WireCommandType type = WireCommandType.READ_TABLE;
            return CompletableFuture.supplyAsync(() -> {
                synchronized (lock) {
                    Map<ByteBuffer, TableEntry<byte[], byte[]>> table = mapOfTables.get(tableName);
                    Map<ByteBuffer, Long> tablePos = mapOfTablesPosition.get(tableName);
                    if (table == null) {
                        throw new WireCommandFailedException(type,
                                WireCommandFailedException.Reason.SegmentDoesNotExist);
                    } else {
                        long floor;
                        if (state.equals(IteratorState.EMPTY)) {
                            floor = 0L;
                        } else {
                            floor = BitConverter.readLong(state.toBytes().array(), 0);
                        }
                        AtomicLong token = new AtomicLong(floor);
                        List<TableKey<byte[]>> list = tablePos.entrySet().stream()
                                                                        .sorted(Comparator.comparingLong(Map.Entry::getValue))
                                                                        .filter(c -> c.getValue() > floor)
                                                                        .map(r -> {
                                                                            token.set(r.getValue());
                                                                            return table.get(r.getKey()).getKey();
                                                                        })
                                                                        .limit(limit).collect(Collectors.toList());
                        byte[] continuationToken = new byte[Long.BYTES];
                        BitConverter.writeLong(continuationToken, 0, token.get());
                        IteratorStateImpl newState = new IteratorStateImpl(Unpooled.wrappedBuffer(continuationToken));
                        return new TableSegment.IteratorItem<>(newState, list);
                    }
                }
            }, executor);
        }).when(helper).readTableKeys(anyString(), anyInt(), any(), anyString(), anyLong());
        // endregion        
        
        // region readTableEntries
        doAnswer(x -> {
            String tableName = x.getArgument(0);
            int limit = x.getArgument(1);
            IteratorState state = x.getArgument(2);
            final WireCommandType type = WireCommandType.READ_TABLE;
            return CompletableFuture.supplyAsync(() -> {
                synchronized (lock) {
                    Map<ByteBuffer, TableEntry<byte[], byte[]>> table = mapOfTables.get(tableName);
                    Map<ByteBuffer, Long> tablePos = mapOfTablesPosition.get(tableName);
                    if (table == null) {
                        throw new WireCommandFailedException(type,
                                WireCommandFailedException.Reason.SegmentDoesNotExist);
                    } else {
                        long floor;
                        if (state.equals(IteratorState.EMPTY)) {
                            floor = 0L;
                        } else {
                            floor = BitConverter.readLong(state.toBytes().array(), 0);
                        }
                        AtomicLong token = new AtomicLong(floor);
                        List<TableEntry<byte[], byte[]>> list = tablePos.entrySet().stream()
                                                                        .sorted(Comparator.comparingLong(Map.Entry::getValue))
                                                                        .filter(c -> c.getValue() > floor)
                                                                        .map(r -> {
                                                                            token.set(r.getValue());
                                                                            return table.get(r.getKey());
                                                                        })
                                                                        .limit(limit).collect(Collectors.toList());
                        byte[] continuationToken = new byte[Long.BYTES];
                        BitConverter.writeLong(continuationToken, 0, token.get());
                        IteratorStateImpl newState = new IteratorStateImpl(Unpooled.wrappedBuffer(continuationToken));
                        return new TableSegment.IteratorItem<>(newState, list);
                    }
                }
            }, executor);
        }).when(helper).readTableEntries(anyString(), anyInt(), any(), anyString(), anyLong());
        // endregion
        return helper;
    }
}
