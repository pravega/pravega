/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.mocks;

import io.pravega.client.netty.impl.ConnectionFactory;
import io.pravega.client.tables.impl.KeyVersion;
import io.pravega.client.tables.impl.KeyVersionImpl;
import io.pravega.client.tables.impl.TableEntry;
import io.pravega.client.tables.impl.TableEntryImpl;
import io.pravega.client.tables.impl.TableKey;
import io.pravega.client.tables.impl.TableKeyImpl;
import io.pravega.common.concurrent.Futures;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.server.WireCommandFailedException;
import io.pravega.controller.server.rpc.auth.AuthHelper;
import io.pravega.controller.store.host.HostControllerStore;
import io.pravega.controller.store.host.HostStoreFactory;
import io.pravega.controller.store.host.impl.HostMonitorConfigImpl;
import io.pravega.controller.stream.api.grpc.v1.Controller.NodeUri;
import io.pravega.shared.protocol.netty.WireCommandType;
import io.pravega.shared.protocol.netty.WireCommands;

import javax.annotation.concurrent.GuardedBy;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

public class SegmentHelperMock {
    private static final int SERVICE_PORT = 12345;
    
    public static SegmentHelper getSegmentHelperMock(HostControllerStore hostControllerStore, ConnectionFactory clientCF, AuthHelper authHelper) {
        SegmentHelper helper = spy(new SegmentHelper(hostControllerStore, clientCF, authHelper));

        doReturn(NodeUri.newBuilder().setEndpoint("localhost").setPort(SERVICE_PORT).build()).when(helper).getSegmentUri(
                anyString(), anyString(), anyLong());

        doReturn(CompletableFuture.completedFuture(true)).when(helper).sealSegment(
                anyString(), anyString(), anyLong(), anyLong());

        doReturn(CompletableFuture.completedFuture(true)).when(helper).createSegment(
                anyString(), anyString(), anyLong(), any(), anyLong());

        doReturn(CompletableFuture.completedFuture(true)).when(helper).deleteSegment(
                anyString(), anyString(), anyLong(), anyLong());

        doReturn(CompletableFuture.completedFuture(true)).when(helper).createTransaction(
                anyString(), anyString(), anyLong(), any());

        doReturn(CompletableFuture.completedFuture(true)).when(helper).abortTransaction(
                anyString(), anyString(), anyLong(), any());

        doReturn(CompletableFuture.completedFuture(true)).when(helper).commitTransaction(
                anyString(), anyString(), anyLong(), anyLong(), any());

        doReturn(CompletableFuture.completedFuture(true)).when(helper).updatePolicy(
                anyString(), anyString(), any(), anyLong(), anyLong());

        doReturn(CompletableFuture.completedFuture(true)).when(helper).truncateSegment(
                anyString(), anyString(), anyLong(), anyLong(), anyLong());

        doReturn(CompletableFuture.completedFuture(new WireCommands.StreamSegmentInfo(0L, "", true, true, false, 0L, 0L, 0L))).when(helper).getSegmentInfo(
                anyString(), anyString(), anyLong());

        return helper;
    }

    public static SegmentHelper getFailingSegmentHelperMock(HostControllerStore hostControllerStore, ConnectionFactory clientCF, AuthHelper authHelper) {
        SegmentHelper helper = spy(new SegmentHelper(hostControllerStore, clientCF, authHelper));

        doReturn(NodeUri.newBuilder().setEndpoint("localhost").setPort(SERVICE_PORT).build()).when(helper).getSegmentUri(
                anyString(), anyString(), anyLong());

        doReturn(Futures.failedFuture(new RuntimeException())).when(helper).sealSegment(
                anyString(), anyString(), anyLong(), anyLong());

        doReturn(Futures.failedFuture(new RuntimeException())).when(helper).createSegment(
                anyString(), anyString(), anyLong(), any(), anyLong());

        doReturn(Futures.failedFuture(new RuntimeException())).when(helper).deleteSegment(
                anyString(), anyString(), anyLong(), anyLong());

        doReturn(Futures.failedFuture(new RuntimeException())).when(helper).createTransaction(
                anyString(), anyString(), anyLong(), any());

        doReturn(Futures.failedFuture(new RuntimeException())).when(helper).abortTransaction(
                anyString(), anyString(), anyLong(), any());

        doReturn(Futures.failedFuture(new RuntimeException())).when(helper).commitTransaction(
                anyString(), anyString(), anyLong(), anyLong(), any());

        doReturn(Futures.failedFuture(new RuntimeException())).when(helper).updatePolicy(
                anyString(), anyString(), any(), anyLong(), anyLong());

        return helper;
    }

    public static SegmentHelper getSegmentHelperMockForTables() {
        HostControllerStore hostStore = HostStoreFactory.createInMemoryStore(HostMonitorConfigImpl.dummyConfig());
        ConnectionFactory connectionFactory = mock(ConnectionFactory.class);
        return getSegmentHelperMockForTables(hostStore, connectionFactory, AuthHelper.getDisabledAuthHelper());
    }
    
    public static SegmentHelper getSegmentHelperMockForTables(HostControllerStore hostControllerStore, ConnectionFactory clientCF, AuthHelper authHelper) {
        SegmentHelper helper = getSegmentHelperMock(hostControllerStore, clientCF, authHelper);
        final Object lock = new Object();
        final Map<String, Map<ByteBuffer, TableEntry<byte[], byte[]>>> mapOfTables = new HashMap<>();

        // region create table
        doAnswer(x -> {
            synchronized (lock) {
                String scope = x.getArgument(0);
                String tableName = x.getArgument(1);
                mapOfTables.putIfAbsent(scope + "/" + tableName, new HashMap<>());
            }
            return CompletableFuture.completedFuture(null);
        }).when(helper).createTableSegment(anyString(), anyString(), anyLong());
        // endregion
        
        // region delete table
        doAnswer(x -> {
            CompletableFuture<Boolean> result = new CompletableFuture<>();
            synchronized (lock) {
                String scope = x.getArgument(0);
                String tableName = x.getArgument(1);
                Boolean mustBeEmpty = x.getArgument(2);
                String key = scope + "/" + tableName;
                synchronized (lock) {
                    boolean empty = Optional.ofNullable(mapOfTables.get(key)).orElse(Collections.emptyMap()).isEmpty();
                    if (!mustBeEmpty || empty) {
                        mapOfTables.remove(key);
                        result.complete(true);
                    } else {
                        final WireCommandType type = WireCommandType.DELETE_TABLE_SEGMENT;
                        result.completeExceptionally(new WireCommandFailedException(type, 
                                WireCommandFailedException.Reason.TableSegmentNotEmpty));
                    }
                }
            }
            return result;
        }).when(helper).deleteTableSegment(anyString(), anyString(), anyBoolean(), anyLong());
        // endregion
        
        // region update keys
        doAnswer(x -> {
            CompletableFuture<List<KeyVersion>> result = new CompletableFuture<>();
            final WireCommandType type = WireCommandType.UPDATE_TABLE_ENTRIES;

            synchronized (lock) {
                String scope = x.getArgument(0);
                String tableName = x.getArgument(1);
                List<TableEntry<byte[], byte[]>> entries = x.getArgument(2);
                String tableScopedName = scope + "/" + tableName;
                synchronized (lock) {
                    Map<ByteBuffer, TableEntry<byte[], byte[]>> table = mapOfTables.get(tableScopedName);
                    if (table == null) {
                        result.completeExceptionally(new WireCommandFailedException(type, 
                                WireCommandFailedException.Reason.SegmentDoesNotExist));
                    } else {
                        List<KeyVersion> resultList = new LinkedList<>();
                        entries.forEach(entry -> {
                            ByteBuffer key = ByteBuffer.wrap(entry.getKey().getKey());
                            byte[] value = entry.getValue();
                            TableEntry<byte[], byte[]> existingEntry = table.get(key);
                            if (existingEntry == null) {
                                if (entry.getKey().getVersion().equals(KeyVersion.NOT_EXISTS)) {
                                    KeyVersion newVersion = new KeyVersionImpl(1);
                                    TableEntry<byte[], byte[]> newEntry = new TableEntryImpl<>(
                                            new TableKeyImpl<>(key.array(), newVersion), value);
                                    table.put(key, newEntry);
                                    resultList.add(newVersion);
                                } else {
                                    result.completeExceptionally(new WireCommandFailedException(type,
                                            WireCommandFailedException.Reason.TableKeyDoesNotExist));
                                }
                            } else if (existingEntry.getKey().getVersion().equals(entry.getKey().getVersion())) {
                                KeyVersion newVersion = new KeyVersionImpl(
                                        existingEntry.getKey().getVersion().getSegmentVersion() + 1);
                                TableEntry<byte[], byte[]> newEntry = new TableEntryImpl<>(
                                        new TableKeyImpl<>(key.array(), newVersion), value);
                                table.put(key, newEntry);
                                resultList.add(newVersion);
                            } else {
                                result.completeExceptionally(new WireCommandFailedException(type, 
                                        WireCommandFailedException.Reason.TableKeyBadVersion));
                            }
                        });
                        if (!result.isDone()) {
                            result.complete(resultList);
                        }
                    }
                }
            }
            return result;
        }).when(helper).updateTableEntries(anyString(), anyString(), any(), anyLong());
        // endregion
    
        // region remove keys    
        doAnswer(x -> {
            CompletableFuture<Void> result = new CompletableFuture<>();
            final WireCommandType type = WireCommandType.REMOVE_TABLE_KEYS;

            synchronized (lock) {
                String scope = x.getArgument(0);
                String tableName = x.getArgument(1);
                List<TableKey<byte[]>> entries = x.getArgument(2);
                String tableScopedName = scope + "/" + tableName;
                synchronized (lock) {
                    Map<ByteBuffer, TableEntry<byte[], byte[]>> table = mapOfTables.get(tableScopedName);
                    if (table == null) {
                        result.completeExceptionally(new WireCommandFailedException(type, 
                                WireCommandFailedException.Reason.SegmentDoesNotExist));
                    } else {
                        entries.forEach(entry -> {
                            ByteBuffer key = ByteBuffer.wrap(entry.getKey());
                            TableEntry<byte[], byte[]> existingEntry = table.get(key);
                            if (existingEntry == null) {
                                result.complete(null);
                            } else if (existingEntry.getKey().getVersion().equals(entry.getVersion()) 
                                    || entry.getVersion() == null || entry.getVersion().equals(KeyVersion.NOT_EXISTS)) {
                                table.remove(key);
                            } else {
                                result.completeExceptionally(new WireCommandFailedException(type, 
                                        WireCommandFailedException.Reason.TableKeyBadVersion));
                            }
                        });
                        if (!result.isDone()) {
                            result.complete(null);
                        }
                    }
                }
            }
            return result;
        }).when(helper).removeTableKeys(anyString(), anyString(), any(), anyLong());
        // endregion

        // region read keys    
        doAnswer(x -> {
            CompletableFuture<List<TableEntry<byte[], byte[]>>> result = new CompletableFuture<>();
            final WireCommandType type = WireCommandType.READ_TABLE;

            synchronized (lock) {
                String scope = x.getArgument(0);
                String tableName = x.getArgument(1);
                List<TableKey<byte[]>> entries = x.getArgument(2);
                String tableScopedName = scope + "/" + tableName;
                synchronized (lock) {
                    Map<ByteBuffer, TableEntry<byte[], byte[]>> table = mapOfTables.get(tableScopedName);
                    if (table == null) {
                        result.completeExceptionally(new WireCommandFailedException(type, 
                                WireCommandFailedException.Reason.SegmentDoesNotExist));
                    } else {
                        List<TableEntry<byte[], byte[]>> resultList = new LinkedList<>();
                        
                        entries.forEach(entry -> {
                            ByteBuffer key = ByteBuffer.wrap(entry.getKey());
                            TableEntry<byte[], byte[]> existingEntry = table.get(key);
                            if (existingEntry == null) {
                                result.completeExceptionally(new WireCommandFailedException(type, WireCommandFailedException.Reason.TableKeyDoesNotExist));
                            } else if (existingEntry.getKey().getVersion().equals(entry.getVersion()) 
                                    || entry.getVersion() == null || entry.getVersion().equals(KeyVersion.NOT_EXISTS)) {
                                resultList.add(table.get(key));
                            } else {
                                result.completeExceptionally(new WireCommandFailedException(type, 
                                        WireCommandFailedException.Reason.TableKeyBadVersion));
                            }
                        });
                        
                        if (!result.isDone()) {
                            result.complete(resultList);
                        }
                    }
                }
            }
            return result;
        }).when(helper).readTable(anyString(), anyString(), any(), anyLong());
        // endregion
        
        return helper;
    }
}
