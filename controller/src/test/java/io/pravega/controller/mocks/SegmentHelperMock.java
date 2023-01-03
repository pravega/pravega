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
package io.pravega.controller.mocks;

import io.netty.buffer.Unpooled;
import io.pravega.client.connection.impl.ConnectionPool;
import io.pravega.client.tables.impl.HashTableIteratorItem;
import io.pravega.client.tables.impl.TableSegmentEntry;
import io.pravega.client.tables.impl.TableSegmentKey;
import io.pravega.client.tables.impl.TableSegmentKeyVersion;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.BitConverter;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.server.WireCommandFailedException;
import io.pravega.controller.store.host.HostControllerStore;
import io.pravega.controller.store.host.HostStoreFactory;
import io.pravega.controller.store.host.impl.HostMonitorConfigImpl;
import io.pravega.controller.stream.api.grpc.v1.Controller.NodeUri;
import io.pravega.controller.stream.api.grpc.v1.Controller.TxnStatus;
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
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

public class SegmentHelperMock {
    private static final int SERVICE_PORT = 12345;
    public static SegmentHelper getSegmentHelperMock() {
        SegmentHelper helper = spy(new SegmentHelper(mock(ConnectionPool.class), HostStoreFactory.createInMemoryStore(HostMonitorConfigImpl.dummyConfig()), mock(ScheduledExecutorService.class)));

        doReturn(NodeUri.newBuilder().setEndpoint("localhost").setPort(SERVICE_PORT).build()).when(helper).getSegmentUri(
                anyString(), anyString(), anyLong());

        doReturn(CompletableFuture.completedFuture(null)).when(helper).sealSegment(
                anyString(), anyString(), anyLong(), any(), anyLong());

        doReturn(CompletableFuture.completedFuture(null)).when(helper).createSegment(
                anyString(), anyString(), anyLong(), any(), any(), anyLong(), anyLong());

        doReturn(CompletableFuture.completedFuture(null)).when(helper).deleteSegment(
                anyString(), anyString(), anyLong(), any(), anyLong());

        doReturn(CompletableFuture.completedFuture(null)).when(helper).createTransaction(
                anyString(), anyString(), anyLong(), any(), any(), anyLong(), anyLong());

        TxnStatus txnStatus = TxnStatus.newBuilder().setStatus(TxnStatus.Status.SUCCESS).build();
        doReturn(CompletableFuture.completedFuture(txnStatus)).when(helper).abortTransaction(
                anyString(), anyString(), anyLong(), any(), any(), anyLong());

        doReturn(CompletableFuture.completedFuture(0L)).when(helper).mergeTxnSegments(
                anyString(), anyString(), anyLong(), anyLong(), any(), any(), anyLong());
        
        doAnswer(x -> {
            List<Long> list = ((List<UUID>) x.getArgument(4)).stream().map(z -> 0L).collect(Collectors.toList());
            return CompletableFuture.completedFuture(list);
        }).when(helper).mergeTxnSegments(
                anyString(), anyString(), anyLong(), anyLong(), any(), any(), anyLong());

        doReturn(CompletableFuture.completedFuture(null)).when(helper).updatePolicy(
                anyString(), anyString(), any(), anyLong(), any(), anyLong());

        doReturn(CompletableFuture.completedFuture(null)).when(helper).truncateSegment(
                anyString(), anyString(), anyLong(), anyLong(), any(), anyLong());

        doReturn(CompletableFuture.completedFuture(new WireCommands.StreamSegmentInfo(
                0L, "", true, true, false, 0L, 0L, 0L)))
                .when(helper).getSegmentInfo(anyString(), anyString(), anyLong(), anyString(), anyLong());

        doReturn(CompletableFuture.completedFuture(null)).when(helper).createTableSegment(
                anyString(), anyString(), anyLong(), anyBoolean(), anyInt(), anyLong());

        doReturn(CompletableFuture.completedFuture(null)).when(helper).deleteTableSegment(
                anyString(), anyBoolean(), anyString(), anyLong());
        return helper;
    }

    public static SegmentHelper getFailingSegmentHelperMock() {
        SegmentHelper helper = spy(new SegmentHelper(mock(ConnectionPool.class), mock(HostControllerStore.class), mock(ScheduledExecutorService.class)));

        doReturn(NodeUri.newBuilder().setEndpoint("localhost").setPort(SERVICE_PORT).build()).when(helper).getSegmentUri(
                anyString(), anyString(), anyLong());

        doReturn(Futures.failedFuture(new RuntimeException())).when(helper).sealSegment(
                anyString(), anyString(), anyLong(), any(), anyLong());

        doReturn(Futures.failedFuture(new RuntimeException())).when(helper).createSegment(
                anyString(), anyString(), anyLong(), any(), any(), anyLong(), anyLong());

        doReturn(Futures.failedFuture(new RuntimeException())).when(helper).deleteSegment(
                anyString(), anyString(), anyLong(), any(), anyLong());

        doReturn(Futures.failedFuture(new RuntimeException())).when(helper).createTransaction(
                anyString(), anyString(), anyLong(), any(), any(), anyLong(), anyLong());

        doReturn(Futures.failedFuture(new RuntimeException())).when(helper).abortTransaction(
                anyString(), anyString(), anyLong(), any(), any(), anyLong());

        doReturn(Futures.failedFuture(new RuntimeException())).when(helper).mergeTxnSegments(
                anyString(), anyString(), anyLong(), anyLong(), any(), anyString(), anyLong());

        doReturn(Futures.failedFuture(new RuntimeException())).when(helper).updatePolicy(
                anyString(), anyString(), any(), anyLong(), any(), anyLong());

        doReturn(Futures.failedFuture(new RuntimeException())).when(helper).createTableSegment(
                anyString(), anyString(), anyLong(), anyBoolean(), anyInt(), anyLong());

        return helper;
    }

    public static SegmentHelper getSegmentHelperMockForTables(ScheduledExecutorService executor) {
        SegmentHelper helper = getSegmentHelperMock();
        final Object lock = new Object();
        final Map<String, Map<ByteBuffer, TableSegmentEntry>> mapOfTables = new HashMap<>();
        final Map<String, Map<ByteBuffer, Long>> mapOfTablesPosition = new HashMap<>();

        // region create table
        doAnswer(x -> {
            String tableName = x.getArgument(0);
            return CompletableFuture.runAsync(() -> {
                synchronized (lock) {
                    mapOfTables.putIfAbsent(tableName, new HashMap<>());
                    mapOfTablesPosition.putIfAbsent(tableName, new HashMap<>());
                }
            }, executor);
        }).when(helper).createTableSegment(anyString(), anyString(), anyLong(), anyBoolean(), anyInt(), anyLong());
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
                        return null;
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
            List<TableSegmentEntry> entries = x.getArgument(1);
            return CompletableFuture.supplyAsync(() -> {
                synchronized (lock) {
                    Map<ByteBuffer, TableSegmentEntry> table = mapOfTables.get(tableName);
                    Map<ByteBuffer, Long> tablePos = mapOfTablesPosition.get(tableName);
                    if (table == null) {
                        throw new WireCommandFailedException(type,
                                WireCommandFailedException.Reason.SegmentDoesNotExist);
                    } else {
                        List<TableSegmentKeyVersion> resultList = new LinkedList<>();
                        entries.forEach(entry -> {
                            ByteBuffer key = entry.getKey().getKey().copy().nioBuffer();
                            byte[] value = entry.getValue().copy().array();
                            TableSegmentEntry existingEntry = table.get(key);
                            if (existingEntry == null) {
                                if (entry.getKey().getVersion().equals(TableSegmentKeyVersion.NOT_EXISTS)) {
                                    TableSegmentEntry newEntry = TableSegmentEntry.versioned(key.array(), value, 0);
                                    table.put(key, newEntry);
                                    tablePos.put(key, System.nanoTime());
                                    resultList.add(newEntry.getKey().getVersion());
                                } else {
                                    throw new WireCommandFailedException(type,
                                            WireCommandFailedException.Reason.TableKeyDoesNotExist);
                                }
                            } else if (existingEntry.getKey().getVersion().equals(entry.getKey().getVersion())) {
                                TableSegmentKeyVersion newVersion = TableSegmentKeyVersion.from(
                                        existingEntry.getKey().getVersion().getSegmentVersion() + 1);
                                TableSegmentEntry newEntry = TableSegmentEntry.versioned(key.array(), value, newVersion.getSegmentVersion());
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
            List<TableSegmentKey> keys = x.getArgument(1);
            return CompletableFuture.runAsync(() -> {
                synchronized (lock) {
                    Map<ByteBuffer, TableSegmentEntry> table = mapOfTables.get(tableName);
                    Map<ByteBuffer, Long> tablePos = mapOfTablesPosition.get(tableName);
                    if (table == null) {
                        throw new WireCommandFailedException(type,
                                WireCommandFailedException.Reason.SegmentDoesNotExist);
                    } else {
                        keys.forEach(rawKey -> {
                            ByteBuffer key = rawKey.getKey().copy().nioBuffer();
                            TableSegmentEntry existingEntry = table.get(key);
                            if (existingEntry != null) {
                                if (existingEntry.getKey().getVersion().equals(rawKey.getVersion())
                                        || rawKey.getVersion() == null
                                        || rawKey.getVersion().equals(TableSegmentKeyVersion.NO_VERSION)) {
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
            List<TableSegmentKey> requestKeys = x.getArgument(1);
            return CompletableFuture.supplyAsync(() -> {
                synchronized (lock) {
                    Map<ByteBuffer, TableSegmentEntry> table = mapOfTables.get(tableName);
                    if (table == null) {
                        throw new WireCommandFailedException(type,
                                WireCommandFailedException.Reason.SegmentDoesNotExist);
                    } else {
                        List<TableSegmentEntry> resultList = new LinkedList<>();

                        requestKeys.forEach(requestKey -> {
                            ByteBuffer key = requestKey.getKey().copy().nioBuffer();
                            TableSegmentEntry existingEntry = table.get(key);
                            if (existingEntry == null) {
                                resultList.add(TableSegmentEntry.notExists(key.array(), new byte[0]));
                            } else if (existingEntry.getKey().getVersion().equals(requestKey.getVersion())
                                    || requestKey.getVersion() == null
                                    || requestKey.getVersion().equals(TableSegmentKeyVersion.NO_VERSION)) {
                                resultList.add(duplicate(existingEntry));
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
            HashTableIteratorItem.State state = x.getArgument(2);
            final WireCommandType type = WireCommandType.READ_TABLE;
            return CompletableFuture.supplyAsync(() -> {
                synchronized (lock) {
                    Map<ByteBuffer, TableSegmentEntry> table = mapOfTables.get(tableName);
                    Map<ByteBuffer, Long> tablePos = mapOfTablesPosition.get(tableName);
                    if (table == null) {
                        throw new WireCommandFailedException(type,
                                WireCommandFailedException.Reason.SegmentDoesNotExist);
                    } else {
                        long floor;
                        if (state.equals(HashTableIteratorItem.State.EMPTY)) {
                            floor = 0L;
                        } else {
                            floor = new ByteArraySegment(state.toBytes()).getLong(0);
                        }
                        AtomicLong token = new AtomicLong(floor);
                        List<TableSegmentKey> list = tablePos.entrySet().stream()
                                                             .sorted(Comparator.comparingLong(Map.Entry::getValue))
                                                             .filter(c -> c.getValue() > floor)
                                                             .map(r -> {
                                                                 token.set(r.getValue());
                                                                 return duplicate(table.get(r.getKey()).getKey());
                                                             })
                                                             .limit(limit).collect(Collectors.toList());
                        byte[] continuationToken = new byte[Long.BYTES];
                        BitConverter.writeLong(continuationToken, 0, token.get());
                        HashTableIteratorItem.State newState = HashTableIteratorItem.State.fromBytes(Unpooled.wrappedBuffer(continuationToken));
                        return new HashTableIteratorItem<>(newState, list);
                    }
                }
            }, executor);
        }).when(helper).readTableKeys(anyString(), anyInt(), any(), anyString(), anyLong());
        // endregion        
        
        // region readTableEntries
        doAnswer(x -> {
            String tableName = x.getArgument(0);
            int limit = x.getArgument(1);
            HashTableIteratorItem.State state = x.getArgument(2);
            final WireCommandType type = WireCommandType.READ_TABLE;
            return CompletableFuture.supplyAsync(() -> {
                synchronized (lock) {
                    Map<ByteBuffer, TableSegmentEntry> table = mapOfTables.get(tableName);
                    Map<ByteBuffer, Long> tablePos = mapOfTablesPosition.get(tableName);
                    if (table == null) {
                        throw new WireCommandFailedException(type,
                                WireCommandFailedException.Reason.SegmentDoesNotExist);
                    } else {
                        long floor;
                        if (state.equals(HashTableIteratorItem.State.EMPTY)) {
                            floor = 0L;
                        } else {
                            floor = new ByteArraySegment(state.toBytes()).getLong(0);
                        }
                        AtomicLong token = new AtomicLong(floor);
                        List<TableSegmentEntry> list = tablePos.entrySet().stream()
                                                               .sorted(Comparator.comparingLong(Map.Entry::getValue))
                                                               .filter(c -> c.getValue() > floor)
                                                               .map(r -> {
                                                                   token.set(r.getValue());
                                                                   return duplicate(table.get(r.getKey()));
                                                               })
                                                               .limit(limit).collect(Collectors.toList());
                        byte[] continuationToken = new byte[Long.BYTES];
                        BitConverter.writeLong(continuationToken, 0, token.get());
                        HashTableIteratorItem.State newState = HashTableIteratorItem.State.fromBytes(Unpooled.wrappedBuffer(continuationToken));
                        return new HashTableIteratorItem<>(newState, list);
                    }
                }
            }, executor);
        }).when(helper).readTableEntries(anyString(), anyInt(), any(), anyString(), anyLong());
        // endregion
        return helper;
    }

    private static TableSegmentKey duplicate(TableSegmentKey key) {
        return key.getVersion().equals(TableSegmentKeyVersion.NOT_EXISTS)
                ? TableSegmentKey.notExists(key.getKey().copy())
                : TableSegmentKey.versioned(key.getKey().copy(), key.getVersion().getSegmentVersion());
    }

    private static TableSegmentEntry duplicate(TableSegmentEntry entry) {
        return entry.getKey().getVersion().equals(TableSegmentKeyVersion.NOT_EXISTS)
                ? TableSegmentEntry.notExists(entry.getKey().getKey().copy(), entry.getValue().copy())
                : TableSegmentEntry.versioned(entry.getKey().getKey().copy(), entry.getValue().copy(), entry.getKey().getVersion().getSegmentVersion());
    }
}
