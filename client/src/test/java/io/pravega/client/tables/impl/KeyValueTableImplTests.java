/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.tables.impl;

import io.netty.buffer.ByteBuf;
import io.pravega.client.admin.KeyValueTableInfo;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.mock.MockConnectionFactoryImpl;
import io.pravega.client.stream.mock.MockController;
import io.pravega.client.tables.BadKeyVersionException;
import io.pravega.client.tables.ConditionalTableUpdateException;
import io.pravega.client.tables.IteratorItem;
import io.pravega.client.tables.IteratorState;
import io.pravega.client.tables.KeyValueTable;
import io.pravega.client.tables.KeyValueTableConfiguration;
import io.pravega.client.tables.NoSuchKeyException;
import io.pravega.client.tables.TableEntry;
import io.pravega.common.Exceptions;
import io.pravega.common.util.AsyncIterator;
import io.pravega.test.common.AssertExtensions;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import javax.annotation.concurrent.GuardedBy;
import lombok.Cleanup;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for the {@link KeyValueTableImpl} class.
 */
public class KeyValueTableImplTests extends KeyValueTableTestBase {
    private static final KeyValueTableInfo KVT = new KeyValueTableInfo("Scope", "KVT");
    private MockConnectionFactoryImpl connectionFactory;
    private MockTableSegmentFactory segmentFactory;
    private MockController controller;
    private KeyValueTable<Integer, String> keyValueTable;

    @Override
    protected int getThreadPoolSize() {
        return 1;
    }

    @Override
    protected KeyValueTable<Integer, String> createKeyValueTable() {
        return this.keyValueTable;
    }

    @Before
    public void setup() throws Exception {
        super.setup();
        this.connectionFactory = new MockConnectionFactoryImpl();
        this.controller = new MockController("localhost", 0, this.connectionFactory, false);
        this.controller.createScope(KVT.getScope());
        this.controller.createKeyValueTable(KVT.getScope(), KVT.getKeyValueTableName(),
                KeyValueTableConfiguration.builder().partitionCount(getSegmentCount()).build());
        this.segmentFactory = new MockTableSegmentFactory(getSegmentCount());
        this.keyValueTable = new KeyValueTableImpl<>(KVT, this.segmentFactory, this.controller, KEY_SERIALIZER, VALUE_SERIALIZER);
    }

    @After
    public void tearDown() {
        this.keyValueTable.close();
        this.controller.close();
        this.connectionFactory.close();
    }

    /**
     * Tests the {@link KeyValueTable#close()} method.
     */
    @Test
    public void testClose() {
        @Cleanup
        val kvt = createKeyValueTable();
        val iteration = new AtomicInteger(0);
        forEveryKeyFamily(false, (keyFamily, keyIds) -> {
            val entry = TableEntry.notExists(getKey(0), getValue(0, iteration.get()));
            kvt.replaceAll(keyFamily, Collections.singletonList(entry)).join();
        });

        Assert.assertEquals("Unexpected number of open segments before closing.", getSegmentCount(), this.segmentFactory.getOpenSegmentCount());
        kvt.close();
        Assert.assertEquals("Not expecting any open segments after closing.", 0, this.segmentFactory.getOpenSegmentCount());
    }

    @RequiredArgsConstructor
    private class MockTableSegmentFactory implements TableSegmentFactory {
        @GuardedBy("segments")
        private final HashMap<Segment, TableSegment> segments = new HashMap<>();
        private final int segmentCount;

        @Override
        public TableSegment forSegment(@NonNull Segment segment) {
            AssertExtensions.assertLessThan("Too many segments requested.", this.segmentCount, segment.getSegmentId());
            synchronized (this.segments) {
                Assert.assertNull("Segment requested multiple times.", this.segments.get(segment));
                TableSegment ts = new MockTableSegment(segment, this::segmentClosed);
                this.segments.put(segment, ts);
                return ts;
            }
        }

        int getOpenSegmentCount() {
            synchronized (this.segments) {
                return this.segments.size();
            }
        }

        private void segmentClosed(Segment s) {
            synchronized (this.segments) {
                this.segments.remove(s);
            }
        }
    }

    @RequiredArgsConstructor
    private class MockTableSegment implements TableSegment {
        private final Segment segment;
        private final Consumer<Segment> onClose;
        private final AtomicLong nextVersion = new AtomicLong();
        @GuardedBy("data")
        private final Map<ByteBuf, EntryValue> data = new HashMap<>();
        @GuardedBy("data")
        private boolean closed = false;

        @Override
        public long getSegmentId() {
            return this.segment.getSegmentId();
        }

        @Override
        public void close() {
            Consumer<Segment> callback = null;
            synchronized (this.data) {
                if (!this.closed) {
                    this.data.forEach((k, e) -> {
                        k.release();
                        e.value.release();
                    });
                    this.data.clear();
                    callback = this.onClose;
                    this.closed = true;
                }
            }

            if (callback != null) {
                callback.accept(this.segment);
            }
        }

        @Override
        public CompletableFuture<List<TableSegmentKeyVersion>> put(Iterator<TableSegmentEntry> entries) {
            return CompletableFuture.supplyAsync(() -> {
                synchronized (this.data) {
                    Exceptions.checkNotClosed(this.closed, this);
                    val result = new ArrayList<TableSegmentKeyVersion>();
                    val toUpdate = new HashMap<ByteBuf, EntryValue>();
                    entries.forEachRemaining(e -> {
                        checkVersion(e.getKey());
                        long version = this.nextVersion.getAndIncrement();
                        toUpdate.put(e.getKey().getKey().copy(), new EntryValue(e.getValue().copy(), version));
                        result.add(TableSegmentKeyVersion.from(version));
                    });
                    this.data.putAll(toUpdate);
                    return result;
                }
            }, executorService());
        }

        @Override
        public CompletableFuture<Void> remove(Iterator<TableSegmentKey> keys) {
            return CompletableFuture.runAsync(() -> {
                synchronized (this.data) {
                    Exceptions.checkNotClosed(this.closed, this);
                    val toRemove = new ArrayList<ByteBuf>();
                    keys.forEachRemaining(k -> {
                        checkVersion(k);
                        toRemove.add(k.getKey());
                    });
                    toRemove.forEach(this.data::remove);
                }
            }, executorService());
        }

        @Override
        public CompletableFuture<List<TableSegmentEntry>> get(Iterator<ByteBuf> keys) {
            return CompletableFuture.supplyAsync(() -> {
                synchronized (this.data) {
                    Exceptions.checkNotClosed(this.closed, this);
                    val result = new ArrayList<TableSegmentEntry>();
                    keys.forEachRemaining(k -> {
                        EntryValue ev = this.data.getOrDefault(k, null);
                        TableSegmentEntry e = ev == null
                                ? TableSegmentEntry.notFound(k.copy())
                                : TableSegmentEntry.versioned(k.copy(), ev.value.copy(), ev.version);
                        result.add(e);
                    });
                    return result;
                }
            }, executorService());
        }

        @Override
        public AsyncIterator<IteratorItem<TableSegmentKey>> keyIterator(IteratorArgs args) {
            return getIterator(args, (key, value, ver) -> TableSegmentKey.versioned(key, ver));
        }

        @Override
        public AsyncIterator<IteratorItem<TableSegmentEntry>> entryIterator(IteratorArgs args) {
            return getIterator(args, TableSegmentEntry::versioned);
        }

        private <T> AsyncIterator<IteratorItem<T>> getIterator(IteratorArgs args, IteratorConverter<T> converter) {
            // The real Table Segment allows iterating while updating. Since we use HashMap, we don't have that luxury,
            // but we can take a snapshot now and iterate through that. This doesn't necessarily break the Table Segment
            // contract as it makes no guarantees about whether (or when) concurrent updates will make it into an ongoing
            // iteration.
            List<T> iteratorItems = getFilteredEntries(args.getKeyPrefixFilter(), converter);
            val position = new AtomicInteger(0);
            if (args.getState() != null) {
                position.set(args.getState().toBytes().getInt());
            }

            return () -> CompletableFuture.supplyAsync(() -> {
                if (position.get() >= iteratorItems.size()) {
                    return null;
                }
                int newPosition = Math.min(position.get() + args.getMaxItemsAtOnce(), iteratorItems.size());
                val result = iteratorItems.subList(position.get(), newPosition);
                position.set(newPosition);
                val newState = IteratorState.fromBytes(ByteBuffer.allocate(Integer.BYTES).putInt(0, newPosition));
                return new IteratorItem<>(newState, result);
            }, executorService());
        }

        private <T> List<T> getFilteredEntries(ByteBuf prefix, IteratorConverter<T> converter) {
            Assert.assertNotNull("Key Family iterations require a prefix.", prefix);
            AssertExtensions.assertGreaterThan("Key Family iterations require a prefix.",
                    KeyFamilySerializer.PREFIX_LENGTH, prefix.readableBytes());
            synchronized (this.data) {
                return this.data.entrySet().stream()
                        .filter(e -> startsWith(e.getKey(), prefix))
                        .map(e -> converter.apply(e.getKey().copy(), e.getValue().value.copy(), e.getValue().version))
                        .collect(Collectors.toList());
            }
        }

        private boolean startsWith(ByteBuf key, ByteBuf prefix) {

            return key.readableBytes() >= prefix.readableBytes()
                    && key.slice(0, prefix.readableBytes()).equals(prefix.duplicate());
        }

        @GuardedBy("data")
        @SneakyThrows(ConditionalTableUpdateException.class)
        private void checkVersion(TableSegmentKey key) {
            TableSegmentKeyVersion v = key.getVersion();
            if (v != null && !v.equals(TableSegmentKeyVersion.NO_VERSION)) {
                EntryValue existing = this.data.get(key.getKey());
                if (v.equals(TableSegmentKeyVersion.NOT_EXISTS)) {
                    if (existing != null) {
                        throw new BadKeyVersionException(this.segment.getScopedName());
                    }
                } else {
                    if (existing == null) {
                        throw new NoSuchKeyException(this.segment.getScopedName());
                    } else if (existing.version != key.getVersion().getSegmentVersion()) {
                        throw new BadKeyVersionException(this.segment.getScopedName());
                    }
                }
            }
        }

        @RequiredArgsConstructor
        private class EntryValue {
            final ByteBuf value;
            final long version;
        }
    }

    @FunctionalInterface
    private interface IteratorConverter<T> {
        T apply(ByteBuf key, ByteBuf value, Long version);
    }
}
