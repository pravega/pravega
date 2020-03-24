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

import com.google.common.collect.Iterators;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.pravega.client.admin.KeyValueTableInfo;
import io.pravega.client.control.impl.Controller;
import io.pravega.client.security.auth.DelegationTokenProviderFactory;
import io.pravega.client.stream.Serializer;
import io.pravega.client.tables.IteratorItem;
import io.pravega.client.tables.IteratorState;
import io.pravega.client.tables.KeyValueTable;
import io.pravega.client.tables.KeyVersion;
import io.pravega.client.tables.TableEntry;
import io.pravega.client.tables.TableKey;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.AsyncIterator;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import javax.annotation.Nullable;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

/**
 * Implementation for {@link KeyValueTable}.
 *
 * @param <KeyT>   Type for Key.
 * @param <ValueT> Type for Value.
 */
@Slf4j
public class KeyValueTableImpl<KeyT, ValueT> implements KeyValueTable<KeyT, ValueT>, AutoCloseable {
    //region Members

    private final Serializer<KeyT> keySerializer;
    private final Serializer<ValueT> valueSerializer;
    private final SegmentSelector selector;
    private final String logTraceId;
    private final AtomicBoolean closed;

    //endregion

    //region Constructor

    KeyValueTableImpl(@NonNull KeyValueTableInfo kvt, @NonNull TableSegmentFactory tableSegmentFactory, @NonNull Controller controller,
                      @NonNull Serializer<KeyT> keySerializer, @NonNull Serializer<ValueT> valueSerializer) {
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
        this.selector = new SegmentSelector(kvt, controller, tableSegmentFactory,
                DelegationTokenProviderFactory.create(controller, kvt.getScope(), kvt.getKeyValueTableName()));
        this.logTraceId = String.format("KeyValueTable[%s]", kvt.getScopedName());
        this.closed = new AtomicBoolean(false);
        log.info("{}: Initialized. SegmentCount={}.", this.logTraceId, this.selector.getSegmentCount());
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        if (this.closed.compareAndSet(false, true)) {
            // TODO close and enforce closing
            this.selector.close();
            log.info("{}: Closed.", this.logTraceId);
        }
    }

    //endregion

    //region KeyValueTable Implementation

    @Override
    public CompletableFuture<KeyVersion> put(@Nullable String keyFamily, @NonNull KeyT key, @NonNull ValueT value) {
        ByteBuf keySerialization = serializeKey(key);
        TableSegment s = this.selector.getTableSegment(keyFamily, keySerialization);
        return updateToSegment(s, toTableSegmentEntry(keySerialization, serializeValue(value), KeyVersion.NO_VERSION));
    }

    @Override
    public CompletableFuture<KeyVersion> putIfAbsent(@Nullable String keyFamily, @NonNull KeyT key, @NonNull ValueT value) {
        ByteBuf keySerialization = serializeKey(key);
        TableSegment s = this.selector.getTableSegment(keyFamily, keySerialization);
        return updateToSegment(s, toTableSegmentEntry(keySerialization, serializeValue(value), KeyVersion.NOT_EXISTS));
    }

    @Override
    public CompletableFuture<List<KeyVersion>> putAll(@NonNull String keyFamily, @NonNull Iterable<Map.Entry<KeyT, ValueT>> entries) {
        TableSegment s = this.selector.getTableSegment(keyFamily);
        return updateToSegment(s, toTableSegmentEntries(entries, e -> TableEntry.unversioned(e.getKey(), e.getValue())));
    }

    @Override
    public CompletableFuture<KeyVersion> replace(@Nullable String keyFamily, @NonNull KeyT key, @NonNull ValueT value, @NonNull KeyVersion version) {
        ByteBuf keySerialization = serializeKey(key);
        TableSegment s = this.selector.getTableSegment(keyFamily, keySerialization);
        return updateToSegment(s, toTableSegmentEntry(keySerialization, serializeValue(value), version));
    }

    @Override
    public CompletableFuture<List<KeyVersion>> replaceAll(@NonNull String keyFamily, @NonNull Iterable<TableEntry<KeyT, ValueT>> entries) {
        TableSegment s = this.selector.getTableSegment(keyFamily);
        return updateToSegment(s, toTableSegmentEntries(entries, e -> e));
    }

    @Override
    public CompletableFuture<Void> remove(@Nullable String keyFamily, @NonNull KeyT key) {
        ByteBuf keySerialization = serializeKey(key);
        TableSegment s = this.selector.getTableSegment(keyFamily, keySerialization);
        return removeFromSegment(s, Iterators.singletonIterator(TableSegmentKey.unversioned(keySerialization)));
    }

    @Override
    public CompletableFuture<Void> remove(@Nullable String keyFamily, @NonNull KeyT key, @NonNull KeyVersion version) {
        ByteBuf keySerialization = serializeKey(key);
        TableSegment s = this.selector.getTableSegment(keyFamily, keySerialization);
        return removeFromSegment(s, Iterators.singletonIterator(toTableSegmentKey(keySerialization, version)));
    }

    @Override
    public CompletableFuture<Void> removeAll(@Nullable String keyFamily, @NonNull Iterable<TableKey<KeyT>> keys) {
        TableSegment s = this.selector.getTableSegment(keyFamily);
        return removeFromSegment(s, toTableSegmentKeys(keys));
    }

    @Override
    public CompletableFuture<TableEntry<KeyT, ValueT>> get(@Nullable String keyFamily, @NonNull KeyT key) {
        return getAll(keyFamily, Collections.singleton(key))
                .thenApply(r -> r.get(0));
    }

    @Override
    public CompletableFuture<List<TableEntry<KeyT, ValueT>>> getAll(@Nullable String keyFamily, @NonNull Iterable<KeyT> keys) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        Iterator<ByteBuf> serializedKeys = StreamSupport.stream(keys.spliterator(), false)
                .map(this::serializeKey)
                .iterator();
        if (keyFamily == null) {
            // We are dealing with multiple segments.
            return getFromMultiSegments(serializedKeys);
        } else {
            // Everything goes into a single segment.
            TableSegment s = this.selector.getTableSegment(keyFamily);
            return getFromSingleSegment(s, serializedKeys);
        }
    }

    private static class KeyGroup {
        final ArrayList<ByteBuf> keys = new ArrayList<>();
        final ArrayList<Integer> ordinals = new ArrayList<>();

        void add(ByteBuf key, int ordinal) {
            this.keys.add(key);
            this.ordinals.add(ordinal);
        }
    }

    @Override
    public AsyncIterator<IteratorItem<TableKey<KeyT>>> keyIterator(@NonNull String keyFamily, int maxKeysAtOnce, @Nullable IteratorState state) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        throw new UnsupportedOperationException(); // TODO
    }

    @Override
    public AsyncIterator<IteratorItem<TableEntry<KeyT, ValueT>>> entryIterator(@NonNull String keyFamily, int maxEntriesAtOnce, @Nullable IteratorState state) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        throw new UnsupportedOperationException(""); // TODO
    }

    //endregion

    //region Helpers

    private CompletableFuture<KeyVersion> updateToSegment(TableSegment segment, TableSegmentEntry tableSegmentEntry) {
        return updateToSegment(segment, Iterators.singletonIterator(tableSegmentEntry)).thenApply(r -> r.get(0));
    }

    private CompletableFuture<List<KeyVersion>> updateToSegment(TableSegment segment, Iterator<TableSegmentEntry> tableSegmentEntries) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        return segment.put(tableSegmentEntries)
                .thenApply(versions -> versions.stream().map(v -> new KeyVersionImpl(segment.getSegmentName(), v)).collect(Collectors.toList()));
    }

    private CompletableFuture<Void> removeFromSegment(TableSegment segment, Iterator<TableSegmentKey> tableSegmentKeys) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        return segment.remove(tableSegmentKeys);
    }

    @SuppressWarnings("unchecked")
    private CompletableFuture<List<TableEntry<KeyT, ValueT>>> getFromMultiSegments(Iterator<ByteBuf> keys) {
        val bySegment = new HashMap<TableSegment, KeyGroup>();
        val count = new AtomicInteger(0);
        keys.forEachRemaining(k -> {
            TableSegment ts = this.selector.getTableSegment(null, k);
            KeyGroup g = bySegment.computeIfAbsent(ts, t -> new KeyGroup());
            g.add(k, count.getAndIncrement());
        });

        val futures = new HashMap<TableSegment, CompletableFuture<List<TableSegmentEntry>>>();
        bySegment.forEach((ts, kg) -> futures.put(ts, ts.get(kg.keys.iterator())));
        return Futures.allOf(futures.values())
                .thenApply(v -> {
                    val r = new TableEntry[count.get()];
                    futures.forEach((ts, f) -> {
                        KeyGroup kg = bySegment.get(ts);
                        List<TableSegmentEntry> segmentResult = f.join();
                        assert segmentResult.size() == kg.ordinals.size();
                        for (int i = 0; i < kg.ordinals.size(); i++) {
                            r[kg.ordinals.get(i)] = fromTableSegmentEntry(ts, segmentResult.get(i));
                        }
                    });
                    return Arrays.asList(r);
                });
    }

    private CompletableFuture<List<TableEntry<KeyT, ValueT>>> getFromSingleSegment(TableSegment s, Iterator<ByteBuf> keys) {
        return s.get(keys).thenApply(entries -> entries.stream().map(e -> fromTableSegmentEntry(s, e)).collect(Collectors.toList()));
    }

    private Iterator<TableSegmentKey> toTableSegmentKeys(Iterable<TableKey<KeyT>> keys) {
        return StreamSupport.stream(keys.spliterator(), false)
                .map(k -> toTableSegmentKey(serializeKey(k.getKey()), k.getVersion()))
                .iterator();
    }

    private TableSegmentKey toTableSegmentKey(ByteBuf key, KeyVersion keyVersion) {
        // TODO: validate KeyVersion.getSegmentName
        return new TableSegmentKey(key, toTableSegmentVersion(keyVersion));
    }

    private <T> Iterator<TableSegmentEntry> toTableSegmentEntries(Iterable<T> entries, Function<T, TableEntry<KeyT, ValueT>> getEntry) {
        return StreamSupport.stream(entries.spliterator(), false)
                .map(e -> {
                    TableEntry<KeyT, ValueT> entry = getEntry.apply(e);
                    TableKey<KeyT> key = entry.getKey();
                    return toTableSegmentEntry(serializeKey(key.getKey()), serializeValue(entry.getValue()), key.getVersion());
                })
                .iterator();
    }

    private TableSegmentEntry toTableSegmentEntry(ByteBuf key, ByteBuf value, KeyVersion keyVersion) {
        // TODO: validate KeyVersion.getSegmentName
        return new TableSegmentEntry(toTableSegmentKey(key, keyVersion), value);
    }

    private TableSegmentKeyVersion toTableSegmentVersion(KeyVersion version) {
        return version == null ? TableSegmentKeyVersion.NO_VERSION : TableSegmentKeyVersion.from(version.asImpl().getSegmentVersion());
    }

    private TableEntry<KeyT, ValueT> fromTableSegmentEntry(TableSegment s, TableSegmentEntry e) {
        KeyT key = deserializeKey(e.getKey().getKey());
        ValueT value = deserializeValue(e.getValue());
        KeyVersion version = new KeyVersionImpl(s.getSegmentName(), e.getKey().getVersion());
        return TableEntry.versioned(key, version, value);
    }

    private ByteBuf serializeKey(KeyT k) {
        return Unpooled.wrappedBuffer(this.keySerializer.serialize(k));
    }

    private KeyT deserializeKey(ByteBuf s) {
        KeyT result = this.keySerializer.deserialize(s.nioBuffer());
        s.release();
        return result;
    }

    private ByteBuf serializeValue(ValueT v) {
        return Unpooled.wrappedBuffer(this.valueSerializer.serialize(v));
    }

    private ValueT deserializeValue(ByteBuf s) {
        ValueT result = this.valueSerializer.deserialize(s.nioBuffer());
        s.release();
        return result;
    }

    //endregion
}
