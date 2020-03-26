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

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.pravega.client.admin.KeyValueTableInfo;
import io.pravega.client.control.impl.Controller;
import io.pravega.client.stream.Serializer;
import io.pravega.client.tables.BadKeyVersionException;
import io.pravega.client.tables.IteratorItem;
import io.pravega.client.tables.IteratorState;
import io.pravega.client.tables.KeyValueTable;
import io.pravega.client.tables.KeyVersion;
import io.pravega.client.tables.TableEntry;
import io.pravega.client.tables.TableKey;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.AsyncIterator;
import java.nio.ByteBuffer;
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
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.commons.lang3.SerializationException;

/**
 * Implementation for {@link KeyValueTable}.
 *
 * @param <KeyT>   Type for Key.
 * @param <ValueT> Type for Value.
 */
@Slf4j
public class KeyValueTableImpl<KeyT, ValueT> implements KeyValueTable<KeyT, ValueT>, AutoCloseable {
    //region Members

    private static final Serializer<String> KEY_FAMILY_SERIALIZER = new KeyFamilySerializer();
    private final KeyValueTableInfo kvt;
    private final Serializer<KeyT> keySerializer;
    private final Serializer<ValueT> valueSerializer;
    private final SegmentSelector selector;
    private final String logTraceId;
    private final AtomicBoolean closed;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the {@link KeyValueTableImpl} class.
     *
     * @param kvt                 A {@link KeyValueTableInfo} containing information about the Key-Value Table.
     * @param tableSegmentFactory Factory to create {@link TableSegment} instances.
     * @param controller          Controller client.
     * @param keySerializer       Serializer for keys.
     * @param valueSerializer     Serializer for values.
     */
    KeyValueTableImpl(@NonNull KeyValueTableInfo kvt, @NonNull TableSegmentFactory tableSegmentFactory, @NonNull Controller controller,
                      @NonNull Serializer<KeyT> keySerializer, @NonNull Serializer<ValueT> valueSerializer) {
        this.kvt = kvt;
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
        this.selector = new SegmentSelector(this.kvt, controller, tableSegmentFactory);
        this.logTraceId = String.format("KeyValueTable[%s]", this.kvt.getScopedName());
        this.closed = new AtomicBoolean(false);
        log.info("{}: Initialized. SegmentCount={}.", this.logTraceId, this.selector.getSegmentCount());
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        if (this.closed.compareAndSet(false, true)) {
            this.selector.close();
            log.info("{}: Closed.", this.logTraceId);
        }
    }

    //endregion

    //region KeyValueTable Implementation

    @Override
    public CompletableFuture<KeyVersion> put(@Nullable String keyFamily, @NonNull KeyT key, @NonNull ValueT value) {
        ByteBuf keySerialization = serializeKey(keyFamily, key);
        TableSegment s = this.selector.getTableSegment(keyFamily, keySerialization);
        return updateToSegment(s, toTableSegmentEntry(keySerialization, serializeValue(value), KeyVersion.NO_VERSION));
    }

    @Override
    public CompletableFuture<KeyVersion> putIfAbsent(@Nullable String keyFamily, @NonNull KeyT key, @NonNull ValueT value) {
        ByteBuf keySerialization = serializeKey(keyFamily, key);
        TableSegment s = this.selector.getTableSegment(keyFamily, keySerialization);
        return updateToSegment(s, toTableSegmentEntry(keySerialization, serializeValue(value), KeyVersion.NOT_EXISTS));
    }

    @Override
    public CompletableFuture<List<KeyVersion>> putAll(@NonNull String keyFamily, @NonNull Iterable<Map.Entry<KeyT, ValueT>> entries) {
        TableSegment s = this.selector.getTableSegment(keyFamily);
        return updateToSegment(s, toTableSegmentEntries(s, keyFamily, entries, e -> TableEntry.unversioned(e.getKey(), e.getValue())));
    }

    @Override
    public CompletableFuture<KeyVersion> replace(@Nullable String keyFamily, @NonNull KeyT key, @NonNull ValueT value,
                                                 @NonNull KeyVersion version) {
        ByteBuf keySerialization = serializeKey(keyFamily, key);
        TableSegment s = this.selector.getTableSegment(keyFamily, keySerialization);
        validateKeyVersionSegment(s, version);
        return updateToSegment(s, toTableSegmentEntry(keySerialization, serializeValue(value), version));
    }

    @Override
    public CompletableFuture<List<KeyVersion>> replaceAll(@NonNull String keyFamily, @NonNull Iterable<TableEntry<KeyT, ValueT>> entries) {
        TableSegment s = this.selector.getTableSegment(keyFamily);
        return updateToSegment(s, toTableSegmentEntries(s, keyFamily, entries, e -> e));
    }

    @Override
    public CompletableFuture<Void> remove(@Nullable String keyFamily, @NonNull KeyT key) {
        ByteBuf keySerialization = serializeKey(keyFamily, key);
        TableSegment s = this.selector.getTableSegment(keyFamily, keySerialization);
        return removeFromSegment(s, Iterators.singletonIterator(toTableSegmentKey(keySerialization, KeyVersion.NO_VERSION)));
    }

    @Override
    public CompletableFuture<Void> remove(@Nullable String keyFamily, @NonNull KeyT key, @NonNull KeyVersion version) {
        ByteBuf keySerialization = serializeKey(keyFamily, key);
        TableSegment s = this.selector.getTableSegment(keyFamily, keySerialization);
        validateKeyVersionSegment(s, version);
        return removeFromSegment(s, Iterators.singletonIterator(toTableSegmentKey(keySerialization, version)));
    }

    @Override
    public CompletableFuture<Void> removeAll(@NonNull String keyFamily, @NonNull Iterable<TableKey<KeyT>> keys) {
        TableSegment s = this.selector.getTableSegment(keyFamily);
        return removeFromSegment(s, toTableSegmentKeys(s, keyFamily, keys));
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
                .map(k -> serializeKey(keyFamily, k))
                .iterator();
        if (keyFamily == null) {
            // We are dealing with multiple segments.
            return getFromMultiSegments(serializedKeys);
        } else {
            // Everything goes into a single segment.
            TableSegment s = this.selector.getTableSegment(keyFamily);
            return getFromSingleSegment(s, serializedKeys, keyFamily);
        }
    }

    @Override
    public AsyncIterator<IteratorItem<TableKey<KeyT>>> keyIterator(@NonNull String keyFamily, int maxKeysAtOnce,
                                                                   @Nullable IteratorState state) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        TableSegment ts = this.selector.getTableSegment(keyFamily);
        IteratorArgs args = getIteratorArgs(ts, keyFamily, maxKeysAtOnce, state);
        return ts.keyIterator(args).thenApply(si -> fromSegmentIteratorItem(ts, keyFamily, si, this::fromTableSegmentKey));
    }

    @Override
    public AsyncIterator<IteratorItem<TableEntry<KeyT, ValueT>>> entryIterator(@NonNull String keyFamily, int maxEntriesAtOnce,
                                                                               @Nullable IteratorState state) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        TableSegment ts = this.selector.getTableSegment(keyFamily);
        IteratorArgs args = getIteratorArgs(ts, keyFamily, maxEntriesAtOnce, state);
        return ts.entryIterator(args).thenApply(si -> fromSegmentIteratorItem(ts, keyFamily, si, this::fromTableSegmentEntry));
    }

    //endregion

    //region Helpers

    private CompletableFuture<KeyVersion> updateToSegment(TableSegment segment, TableSegmentEntry tableSegmentEntry) {
        return updateToSegment(segment, Iterators.singletonIterator(tableSegmentEntry)).thenApply(r -> r.get(0));
    }

    private CompletableFuture<List<KeyVersion>> updateToSegment(TableSegment segment, Iterator<TableSegmentEntry> tableSegmentEntries) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        return segment.put(tableSegmentEntries)
                .thenApply(versions -> versions.stream().map(v -> new KeyVersionImpl(segment.getSegmentId(), v)).collect(Collectors.toList()));
    }

    private CompletableFuture<Void> removeFromSegment(TableSegment segment, Iterator<TableSegmentKey> tableSegmentKeys) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        return segment.remove(tableSegmentKeys);
    }

    @SuppressWarnings("unchecked")
    private CompletableFuture<List<TableEntry<KeyT, ValueT>>> getFromMultiSegments(Iterator<ByteBuf> serializedKeys) {
        val bySegment = new HashMap<TableSegment, KeyGroup>();
        val count = new AtomicInteger(0);
        serializedKeys.forEachRemaining(k -> {
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
                        assert f.isDone() : "incomplete CompletableFuture returned by Futures.allOf";
                        val segmentResult = f.join();
                        assert segmentResult.size() == kg.ordinals.size() : "segmentResult count mismatch";
                        for (int i = 0; i < kg.ordinals.size(); i++) {
                            assert r[kg.ordinals.get(i)] == null : "overlapping ordinals";
                            r[kg.ordinals.get(i)] = fromTableSegmentEntry(ts, segmentResult.get(i), null);
                        }
                    });
                    return Arrays.asList(r);
                });
    }

    private CompletableFuture<List<TableEntry<KeyT, ValueT>>> getFromSingleSegment(TableSegment s, Iterator<ByteBuf> serializedKeys,
                                                                                   String expectedKeyFamily) {
        return s.get(serializedKeys)
                .thenApply(entries -> entries.stream().map(e -> fromTableSegmentEntry(s, e, expectedKeyFamily)).collect(Collectors.toList()));
    }

    private IteratorArgs getIteratorArgs(TableSegment ts, String keyFamily, int maxItemsAtOnce, IteratorState state) {
        IteratorState segmentIteratorState = null;
        if (state != null) {
            val kvtState = KeyValueTableIteratorState.fromBytes(state.toBytes());
            Preconditions.checkArgument(this.kvt.getScopedName().equals(kvtState.getKeyValueTableName()),
                    "IteratorState refers to a different Key-ValueTable (%s) than this one (%s).",
                    kvtState.getKeyValueTableName(), this.kvt.getScopedName());
            Preconditions.checkArgument(ts.getSegmentId() == kvtState.getSegmentId(),
                    "IteratorState refers to a different Segment (%s) than assigned to KeyFamily '%s' (%s).",
                    kvtState.getSegmentId(), keyFamily, ts.getSegmentId());
            segmentIteratorState = IteratorStateImpl.fromBytes(kvtState.getSegmentIteratorState());
        }

        return IteratorArgs.builder()
                .keyPrefixFilter(Unpooled.wrappedBuffer(KEY_FAMILY_SERIALIZER.serialize(keyFamily)))
                .maxItemsAtOnce(maxItemsAtOnce)
                .state(segmentIteratorState)
                .build();
    }

    private <T, V> IteratorItem<V> fromSegmentIteratorItem(TableSegment ts, String keyFamily, IteratorItem<T> segmentIteratorItem,
                                                           SegmentItemConverter<T, V> converter) {
        IteratorState segmentState = segmentIteratorItem.getState();
        IteratorState state;
        if (segmentState.isEmpty()) {
            state = IteratorStateImpl.EMPTY;
        } else {
            val kvtState = new KeyValueTableIteratorState(this.kvt.getScopedName(), ts.getSegmentId(), Unpooled.wrappedBuffer(segmentState.toBytes()));
            state = IteratorStateImpl.fromBytes(kvtState.toBytes());
        }

        val items = segmentIteratorItem.getItems().stream().map(k -> converter.apply(ts, k, keyFamily)).collect(Collectors.toList());
        return new IteratorItem<>(state, items);
    }

    private Iterator<TableSegmentKey> toTableSegmentKeys(TableSegment tableSegment, String keyFamily, Iterable<TableKey<KeyT>> keys) {
        return StreamSupport.stream(keys.spliterator(), false)
                .map(k -> {
                    validateKeyVersionSegment(tableSegment, k.getVersion());
                    return toTableSegmentKey(serializeKey(keyFamily, k.getKey()), k.getVersion());
                })
                .iterator();
    }

    private TableSegmentKey toTableSegmentKey(ByteBuf key, KeyVersion keyVersion) {
        return new TableSegmentKey(key, toTableSegmentVersion(keyVersion));
    }

    private <T> Iterator<TableSegmentEntry> toTableSegmentEntries(TableSegment tableSegment, String keyFamily, Iterable<T> entries,
                                                                  Function<T, TableEntry<KeyT, ValueT>> getEntry) {
        return StreamSupport.stream(entries.spliterator(), false)
                .map(e -> {
                    TableEntry<KeyT, ValueT> entry = getEntry.apply(e);
                    TableKey<KeyT> key = entry.getKey();
                    validateKeyVersionSegment(tableSegment, key.getVersion());
                    return toTableSegmentEntry(serializeKey(keyFamily, key.getKey()), serializeValue(entry.getValue()), key.getVersion());
                })
                .iterator();
    }

    private TableSegmentEntry toTableSegmentEntry(ByteBuf keySerialization, ByteBuf valueSerialization, KeyVersion keyVersion) {
        return new TableSegmentEntry(toTableSegmentKey(keySerialization, keyVersion), valueSerialization);
    }

    private TableSegmentKeyVersion toTableSegmentVersion(KeyVersion version) {
        return version == null ? TableSegmentKeyVersion.NO_VERSION : TableSegmentKeyVersion.from(version.asImpl().getSegmentVersion());
    }

    private TableEntry<KeyT, ValueT> fromTableSegmentEntry(TableSegment s, TableSegmentEntry e, String expectedKeyFamily) {
        TableKey<KeyT> segmentKey = fromTableSegmentKey(s, e.getKey(), expectedKeyFamily);
        ValueT value = e.getValue() == null ? null : deserializeValue(e.getValue());
        assert value != null ^ e.getKey().getVersion().equals(TableSegmentKeyVersion.NOT_EXISTS)
                : "Illegal value-version combination. Value = '" + value + "'; Version = " + e.getKey().getVersion();
        return TableEntry.versioned(segmentKey.getKey(), segmentKey.getVersion(), value);
    }

    private TableKey<KeyT> fromTableSegmentKey(TableSegment s, TableSegmentKey tableSegmentKey, String expectedKeyFamily) {
        DeserializedKey key = deserializeKey(tableSegmentKey.getKey());
        validateKeyFamily(expectedKeyFamily, key.keyFamily);
        KeyVersion version = new KeyVersionImpl(s.getSegmentId(), tableSegmentKey.getVersion());
        return TableKey.versioned(key.key, version);
    }

    private ByteBuf serializeKey(String keyFamily, KeyT k) {
        ByteBuf keyFamilySerialization = Unpooled.wrappedBuffer(KEY_FAMILY_SERIALIZER.serialize(keyFamily));
        ByteBuf keySerialization = Unpooled.wrappedBuffer(this.keySerializer.serialize(k));
        return Unpooled.wrappedBuffer(keyFamilySerialization, keySerialization);
    }

    private DeserializedKey deserializeKey(ByteBuf keySerialization) {
        ByteBuffer buf = keySerialization.nioBuffer();
        String keyFamily = KEY_FAMILY_SERIALIZER.deserialize(buf);
        KeyT key = this.keySerializer.deserialize(buf);
        keySerialization.release();
        return new DeserializedKey(key, keyFamily);
    }

    private ByteBuf serializeValue(ValueT v) {
        return Unpooled.wrappedBuffer(this.valueSerializer.serialize(v));
    }

    private ValueT deserializeValue(ByteBuf s) {
        ValueT result = this.valueSerializer.deserialize(s.nioBuffer());
        s.release();
        return result;
    }

    private void validateKeyFamily(String expected, String actual) {
        boolean valid = (expected == null && actual == null) || (expected != null && expected.equals(actual));
        if (!valid) {
            throw new SerializationException(String.format(
                    "Unexpected Key Family deserialized. Expected '%s', actual '%s'.", expected, actual));
        }
    }

    @SneakyThrows(BadKeyVersionException.class)
    private void validateKeyVersionSegment(TableSegment ts, KeyVersion version) {
        if (version == null) {
            return;
        }

        KeyVersionImpl impl = version.asImpl();
        boolean valid = impl.getSegmentId() == KeyVersionImpl.NO_SEGMENT_ID || ts.getSegmentId() == impl.getSegmentId();
        if (!valid) {
            throw new BadKeyVersionException(this.kvt.getScopedName(), "Wrong TableSegment.");
        }
    }

    //endregion

    //region Helper classes

    @RequiredArgsConstructor
    private class DeserializedKey {
        final KeyT key;
        final String keyFamily;
    }

    private static class KeyGroup {
        final ArrayList<ByteBuf> keys = new ArrayList<>();
        final ArrayList<Integer> ordinals = new ArrayList<>();

        void add(ByteBuf key, int ordinal) {
            this.keys.add(key);
            this.ordinals.add(ordinal);
        }
    }

    @FunctionalInterface
    private interface SegmentItemConverter<SegmentItemType, TableItemType> {
        TableItemType apply(TableSegment ts, SegmentItemType item, String keyFamily);
    }

    //endregion
}
