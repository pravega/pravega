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
package io.pravega.client.tables.impl;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.pravega.client.admin.KeyValueTableInfo;
import io.pravega.client.control.impl.Controller;
import io.pravega.client.tables.BadKeyVersionException;
import io.pravega.client.tables.ConditionalTableUpdateException;
import io.pravega.client.tables.KeyValueTable;
import io.pravega.client.tables.KeyValueTableConfiguration;
import io.pravega.client.tables.Put;
import io.pravega.client.tables.Remove;
import io.pravega.client.tables.TableEntry;
import io.pravega.client.tables.TableEntryUpdate;
import io.pravega.client.tables.TableKey;
import io.pravega.client.tables.TableModification;
import io.pravega.client.tables.Version;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.Data;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

/**
 * Implementation for {@link KeyValueTable}.
 */
@Slf4j
public class KeyValueTableImpl implements KeyValueTable, AutoCloseable {
    //region Members

    private final KeyValueTableInfo kvt;
    private final SegmentSelector selector;
    private final String logTraceId;
    private final AtomicBoolean closed;
    private final KeyValueTableConfiguration config;
    private final int totalKeyLength;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the {@link KeyValueTableImpl} class.
     *
     * @param kvt                 A {@link KeyValueTableInfo} containing information about the Key-Value Table.
     * @param config              A {@link KeyValueTableConfiguration} representing the config for the Key-Value Table.
     * @param tableSegmentFactory Factory to create {@link TableSegment} instances.
     * @param controller          Controller client.
     */
    KeyValueTableImpl(@NonNull KeyValueTableInfo kvt, @NonNull KeyValueTableConfiguration config,
                      @NonNull TableSegmentFactory tableSegmentFactory, @NonNull Controller controller) {
        this.kvt = kvt;
        this.config = config;
        this.selector = new SegmentSelector(this.kvt, controller, tableSegmentFactory);
        this.logTraceId = String.format("KeyValueTable[%s]", this.kvt.getScopedName());
        this.closed = new AtomicBoolean(false);
        Preconditions.checkArgument(config.getPartitionCount() == this.selector.getSegmentCount(),
                "Inconsistent Segment Count. Expected %s, actual %s.", config.getPartitionCount(), this.selector.getSegmentCount());
        log.info("{}: Initialized. Config: {}.", this.logTraceId, this.config);
        this.totalKeyLength = this.config.getPrimaryKeyLength() + this.config.getSecondaryKeyLength();
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
    public CompletableFuture<Version> update(@NonNull TableModification update) {
        val s = this.selector.getTableSegment(update.getKey().getPrimaryKey());
        if (update.isRemoval()) {
            val removeArgs = new UpdateArg<TableSegmentKey>(update.getKey().getPrimaryKey(), s, Iterators.singletonIterator(toTableSegmentKey(s, (Remove) update)));
            return removeFromSegment(removeArgs.getTableSegment(), removeArgs.getAllArgs()).thenApply(r -> null);
        } else {
            val updateArgs = new UpdateArg<>(update.getKey().getPrimaryKey(), s, Iterators.singletonIterator(toTableSegmentEntry(s, (TableEntryUpdate) update)));
            return updateToSegment(updateArgs.getTableSegment(), updateArgs.getAllArgs()).thenApply(r -> r.get(0));
        }
    }

    @Override
    public CompletableFuture<List<Version>> update(@NonNull Iterable<TableModification> updates) {
        val inputIterator = updates.iterator();
        if (!inputIterator.hasNext()) {
            // Empty input - nothing to do.
            return CompletableFuture.completedFuture(Collections.emptyList());
        }

        val firstInput = inputIterator.next();
        val ts = this.selector.getTableSegment(firstInput.getKey().getPrimaryKey());
        if (firstInput.isRemoval()) {
            val args = toArg(firstInput, inputIterator, ts, u -> toTableSegmentKey(ts, (Remove) u));
            return removeFromSegment(args.getTableSegment(), args.getAllArgs()).thenApply(r -> Collections.emptyList());
        } else {
            val args = toArg(firstInput, inputIterator, ts, u -> toTableSegmentEntry(ts, (TableEntryUpdate) u));
            return updateToSegment(args.getTableSegment(), args.getAllArgs());
        }
    }

    @Override
    public CompletableFuture<Boolean> exists(@NonNull TableKey key) {
        // We attempt a removal conditioned on the key not existing (no-op if key actual exists). This is preferred to
        // using get(key) because get() will also attempt to read and return the value (of no use in this case).
        return update(new Remove(key, Version.NOT_EXISTS))
                .handle((r, ex) -> {
                    if (ex != null) {
                        if (ex instanceof ConditionalTableUpdateException) {
                            return true;
                        } else {
                            throw new CompletionException(ex);
                        }
                    }
                    return false;
                });
    }

    @Override
    public CompletableFuture<TableEntry> get(@NonNull TableKey key) {
        return getAll(Collections.singleton(key))
                .thenApply(r -> r.get(0));
    }

    @Override
    public CompletableFuture<List<TableEntry>> getAll(@NonNull Iterable<TableKey> keys) {
        Exceptions.checkNotClosed(this.closed.get(), this);

        val bySegment = new HashMap<TableSegment, KeyGroup>();
        val count = new AtomicInteger(0);
        keys.forEach(k -> {
            TableSegment ts = this.selector.getTableSegment(k.getPrimaryKey());
            KeyGroup g = bySegment.computeIfAbsent(ts, t -> new KeyGroup());
            g.add(serializeKey(k), count.getAndIncrement());
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
                            r[kg.ordinals.get(i)] = fromTableSegmentEntry(ts, segmentResult.get(i));
                        }
                    });
                    return Arrays.asList(r);
                });
    }

    @Override
    public KeyValueTableIteratorImpl.Builder iterator() {
        Exceptions.checkNotClosed(this.closed.get(), this);
        return new KeyValueTableIteratorImpl.Builder(this.config);
    }

    //endregion

    //region Helpers

    private <T> UpdateArg<T> toArg(TableModification firstInput, Iterator<TableModification> inputIterator, TableSegment ts,
                                   Function<TableModification, T> convert) {
        val firstInputIterator = Iterators.singletonIterator(convert.apply(firstInput));
        val restIterator = Iterators.transform(inputIterator, i -> {
            val pk = i.getKey().getPrimaryKey();
            Preconditions.checkArgument(firstInput.getKey().getPrimaryKey().equals(pk), "All Keys must have the same Primary Key.");
            Preconditions.checkArgument(firstInput.isRemoval() == i.isRemoval(), "Cannot combine Removals with Updates.");
            return convert.apply(i);
        });

        return new UpdateArg<T>(firstInput.getKey().getPrimaryKey(), ts, Iterators.concat(firstInputIterator, restIterator));
    }

    private CompletableFuture<List<Version>> updateToSegment(TableSegment segment, Iterator<TableSegmentEntry> tableSegmentEntries) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        return segment.put(tableSegmentEntries)
                .thenApply(versions -> versions.stream().map(v -> new VersionImpl(segment.getSegmentId(), v)).collect(Collectors.toList()));
    }

    private CompletableFuture<Void> removeFromSegment(TableSegment segment, Iterator<TableSegmentKey> tableSegmentKeys) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        return segment.remove(tableSegmentKeys);
    }

    private TableSegmentKey toTableSegmentKey(ByteBuf key, Version keyVersion) {
        return new TableSegmentKey(key, toTableSegmentVersion(keyVersion));
    }

    private TableSegmentKey toTableSegmentKey(TableSegment tableSegment, Remove removal) {
        validateKeyVersionSegment(tableSegment, removal.getVersion());
        return toTableSegmentKey(serializeKey(removal.getKey()), removal.getVersion());
    }

    private TableSegmentEntry toTableSegmentEntry(TableSegment tableSegment, TableEntryUpdate update) {
        TableKey key = update.getKey();
        if (update instanceof Put) {
            validateKeyVersionSegment(tableSegment, update.getVersion());
        }
        return new TableSegmentEntry(toTableSegmentKey(serializeKey(key), update.getVersion()), serializeValue(update.getValue()));
    }

    private TableSegmentKeyVersion toTableSegmentVersion(Version version) {
        return version == null ? TableSegmentKeyVersion.NO_VERSION : TableSegmentKeyVersion.from(version.asImpl().getSegmentVersion());
    }

    private TableEntry fromTableSegmentEntry(TableSegment s, TableSegmentEntry e) {
        if (e == null) {
            return null;
        }

        DeserializedKey rawKey = deserializeKey(e.getKey().getKey());
        Version version = new VersionImpl(s.getSegmentId(), e.getKey().getVersion());
        TableKey key = new TableKey(rawKey.primaryKey, rawKey.secondaryKey);
        ByteBuffer value = deserializeValue(e.getValue());
        return new TableEntry(key, version, value);
    }

    private ByteBuf serializeKey(TableKey k) {
        Preconditions.checkArgument(k.getPrimaryKey().remaining() == this.config.getPrimaryKeyLength(),
                "Invalid Primary Key Length. Expected %s, actual %s.", this.config.getPrimaryKeyLength(), k.getPrimaryKey().remaining());
        if (this.config.getSecondaryKeyLength() == 0) {
            Preconditions.checkArgument(k.getSecondaryKey() == null || k.getSecondaryKey().remaining() == this.config.getSecondaryKeyLength(),
                    "Not expecting a Secondary Key.");
            return Unpooled.wrappedBuffer(k.getPrimaryKey());
        } else {
            Preconditions.checkArgument(k.getSecondaryKey().remaining() == this.config.getSecondaryKeyLength(),
                    "Invalid Secondary Key Length. Expected %s, actual %s.", this.config.getSecondaryKeyLength(), k.getSecondaryKey().remaining());
            return Unpooled.wrappedBuffer(k.getPrimaryKey(), k.getSecondaryKey());
        }
    }

    private DeserializedKey deserializeKey(ByteBuf keySerialization) {
        Preconditions.checkArgument(keySerialization.readableBytes() == this.totalKeyLength,
                "Unexpected key length read back. Expected %s, found %s.", this.totalKeyLength, keySerialization.readableBytes());
        val pk = keySerialization.slice(0, this.config.getPrimaryKeyLength()).copy().nioBuffer();
        val sk = keySerialization.slice(this.config.getPrimaryKeyLength(), this.config.getSecondaryKeyLength()).copy().nioBuffer();
        keySerialization.release(); // Safe to do so now - we made copies of the original buffer.
        return new DeserializedKey(pk, sk);
    }

    private ByteBuf serializeValue(ByteBuffer v) {
        Preconditions.checkArgument(v.remaining() <= KeyValueTable.MAXIMUM_VALUE_LENGTH,
                "Value Too Long. Expected at most %s, actual %s.", KeyValueTable.MAXIMUM_VALUE_LENGTH, v.remaining());
        return Unpooled.wrappedBuffer(v);
    }

    private ByteBuffer deserializeValue(ByteBuf s) {
        val result = s.copy().nioBuffer();
        s.release();
        return result;
    }

    @SneakyThrows(BadKeyVersionException.class)
    private void validateKeyVersionSegment(TableSegment ts, Version version) {
        if (version == null) {
            return;
        }

        VersionImpl impl = version.asImpl();
        boolean valid = impl.getSegmentId() == VersionImpl.NO_SEGMENT_ID || ts.getSegmentId() == impl.getSegmentId();
        if (!valid) {
            throw new BadKeyVersionException(this.kvt.getScopedName(), "Wrong TableSegment.");
        }
    }

    //endregion

    //region Helper classes

    @RequiredArgsConstructor
    private static class DeserializedKey {
        final ByteBuffer primaryKey;
        final ByteBuffer secondaryKey;
    }

    private static class KeyGroup {
        final ArrayList<ByteBuf> keys = new ArrayList<>();
        final ArrayList<Integer> ordinals = new ArrayList<>();

        void add(ByteBuf key, int ordinal) {
            this.keys.add(key);
            this.ordinals.add(ordinal);
        }
    }

    @Data
    private static class UpdateArg<T> {
        private final ByteBuffer primaryKey;
        private final TableSegment tableSegment;
        private final Iterator<T> allArgs;
    }

    //endregion
}
