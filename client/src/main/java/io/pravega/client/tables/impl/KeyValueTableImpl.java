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
import io.pravega.client.admin.KeyValueTableInfo;
import io.pravega.client.control.impl.Controller;
import io.pravega.client.tables.ConditionalTableUpdateException;
import io.pravega.client.tables.KeyValueTable;
import io.pravega.client.tables.KeyValueTableConfiguration;
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
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.Data;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

/**
 * Implementation for {@link KeyValueTable}.
 */
@Slf4j
public class KeyValueTableImpl implements KeyValueTable, AutoCloseable {
    //region Members

    private final SegmentSelector selector;
    private final String logTraceId;
    private final AtomicBoolean closed;
    private final KeyValueTableConfiguration config;
    private final TableEntryHelper entryHelper;
    private final Executor executor;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the {@link KeyValueTableImpl} class.
     *
     * @param kvt                 A {@link KeyValueTableInfo} containing information about the Key-Value Table.
     * @param tableSegmentFactory Factory to create {@link TableSegment} instances.
     * @param controller          Controller client.
     * @param executor            An Executor for async operations.
     */
    KeyValueTableImpl(@NonNull KeyValueTableInfo kvt, @NonNull TableSegmentFactory tableSegmentFactory,
                      @NonNull Controller controller, @NonNull Executor executor) {
        this.executor = executor;
        this.selector = new SegmentSelector(kvt, controller, tableSegmentFactory);
        this.config = getConfig(kvt, controller);
        this.entryHelper = new TableEntryHelper(this.selector, this.config);
        this.logTraceId = String.format("KeyValueTable[%s]", kvt.getScopedName());
        this.closed = new AtomicBoolean(false);
        Preconditions.checkArgument(config.getPartitionCount() == this.selector.getSegmentCount(),
                "Inconsistent Segment Count. Expected %s, actual %s.", config.getPartitionCount(), this.selector.getSegmentCount());
        log.info("{}: Initialized. Config: {}.", this.logTraceId, this.config);
    }

    private KeyValueTableConfiguration getConfig(KeyValueTableInfo kvt, Controller controller) {
        return Futures.getAndHandleExceptions(
                controller.getKeyValueTableConfiguration(kvt.getScope(), kvt.getKeyValueTableName()),
                RuntimeException::new);
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
            val removeArgs = new UpdateArg<TableSegmentKey>(update.getKey().getPrimaryKey(), s,
                    Iterators.singletonIterator(this.entryHelper.toTableSegmentKey(s, (Remove) update)));
            return removeFromSegment(removeArgs.getTableSegment(), removeArgs.getAllArgs()).thenApply(r -> null);
        } else {
            val updateArgs = new UpdateArg<>(update.getKey().getPrimaryKey(), s,
                    Iterators.singletonIterator(this.entryHelper.toTableSegmentEntry(s, (TableEntryUpdate) update)));
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
            val args = toArg(firstInput, inputIterator, ts,
                    u -> this.entryHelper.toTableSegmentKey(ts, (Remove) u));
            return removeFromSegment(args.getTableSegment(), args.getAllArgs()).thenApply(r -> Collections.emptyList());
        } else {
            val args = toArg(firstInput, inputIterator, ts,
                    u -> this.entryHelper.toTableSegmentEntry(ts, (TableEntryUpdate) u));
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
                        ex = Exceptions.unwrap(ex);
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
            g.add(this.entryHelper.serializeKey(k), count.getAndIncrement());
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
                            r[kg.ordinals.get(i)] = this.entryHelper.fromTableSegmentEntry(ts, segmentResult.get(i));
                        }
                    });
                    return Arrays.asList(r);
                });
    }

    @Override
    public KeyValueTableIteratorImpl.Builder iterator() {
        Exceptions.checkNotClosed(this.closed.get(), this);
        return new KeyValueTableIteratorImpl.Builder(this.config, this.entryHelper, this.executor);
    }

    //endregion

    //region Helpers

    private <T> UpdateArg<T> toArg(TableModification firstInput, Iterator<TableModification> inputIterator, TableSegment ts,
                                   Function<TableModification, T> convert) {
        val firstInputIterator = Iterators.singletonIterator(convert.apply(firstInput));
        val restIterator = Iterators.transform(inputIterator, i -> {
            final ByteBuffer pk = i.getKey().getPrimaryKey();
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

    //endregion

    //region Helper classes

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
