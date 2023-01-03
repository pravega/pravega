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
package io.pravega.segmentstore.storage.mocks;

import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
import io.pravega.common.util.AsyncIterator;
import io.pravega.common.util.BufferView;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.segmentstore.contracts.SegmentType;
import io.pravega.segmentstore.contracts.StreamSegmentExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.contracts.tables.BadKeyVersionException;
import io.pravega.segmentstore.contracts.tables.IteratorArgs;
import io.pravega.segmentstore.contracts.tables.IteratorItem;
import io.pravega.segmentstore.contracts.tables.KeyNotExistsException;
import io.pravega.segmentstore.contracts.tables.TableEntry;
import io.pravega.segmentstore.contracts.tables.TableKey;
import io.pravega.segmentstore.contracts.tables.TableSegmentConfig;
import io.pravega.segmentstore.contracts.tables.TableSegmentInfo;
import io.pravega.segmentstore.contracts.tables.TableStore;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import lombok.Data;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;

@RequiredArgsConstructor
@ThreadSafe
public class InMemoryTableStore implements TableStore {
    @GuardedBy("tables")
    private final HashMap<String, TableData> tables = new HashMap<>();
    private final AtomicBoolean closed = new AtomicBoolean();

    @NonNull
    private final Executor executor;

    //region TableStore Implementation

    @Override
    public CompletableFuture<Void> createSegment(String segmentName, SegmentType segmentType, TableSegmentConfig config, Duration timeout) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        Preconditions.checkArgument(segmentType.isTableSegment(), "Expected SegmentType.isSortedSegment.");
        Preconditions.checkArgument(segmentType.isSystem(), "Expected SegmentType.isSystem.");
        Preconditions.checkArgument(segmentType.isCritical(), "Expected SegmentType.isCritical.");
        Preconditions.checkArgument(!segmentType.isFixedKeyLengthTableSegment(), "Fixed-Key-Length Table Segments not supported in this mock.");
        return CompletableFuture.runAsync(() -> {
            synchronized (this.tables) {
                if (this.tables.containsKey(segmentName)) {
                    throw new CompletionException(new StreamSegmentExistsException(segmentName));
                }

                this.tables.put(segmentName, new TableData(segmentName));
            }
        }, this.executor);
    }

    @Override
    public CompletableFuture<Void> deleteSegment(String segmentName, boolean mustBeEmpty, Duration timeout) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        return CompletableFuture.runAsync(() -> {
            synchronized (this.tables) {
                if (this.tables.remove(segmentName) == null) {
                    throw new CompletionException(new StreamSegmentNotExistsException(segmentName));
                }
            }
        }, this.executor);
    }

    @Override
    public CompletableFuture<List<Long>> put(String segmentName, List<TableEntry> entries, Duration timeout) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        return CompletableFuture.supplyAsync(() -> getTableData(segmentName).put(entries), this.executor);
    }

    @Override
    public CompletableFuture<List<Long>> put(String segmentName, List<TableEntry> entries, long tableSegmentOffset, Duration timeout) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        return CompletableFuture.supplyAsync(() -> getTableData(segmentName).put(entries), this.executor);
    }

    @Override
    public CompletableFuture<Void> remove(String segmentName, Collection<TableKey> keys, Duration timeout) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        return CompletableFuture.runAsync(() -> getTableData(segmentName).remove(keys), this.executor);
    }

    @Override
    public CompletableFuture<Void> remove(String segmentName, Collection<TableKey> keys, long tableSegmentOffset, Duration timeout) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        return CompletableFuture.runAsync(() -> getTableData(segmentName).remove(keys), this.executor);
    }

    @Override
    public CompletableFuture<List<TableEntry>> get(String segmentName, List<BufferView> keys, Duration timeout) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        return CompletableFuture.supplyAsync(() -> getTableData(segmentName).get(keys), this.executor);
    }

    @Override
    public CompletableFuture<AsyncIterator<IteratorItem<TableKey>>> keyIterator(String segmentName, IteratorArgs args) {
        Collection<TableEntry> tableEntries = getTableEntries(segmentName);
        val item = new IteratorItemImpl<>(args.getContinuationToken(), tableEntries.stream().map(TableEntry::getKey).collect(Collectors.toList()));
        return CompletableFuture.completedFuture(AsyncIterator.singleton(item));
    }

    @Override
    public CompletableFuture<AsyncIterator<IteratorItem<TableEntry>>> entryIterator(String segmentName, IteratorArgs args) {
        Collection<TableEntry> tableEntries = getTableEntries(segmentName);
        val item = new IteratorItemImpl<>(args.getContinuationToken(), tableEntries);
        return CompletableFuture.completedFuture(AsyncIterator.singleton(item));
    }

    @Override
    public CompletableFuture<AsyncIterator<IteratorItem<TableEntry>>> entryDeltaIterator(String segmentName, long fromPosition, Duration fetchTimeout) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<TableSegmentInfo> getInfo(String segmentName, Duration timeout) {
        throw new UnsupportedOperationException();
    }

    @SneakyThrows(StreamSegmentNotExistsException.class)
    private TableData getTableData(String segmentName) {
        synchronized (this.tables) {
            TableData result = this.tables.get(segmentName);
            if (result == null) {
                throw new StreamSegmentNotExistsException(segmentName);
            }
            return result;
        }
    }

    private Collection<TableEntry> getTableEntries(String segmentName) {
        TableData tableData = this.tables.get(segmentName);
        Collection<TableEntry> tableEntries =  null != tableData ?
                tableData.entries.values().stream()
                        .filter(tableEntry -> tableEntry != null && tableEntry.getValue() != null)
                        .collect(Collectors.toList())
                : Collections.emptyList();
        return tableEntries;
    }

    //endregion

    //region TableData

    @ThreadSafe
    @RequiredArgsConstructor
    private static class TableData {
        private final AtomicLong nextVersion = new AtomicLong();
        @GuardedBy("this")
        private final HashMap<BufferView, TableEntry> entries = new HashMap<>();
        private final String segmentName;

        synchronized List<Long> put(List<TableEntry> entries) {
            validateKeys(entries, TableEntry::getKey);
            return entries
                    .stream()
                    .map(e -> {
                        long version = this.nextVersion.incrementAndGet();
                        val key = new ByteArraySegment(e.getKey().getKey().getCopy());
                        this.entries.put(key,
                                TableEntry.versioned(key, new ByteArraySegment(e.getValue().getCopy()), version));
                        return version;
                    })
                    .collect(Collectors.toList());
        }

        synchronized void remove(Collection<TableKey> keys) {
            validateKeys(keys, k -> k);
            keys.forEach(k -> this.entries.remove(k.getKey()));
        }

        synchronized List<TableEntry> get(List<BufferView> keys) {
            return keys.stream().map(this.entries::get).collect(Collectors.toList());
        }

        @GuardedBy("this")
        private <T> void validateKeys(Collection<T> items, Function<T, TableKey> getKey) {
            items.stream()
                    .map(getKey)
                    .filter(TableKey::hasVersion)
                    .forEach(k -> {
                        TableEntry e = this.entries.get(k.getKey());
                        if (e == null) {
                            if (k.getVersion() != TableKey.NOT_EXISTS) {
                                throw new CompletionException(new KeyNotExistsException(this.segmentName, k.getKey()));
                            }
                        } else if (k.getVersion() != e.getKey().getVersion()) {
                            throw new CompletionException(new BadKeyVersionException(this.segmentName, Collections.emptyMap()));
                        }
                    });
        }

        synchronized TableData deepCopy() {
            TableData clone = new TableData(this.segmentName);
            clone.nextVersion.set(this.nextVersion.get());
            clone.entries.putAll(this.entries);
            return clone;
        }
    }

    /**
     * Implementation of {@link IteratorItem} for {@link InMemoryTableStore}.
     * @param <T> Entry type.
     */
    @Data
    private static class IteratorItemImpl<T> implements IteratorItem<T> {
        private final BufferView state;
        private final Collection<T> entries;
    }

    //endregion

    /**
     * Creates a clone for given {@link InMemoryTableStore}. Useful to simulate zombie segment container.
     *
     * @param original Metadata store to clone.
     * @return Clone of given instance.
     */
    public static InMemoryTableStore clone(InMemoryTableStore original) {
        InMemoryTableStore clone = new InMemoryTableStore(original.executor);
        for (val e : original.tables.entrySet()) {
            clone.tables.put(e.getKey(), e.getValue().deepCopy());
        }
        return clone;
    }
}
