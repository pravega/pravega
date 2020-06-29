/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server;

import io.pravega.common.Exceptions;
import io.pravega.common.util.ArrayView;
import io.pravega.common.util.AsyncIterator;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.common.util.HashedArray;
import io.pravega.segmentstore.contracts.StreamSegmentExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.contracts.tables.BadKeyVersionException;
import io.pravega.segmentstore.contracts.tables.IteratorItem;
import io.pravega.segmentstore.contracts.tables.KeyNotExistsException;
import io.pravega.segmentstore.contracts.tables.TableEntry;
import io.pravega.segmentstore.contracts.tables.TableKey;
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
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;

/**
 * {@link TableStoreMock} mock used for testing purposes.
 */
@RequiredArgsConstructor
@ThreadSafe
public class TableStoreMock implements TableStore {
    @GuardedBy("tables")
    private final HashMap<String, TableData> tables = new HashMap<>();
    private final AtomicBoolean closed = new AtomicBoolean();
    @NonNull
    private final Executor executor;

    //region TableStore Implementation

    @Override
    public CompletableFuture<Void> createSegment(String segmentName, Duration timeout) {
        Exceptions.checkNotClosed(this.closed.get(), this);
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
    public CompletableFuture<Void> remove(String segmentName, Collection<TableKey> keys, Duration timeout) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        return CompletableFuture.runAsync(() -> getTableData(segmentName).remove(keys), this.executor);
    }

    @Override
    public CompletableFuture<List<TableEntry>> get(String segmentName, List<ArrayView> keys, Duration timeout) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        return CompletableFuture.supplyAsync(() -> getTableData(segmentName).get(keys), this.executor);
    }

    @Override
    public CompletableFuture<Void> merge(String targetSegmentName, String sourceSegmentName, Duration timeout) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Void> seal(String segmentName, Duration timeout) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<AsyncIterator<IteratorItem<TableKey>>> keyIterator(String segmentName, byte[] serializedState, Duration fetchTimeout) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<AsyncIterator<IteratorItem<TableEntry>>> entryIterator(String segmentName, byte[] serializedState, Duration fetchTimeout) {
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

    //endregion

    //region TableData

    @ThreadSafe
    @RequiredArgsConstructor
    private static class TableData {
        private final AtomicLong nextVersion = new AtomicLong();
        @GuardedBy("this")
        private final HashMap<HashedArray, TableEntry> entries = new HashMap<>();
        private final String segmentName;

        synchronized List<Long> put(List<TableEntry> entries) {
            validateKeys(entries, TableEntry::getKey);
            return entries
                    .stream()
                    .map(e -> {
                        long version = this.nextVersion.incrementAndGet();
                        val key = new HashedArray(e.getKey().getKey().getCopy());
                        this.entries.put(key,
                                TableEntry.versioned(key, new ByteArraySegment(e.getValue().getCopy()), version));
                        return version;
                    })
                    .collect(Collectors.toList());
        }

        synchronized void remove(Collection<TableKey> keys) {
            validateKeys(keys, k -> k);
            keys.forEach(k -> this.entries.remove(new HashedArray(k.getKey())));
        }

        synchronized List<TableEntry> get(List<ArrayView> keys) {
            return keys.stream().map(k -> this.entries.get(new HashedArray(k))).collect(Collectors.toList());
        }

        @GuardedBy("this")
        private <T> void validateKeys(Collection<T> items, Function<T, TableKey> getKey) {
            items.stream()
                 .map(getKey)
                 .filter(TableKey::hasVersion)
                 .forEach(k -> {
                     TableEntry e = this.entries.get(new HashedArray(k.getKey()));
                     if (e == null) {
                         if (k.getVersion() != TableKey.NOT_EXISTS) {
                             throw new CompletionException(new KeyNotExistsException(this.segmentName, k.getKey()));
                         }
                     } else if (k.getVersion() != e.getKey().getVersion()) {
                         throw new CompletionException(new BadKeyVersionException(this.segmentName, Collections.emptyMap()));
                     }
                 });
        }
    }

    //endregion
}