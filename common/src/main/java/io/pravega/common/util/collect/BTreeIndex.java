/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.util.collect;

import com.google.common.base.Preconditions;
import io.pravega.common.TimeoutTimer;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.ArrayView;
import io.pravega.common.util.BitConverter;
import io.pravega.common.util.ByteArraySegment;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.val;

public class BTreeIndex<K, V> {
    private static final int INDEX_VALUE_LENGTH = Long.BYTES + Integer.BYTES;
    private static final ByteArrayComparator KEY_COMPARATOR = new ByteArrayComparator();
    private final DataSource dataSource;
    private final Serializer<K> keySerializer;
    private final Serializer<V> valueSerializer;
    private final Executor executor;
    private final BTreePage.Config indexPageConfig;
    private final BTreePage.Config leafPageConfig;

    BTreeIndex(int maxPageSize, DataSource dataSource, Serializer<K> keySerializer, Serializer<V> valueSerializer, Executor executor) {
        this.dataSource = dataSource;
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
        this.executor = executor;
        this.indexPageConfig = new BTreePage.Config(this.keySerializer.length, INDEX_VALUE_LENGTH, maxPageSize, true);
        this.leafPageConfig = new BTreePage.Config(this.keySerializer.length, this.valueSerializer.length, maxPageSize, false);
    }

    public CompletableFuture<V> get(K key, Duration timeout) {
        // Serialize the Key.
        val serializedKey = serializeKey(key);
        TimeoutTimer timer = new TimeoutTimer(timeout);

        // Lookup the page where the Key should exist (if at all).
        return locatePage(serializedKey, new PageCollection(), timer)
                .thenApplyAsync(page -> {
                    // Found the eligible page. Return the value, if it exists.
                    val value = page.getPage().getValue(serializedKey);
                    return value == null ? null : this.valueSerializer.deserialize.apply(value, 0);
                }, this.executor);
    }

    public CompletableFuture<Void> insert(Map<K, V> entries, Duration timeout) {
        TimeoutTimer timer = new TimeoutTimer(timeout);
        return insertIntoPages(entries, timer)
                .thenApply(this::processModifiedPages)
                .thenComposeAsync(pageCollection -> commitModifiedPages(pageCollection, timer), this.executor);
    }

    private CompletableFuture<Void> commitModifiedPages(PageCollection pageCollection, TimeoutTimer timer) {
        // Collect the data to be written.
        val streams = new ArrayList<InputStream>();
        long offset = -1;
        int length = 0;
        for (PageWrapper p : pageCollection.orderPagesByIndex()) {
            if (offset >= 0) {
                Preconditions.checkArgument(p.getAssignedOffset() == offset, "Expecting Page offset %s, found %s.", offset, p.getAssignedOffset());
            }
            streams.add(p.getPage().getContents().getReader());
            offset = p.getAssignedOffset();
            length += p.getPage().getContents().getLength();
        }

        // Write it.
        // TODO: add footer indicating root page length.
        val toWrite = new SequenceInputStream(Collections.enumeration(streams));
        return this.dataSource.write.apply(toWrite, length, timer.getRemaining());
    }

    private PageCollection processModifiedPages(PageCollection pageCollection) {
        // Process all the pages, from bottom (leaf) up:
        // * If it requires a split, do that (it may split into more than two).
        // * Assign new offset.
        // * Index pages need to be updated with new child page pointers.
        val currentBatch = new ArrayList<PageWrapper>();
        pageCollection.collectLeafPages(currentBatch);
        while (!currentBatch.isEmpty()) {
            val parents = new HashSet<Long>();
            for (PageWrapper p : currentBatch) {
                val newChildPointers = new ArrayList<PagePointer>();
                val splitResult = p.getPage().splitIfNecessary();
                if (splitResult != null) {
                    // This Page must be split.
                    for (int i = 0; i < splitResult.size(); i++) {
                        val page = splitResult.get(i);
                        val pageKey = page.getKeyAt(0);
                        long newOffset;
                        if (i == 0) {
                            // The original page will get the first split. Nothing changes about its pointer key.
                            p.setPage(page);
                            pageCollection.assignNewOffset(p);
                            newOffset = p.getAssignedOffset();
                        } else {
                            PageWrapper newPage = new PageWrapper(page, p.getParent(),
                                    new PagePointer(pageKey, -1, page.getLength()));
                            pageCollection.insert(newPage);
                            newOffset = newPage.getAssignedOffset();
                        }

                        newChildPointers.add(new PagePointer(pageKey, newOffset, page.getLength()));
                    }
                } else {
                    // This page has been modified. Assign it a new virtual offset so that we can add a proper pointer
                    // to it from its parent.
                    pageCollection.assignNewOffset(p);
                }

                // Make sure we process its parent as well.
                PageWrapper parentPage = p.getParent();
                if (parentPage == null && newChildPointers.size() > 1) {
                    // There was a split. Make sure that if this was the root, we create a new root.
                    val newRootContents = createEmptyIndexRoot();
                    parentPage = new PageWrapper(newRootContents, null, null);
                    pageCollection.insert(parentPage);
                }

                if (parentPage != null) {
                    // Update child pointers for the parent.
                    val toUpdate = newChildPointers.stream()
                            .map(cp -> new PageEntry(cp.key, serializePointer(cp)))
                            .collect(Collectors.toList());
                    parentPage.getPage().update(toUpdate);

                    // Then queue up the parent for processing.
                    parents.add(parentPage.getAssignedOffset());
                }
            }

            // We are done with this level. Move up one process the pages at that level.
            currentBatch.clear();
            pageCollection.collectPages(parents, currentBatch);
        }

        return pageCollection;
    }

    private CompletableFuture<PageCollection> insertIntoPages(Map<K, V> entries, TimeoutTimer timer) {
        // Serialize Entries and order them by Key.
        val toInsert = entries.entrySet().stream()
                .map(e -> new PageEntry(serializeKey(e.getKey()), serializeValue(e.getValue())))
                              .sorted((e1, e2) -> KEY_COMPARATOR.compare(e1.getKey(), e2.getKey()))
                              .iterator();

        PageCollection pageCollection = new PageCollection();

        AtomicReference<PageWrapper> lastPage = new AtomicReference<>(null);
        val lastPageEntries = new ArrayList<PageEntry>();
        return Futures.loop(toInsert::hasNext,
                () -> {
                    // Locate the page where entry is to be inserted. Do not yet insert it as it is more efficient
                    // to bulk-insert multiple at once. Collect all entries for each Page, and only insert them
                    // once we have "moved on" to another page.
                    val entry = toInsert.next();
                    return locatePage(entry.getKey(), pageCollection, timer)
                            .thenAccept(page -> {
                                PageWrapper last = lastPage.get();
                                if (page != last) {
                                    // This entry goes to a different page than the one we were looking at.
                                    if (last != null) {
                                        // Commit the outstanding entries.
                                        last.getPage().update(lastPageEntries);
                                    }

                                    // Update the pointers.
                                    lastPage.set(page);
                                    lastPageEntries.clear();
                                }

                                // Record the current entry.
                                lastPageEntries.add(new PageEntry(entry.getKey(), entry.getValue()));
                            });
                },
                this.executor)
                      .thenApply(v -> {
                          // We need not forget to insert the last batch of entries into the last page.
                          if (lastPage.get() != null) {
                              lastPage.get().getPage().update(lastPageEntries);
                          }
                          return pageCollection;
                      });
    }

    public CompletableFuture<Void> delete(Collection<K> keys, Duration timeout) {
        return null;
    }

    //region Helpers

    private CompletableFuture<PageWrapper> locatePage(ByteArraySegment key, PageCollection pageCollection, TimeoutTimer timer) {
        int maxPageSize = this.leafPageConfig.getMaxPageSize();
        CompletableFuture<Long> lengthFuture = pageCollection.isInitialized()
                ? CompletableFuture.completedFuture(pageCollection.getIndexLength())
                : this.dataSource.getLength.apply(timer.getRemaining())
                                           .thenApply(length -> {
                                               if (length > 0 && length < maxPageSize) {
                                                   // Root page has fixed size, set to maxPageSize.
                                                   throw new CompletionException(new IOException("Index size too small to fit root page."));
                                               }

                                               pageCollection.initialize(length);
                                               return length;
                                           });

        return lengthFuture.thenCompose(length -> {
            if (length == 0) {
                // No data. Return an empty (leaf) page, which will serve as the root for now.
                return CompletableFuture.completedFuture(new PageWrapper(createEmptyLeafRoot(), null, null));
            }

            AtomicReference<PagePointer> toFetch = new AtomicReference<>(new PagePointer(null, length - maxPageSize, maxPageSize));
            CompletableFuture<PageWrapper> result = new CompletableFuture<>();
            AtomicReference<PageWrapper> previous = new AtomicReference<>(null);
            Futures.loop(
                    () -> !result.isDone(),
                    () -> fetchPage(toFetch.get(), previous.get(), pageCollection, timer.getRemaining())
                            .thenAccept(page -> {
                                if (page.isIndexPage()) {
                                    // TODO: sanity checks.
                                    val value = getPagePointer(key, page.getPage());
                                    toFetch.set(value);
                                    previous.set(page);
                                } else {
                                    // Leaf (data) page. We are done.
                                    result.complete(page);
                                }
                            }),
                    this.executor)
                   .exceptionally(ex -> {
                       result.completeExceptionally(ex);
                       return null;
                   });
            return result;
        });
    }

    private CompletableFuture<PageWrapper> fetchPage(PagePointer pagePointer, PageWrapper parentPage, PageCollection pageCollection, Duration timeout) {
        PageWrapper fromCache = pageCollection.get(pagePointer.offset);
        if (fromCache != null) {
            return CompletableFuture.completedFuture(fromCache);
        }

        return this.dataSource.read
                .apply(pagePointer.offset, pagePointer.length, timeout)
                .thenApply(data -> {
                    PageWrapper wrapper = new PageWrapper(parsePage(data), parentPage, pagePointer);
                    pageCollection.insert(wrapper);
                    return wrapper;
                });
    }

    private BTreePage parsePage(byte[] pageData) {
        ByteArraySegment pageContents = new ByteArraySegment(pageData);
        val pageConfig = BTreePage.isIndexPage(pageContents) ? this.indexPageConfig : this.leafPageConfig;
        return new BTreePage(pageConfig, pageContents);
    }

    private BTreePage createEmptyLeafRoot() {
        return new BTreePage(this.leafPageConfig, 0);
    }

    private BTreePage createEmptyIndexRoot() {
        return new BTreePage(this.indexPageConfig, 0);
    }

    private ByteArraySegment serializeKey(K key) {
        byte[] result = new byte[this.keySerializer.length];
        this.keySerializer.serialize.accept(key, result, 0);
        return new ByteArraySegment(result);
    }

    private ByteArraySegment serializeValue(V value) {
        byte[] result = new byte[this.valueSerializer.length];
        this.valueSerializer.serialize.accept(value, result, 0);
        return new ByteArraySegment(result);
    }

    private PagePointer getPagePointer(ByteArraySegment key, BTreePage page) {
        val pos = page.search(key, 0);
        assert pos.getPosition() >= 0;
        return deserializePointer(page.getValueAt(pos.getPosition()), page.getKeyAt(pos.getPosition()));
    }

    private ByteArraySegment serializePointer(PagePointer pointer) {
        ByteArraySegment result = new ByteArraySegment(new byte[Long.BYTES + Integer.BYTES]);
        BitConverter.writeLong(result, 0, pointer.offset);
        BitConverter.writeInt(result, Long.BYTES, pointer.length);
        return result;
    }

    private PagePointer deserializePointer(ByteArraySegment source, ByteArraySegment searchKey) {
        long pageOffset = BitConverter.readLong(source, 0);
        int pageLength = BitConverter.readInt(source, Long.BYTES);
        return new PagePointer(searchKey, pageOffset, pageLength);
    }

    //endregion

    //region PageCollection

    @RequiredArgsConstructor
    private static class PagePointer {
        final ByteArraySegment key;
        final long offset;
        final int length;
    }

    private static class PageWrapper {
        private final AtomicReference<BTreePage> page;
        @Getter
        private final PageWrapper parent;
        @Getter
        private final PagePointer pointer;
        private final AtomicLong assignedOffset;

        PageWrapper(BTreePage page, PageWrapper parent, PagePointer pointer) {
            this.page = new AtomicReference<>(page);
            this.parent = parent;
            this.pointer = pointer;
            this.assignedOffset = new AtomicLong(this.pointer == null ? -1 : this.pointer.offset);
        }

        BTreePage getPage() {
            return this.page.get();
        }

        boolean isIndexPage() {
            return getPage().getConfig().isIndexPage();
        }

        void setPage(BTreePage page) {
            this.page.set(page);
        }

        long getAssignedOffset() {
            return this.assignedOffset.get();
        }

        void setAssignedOffset(long value) {
            this.assignedOffset.set(value);
        }
    }

    /**
     * Volatile Page Cache for any tree Page that is loaded (and implicitly modified).
     */
    private static class PageCollection {
        private final HashMap<Long, PageWrapper> pageByOffset = new HashMap<>();
        private final AtomicLong indexLength;

        PageCollection() {
            this.indexLength = new AtomicLong(-1);
        }

        void initialize(long indexLength) {
            if (!this.indexLength.compareAndSet(-1, indexLength)) {
                throw new IllegalStateException("Already initialized.");
            }
        }

        boolean isInitialized() {
            return this.indexLength.get() >= 0;
        }

        long getIndexLength() {
            return this.indexLength.get();
        }

        int getCount() {
            return this.pageByOffset.size();
        }

        PageWrapper get(long offset) {
            return this.pageByOffset.getOrDefault(offset, null);
        }

        void assignNewOffset(PageWrapper page) {
            this.pageByOffset.remove(page.getAssignedOffset());
            page.setAssignedOffset(-1);
            insert(page);
        }

        void insert(PageWrapper page) {
            if (page.getAssignedOffset() < 0) {
                // This is a new page, ready to be written. We need to assign an offset.
                long newOffset = this.indexLength.getAndAdd(page.getPage().getLength());
                page.setAssignedOffset(newOffset);
            }

            this.pageByOffset.put(page.getAssignedOffset(), page);
        }

        void collectLeafPages(Collection<PageWrapper> target) {
            this.pageByOffset.values().stream().filter(p -> !p.isIndexPage()).forEach(target::add);
        }

        void collectPages(Collection<Long> offsets, Collection<PageWrapper> target) {
            offsets.forEach(offset -> {
                PageWrapper p = this.pageByOffset.getOrDefault(offset, null);
                if (p != null) {
                    target.add(p);
                }
            });
        }

        List<PageWrapper> orderPagesByIndex() {
            return this.pageByOffset
                    .values().stream()
                    .sorted(Comparator.comparingLong(PageWrapper::getAssignedOffset))
                    .collect(Collectors.toList());
        }
    }

    //endregion

    //region Serializer

    public static class Serializer<T> {
        private final int length;
        private final Serialize<T> serialize;
        private final Deserialize<T> deserialize;

        Serializer(int length, Serialize<T> serialize, Deserialize<T> deserialize) {
            this.length = length;
            this.serialize = Preconditions.checkNotNull(serialize, "serialize");
            this.deserialize = Preconditions.checkNotNull(deserialize, "deserialize");
        }

        @FunctionalInterface
        public interface Serialize<T> {
            void accept(T toSerialize, byte[] target, int targetOffset);
        }

        @FunctionalInterface
        public interface Deserialize<T> {
            T apply(ArrayView target, int targetOffset);
        }
    }

    //endregion

    public static class DataSource {
        private final Read read;
        private final Write write;
        private final GetLength getLength;

        DataSource(Read read, Write write, GetLength getLength) {
            this.read = Preconditions.checkNotNull(read, "read");
            this.write = Preconditions.checkNotNull(write, "write");
            this.getLength = Preconditions.checkNotNull(getLength, "getLength");
        }

        @FunctionalInterface
        public interface GetLength {
            CompletableFuture<Long> apply(Duration timeout);
        }

        @FunctionalInterface
        public interface Read {
            CompletableFuture<byte[]> apply(long offset, int length, Duration timeout);
        }

        @FunctionalInterface
        public interface Write {
            CompletableFuture<Void> apply(InputStream data, int length, Duration timeout);
        }
    }
}

