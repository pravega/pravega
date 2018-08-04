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
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.time.Duration;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import lombok.Data;
import lombok.Getter;
import lombok.val;

public class BTreeIndex<K, V> {
    private static final ByteArrayComparator KEY_COMPARATOR = new ByteArrayComparator();
    private final DataSource dataSource;
    private final Serializer<K> keySerializer;
    private final Serializer<V> valueSerializer;
    private final BTreePageLayout.Index indexLayout;
    private final BTreePageLayout.Leaf leafLayout;
    private final Executor executor;

    public BTreeIndex(int maxPageSize, DataSource dataSource, Serializer<K> keySerializer, Serializer<V> valueSerializer, Executor executor) {
        this.dataSource = dataSource;
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
        this.executor = executor;
        this.indexLayout = new BTreePageLayout.Index(this.keySerializer.length, maxPageSize);
        this.leafLayout = new BTreePageLayout.Leaf(this.keySerializer.length, this.valueSerializer.length, maxPageSize);
    }

    public CompletableFuture<V> get(K key, Duration timeout) {
        // Serialize the Key.
        byte[] serializedKey = serializeKey(key);
        TimeoutTimer timer = new TimeoutTimer(timeout);

        // Lookup the page where the Key should exist (if at all).
        return locatePage(serializedKey, new PageCollection(), timer)
                .thenApplyAsync(page -> {
                    // Found the eligible page. Return the value, if it exists.
                    val value = this.leafLayout.getValue(serializedKey, page.getPage());
                    return value == null ? null : this.valueSerializer.deserialize.apply(value, 0);
                }, this.executor);
    }

    public CompletableFuture<Void> insert(Map<K, V> entries, Duration timeout) {
        TimeoutTimer timer = new TimeoutTimer(timeout);
        return insertIntoPages(entries, timer)
                .thenApply(this::processModifiedPages)
                .thenComposeAsync(pageCollection -> this.dataSource.writePages(pageCollection, timer.getRemaining()), this.executor);
    }

    public CompletableFuture<Void> delete(Collection<K> keys, Duration timeout) {
        TimeoutTimer timer = new TimeoutTimer(timeout);
        return deleteFromPages(keys, timer)
                .thenApply(this::processModifiedPages)
                .thenComposeAsync(pageCollection -> this.dataSource.writePages(pageCollection, timer.getRemaining()), this.executor);
    }

    //region Helpers

    private CompletableFuture<PageCollection> deleteFromPages(Collection<K> keys, TimeoutTimer timer) {
        val toDelete = keys.stream()
                           .map(this::serializeKey)
                           .sorted(KEY_COMPARATOR)
                           .iterator();

        PageCollection pageCollection = new PageCollection();
        AtomicReference<PageWrapper> lastPage = new AtomicReference<>(null);
        val lastPageKeys = new ArrayList<byte[]>();
        return Futures.loop(toDelete::hasNext,
                () -> {
                    // Locate the page where the key is to be removed from. Do not yet delete it as it is more efficient
                    // to bulk-delete multiple at once. Collect all keys for each Page, and only delete them
                    // once we have "moved on" to another page.
                    val key = toDelete.next();
                    return locatePage(key, pageCollection, timer)
                            .thenAccept(page -> {
                                PageWrapper last = lastPage.get();
                                if (page != last) {
                                    // This key goes to a different page than the one we were looking at.
                                    if (last != null) {
                                        // Commit the outstanding entries.
                                        last.getPage().delete(lastPageKeys);
                                    }

                                    // Update the pointers.
                                    lastPage.set(page);
                                    lastPageKeys.clear();
                                }

                                // Record the current key.
                                lastPageKeys.add(key);
                            });
                },
                this.executor)
                      .thenApply(v -> {
                          // We need not forget to delete the last batch of keys from the last page.
                          if (lastPage.get() != null) {
                              lastPage.get().getPage().delete(lastPageKeys);
                          }
                          return pageCollection;
                      });
    }

    private CompletableFuture<PageCollection> insertIntoPages(Map<K, V> entries, TimeoutTimer timer) {
        // Serialize Entries and order them by Key.
        val toInsert = entries.entrySet().stream()
                              .map(e -> new AbstractMap.SimpleImmutableEntry<>(serializeKey(e.getKey()), serializeValue(e.getValue())))
                              .sorted((e1, e2) -> KEY_COMPARATOR.compare(e1.getKey(), e2.getKey()))
                              .iterator();

        PageCollection pageCollection = new PageCollection();

        AtomicReference<PageWrapper> lastPage = new AtomicReference<>(null);
        val lastPageEntries = new ArrayList<ArrayTuple>();
        return Futures.loop(toInsert::hasNext,
                () -> {
                    // Locate the page where the entry is to be inserted. Do not yet insert it as it is more efficient
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
                                lastPageEntries.add(new ArrayTuple(entry.getKey(), entry.getValue()));
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

    private PageCollection processModifiedPages(PageCollection pageCollection) {
        // Process all the pages, from bottom (leaf) up:
        // * If it requires a split, do that (it may split into more than two).
        // * TODO: remove empty pages (in lieu of merging)
        // * Assign new offset.
        // * Index pages need to be updated with new child page pointers.
        val currentBatch = new ArrayList<PageWrapper>();
        pageCollection.collectLeafPages(currentBatch);
        while (!currentBatch.isEmpty()) {
            val parents = new HashSet<Long>();
            for (PageWrapper p : currentBatch) {
                val newChildPointers = new ArrayList<BTreePagePointer>();
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
                                    new BTreePagePointer(pageKey, -1, page.getLength()), p.isIndexPage() ? this.indexLayout : this.leafLayout);
                            pageCollection.insert(newPage);
                            newOffset = newPage.getAssignedOffset();
                        }

                        newChildPointers.add(new BTreePagePointer(pageKey, newOffset, page.getLength()));
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
                    val newRootContents = this.indexLayout.createEmptyRoot();
                    parentPage = new PageWrapper(newRootContents, null, null, this.indexLayout);
                    pageCollection.insert(parentPage);
                }

                if (parentPage != null) {
                    // Update child pointers for the parent.
                    this.indexLayout.updatePointers(newChildPointers, parentPage.getPage());

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

    private CompletableFuture<PageWrapper> locatePage(byte[] key, PageCollection pageCollection, TimeoutTimer timer) {
        return this.dataSource
                .getState(timer.getRemaining())
                .thenCompose(state -> {
                    if (pageCollection.isInitialized()) {
                        Preconditions.checkArgument(pageCollection.getIndexLength() == state.getLength(), "Unexpected length.");
                    } else {
                        pageCollection.initialize(state.getLength());
                    }

                    if (state.getRootPageOffset() < 0) {
                        // No data. Return an empty (leaf) page, which will serve as the root for now.
                        return CompletableFuture.completedFuture(new PageWrapper(this.leafLayout.createEmptyRoot(), null, null, this.leafLayout));
                    }

                    AtomicReference<BTreePagePointer> toFetch = new AtomicReference<>(new BTreePagePointer(null, state.getRootPageOffset(), state.getRootPageLength()));
                    CompletableFuture<PageWrapper> result = new CompletableFuture<>();
                    AtomicReference<PageWrapper> previous = new AtomicReference<>(null);
                    Futures.loop(
                            () -> !result.isDone(),
                            () -> fetchPage(toFetch.get(), previous.get(), pageCollection, timer.getRemaining())
                                    .thenAccept(page -> {
                                        if (page.isIndexPage()) {
                                            // TODO: sanity checks.
                                            val value = this.indexLayout.getPagePointer(key, page.getPage());
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

    private CompletableFuture<PageWrapper> fetchPage(BTreePagePointer pagePointer, PageWrapper parentPage, PageCollection pageCollection, Duration timeout) {
        PageWrapper fromCache = pageCollection.get(pagePointer.getOffset());
        if (fromCache != null) {
            return CompletableFuture.completedFuture(fromCache);
        }

        return this.dataSource
                .readPage(pagePointer.getOffset(), pagePointer.getLength(), timeout)
                .thenApply(data -> {
                    ByteArraySegment pageContents = new ByteArraySegment(data);
                    BTreePageLayout layout = BTreePage.isIndexPage(pageContents) ? this.indexLayout : this.leafLayout;
                    PageWrapper wrapper = new PageWrapper(layout.parse(pageContents), parentPage, pagePointer, layout.isIndexLayout() ? this.indexLayout : this.leafLayout);
                    pageCollection.insert(wrapper);
                    return wrapper;
                });
    }

    private byte[] serializeKey(K key) {
        byte[] result = new byte[this.keySerializer.length];
        this.keySerializer.serialize.accept(result, 0, key);
        return result;
    }

    private byte[] serializeValue(V value) {
        byte[] result = new byte[this.valueSerializer.length];
        this.valueSerializer.serialize.accept(result, 0, value);
        return result;
    }

    //endregion

    //region PageCollection

    private static class PageWrapper {
        private final AtomicReference<BTreePage> page;
        @Getter
        private final PageWrapper parent;
        @Getter
        private final BTreePagePointer pointer;
        private final AtomicLong assignedOffset;
        @Getter
        private final BTreePageLayout layout;

        PageWrapper(BTreePage page, PageWrapper parent, BTreePagePointer pointer, BTreePageLayout layout) {
            this.page = new AtomicReference<>(page);
            this.parent = parent;
            this.pointer = pointer;
            this.assignedOffset = new AtomicLong(this.pointer == null ? -1 : this.pointer.getOffset());
            this.layout = layout;
        }

        boolean isIndexPage() {
            return this.layout.isIndexLayout();
        }

        BTreePage getPage() {
            return this.page.get();
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
            PageWrapper existing = this.pageByOffset.remove(page.getAssignedOffset());
            assert existing != null : "page not registered";
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

        public Serializer(int length, Serialize<T> serialize, Deserialize<T> deserialize) {
            this.length = length;
            this.serialize = Preconditions.checkNotNull(serialize, "serialize");
            this.deserialize = Preconditions.checkNotNull(deserialize, "deserialize");
        }

        @FunctionalInterface
        public interface Serialize<T> {
            void accept(byte[] target, int targetOffset, T toSerialize);
        }

        @FunctionalInterface
        public interface Deserialize<T> {
            T apply(ArrayView target, int targetOffset);
        }
    }

    //endregion

    public static class DataSource {
        private static final int FOOTER_LENGTH = Long.BYTES;
        private final Read read;
        private final Write write;
        private final GetLength getLength;
        private final AtomicReference<State> state;

        public DataSource(Read read, Write write, GetLength getLength) {
            this.read = Preconditions.checkNotNull(read, "read");
            this.write = Preconditions.checkNotNull(write, "write");
            this.getLength = Preconditions.checkNotNull(getLength, "getLength");
            this.state = new AtomicReference<>();
        }

        CompletableFuture<byte[]> readPage(long offset, int length, Duration timeout) {
            return this.read.apply(offset, length, timeout);
        }

        CompletableFuture<Void> writePages(PageCollection pageCollection, Duration timeout) {
            // Collect the data to be written.
            val streams = new ArrayList<InputStream>();
            long offset = -1;
            AtomicInteger length = new AtomicInteger();
            PageWrapper lastPage = null;
            for (PageWrapper p : pageCollection.orderPagesByIndex()) {
                if (offset >= 0) {
                    Preconditions.checkArgument(p.getAssignedOffset() == offset, "Expecting Page offset %s, found %s.", offset, p.getAssignedOffset());
                }

                streams.add(p.getPage().getContents().getReader());
                offset = p.getAssignedOffset();
                length.addAndGet(p.getPage().getContents().getLength());
                lastPage = p;
            }

            // Write a footer with information about locating the root page.
            assert lastPage != null && lastPage.getParent() == null : "last page to be written is not the root page";
            streams.add(getFooter(lastPage.getAssignedOffset()));

            // Write it.
            val toWrite = new SequenceInputStream(Collections.enumeration(streams));
            long lastPageOffset = lastPage.getAssignedOffset();
            int lastPageLength = lastPage.getPage().getContents().getLength();
            return this.write.apply(pageCollection.getIndexLength(), toWrite, length.get(), timeout)
                             .thenRun(() -> this.state.set(new State(pageCollection.getIndexLength() + length.get(), lastPageOffset, lastPageLength)));
        }

        CompletableFuture<State> getState(Duration timeout) {
            if (this.state.get() != null) {
                return CompletableFuture.completedFuture(this.state.get());
            }

            TimeoutTimer timer = new TimeoutTimer(timeout);
            return this.getLength
                    .apply(timer.getRemaining())
                    .thenCompose(length -> {
                        if (length <= FOOTER_LENGTH) {
                            return CompletableFuture.completedFuture(new State(length, -1, 0));
                        }

                        return this.read
                                .apply(length - FOOTER_LENGTH, FOOTER_LENGTH, timer.getRemaining())
                                .thenApply(footer -> {
                                    long rootPageOffset = getRootPageOffset(footer);
                                    return new State(length, rootPageOffset, (int) (length - FOOTER_LENGTH - rootPageOffset));
                                });
                    });
        }

        private InputStream getFooter(long rootPageOffset) {
            byte[] result = new byte[FOOTER_LENGTH];
            BitConverter.writeLong(result, 0, rootPageOffset);
            return new ByteArrayInputStream(result);
        }

        private long getRootPageOffset(byte[] footer) {
            Preconditions.checkArgument(footer.length == FOOTER_LENGTH, "Unexpected footer length.");
            return BitConverter.readLong(footer, 0);
        }

        @Data
        static class State {
            private final long length;
            private final long rootPageOffset;
            private final int rootPageLength;
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
            CompletableFuture<Void> apply(long expectedOffset, InputStream data, int length, Duration timeout);
        }
    }
}

