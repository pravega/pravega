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
import io.pravega.common.util.BitConverter;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.common.util.IllegalDataFormatException;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;

public class BTreeIndex {
    //region Members

    private static final long NO_OFFSET = -1L;
    private static final int INDEX_VALUE_LENGTH = Long.BYTES + Integer.BYTES;
    private static final ByteArrayComparator KEY_COMPARATOR = new ByteArrayComparator();
    private final DataSource dataSource;
    private final BTreePage.Config indexPageConfig;
    private final BTreePage.Config leafPageConfig;
    private final Executor executor;

    //endregion

    //region Constructor

    @Builder
    public BTreeIndex(int maxPageSize, int keyLength, int valueLength, @NonNull Read read, @NonNull Write write,
                      @NonNull GetLength getLength, @NonNull Executor executor) {
        // DataSource validates arguments.
        this.dataSource = new DataSource(read, write, getLength);
        this.executor = Preconditions.checkNotNull(executor, "executor");

        // BTreePage.Config validates the arguments so we don't need to.
        this.indexPageConfig = new BTreePage.Config(keyLength, INDEX_VALUE_LENGTH, maxPageSize, true);
        this.leafPageConfig = new BTreePage.Config(keyLength, valueLength, maxPageSize, false);
    }

    //endregion

    //region Operations

    public CompletableFuture<ByteArraySegment> get(@NonNull ByteArraySegment key, @NonNull Duration timeout) {
        TimeoutTimer timer = new TimeoutTimer(timeout);

        // Lookup the page where the Key should exist (if at all).
        PageCollection pageCollection = new PageCollection();
        return locatePage(key, pageCollection, timer)
                .thenApplyAsync(page -> page.getPage().searchExact(key), this.executor);
    }

    public CompletableFuture<List<ByteArraySegment>> get(@NonNull List<ByteArraySegment> keys, @NonNull Duration timeout) {
        if (keys.size() == 1) {
            // Shortcut for single key.
            return get(keys.get(0), timeout).thenApply(Collections::singletonList);
        }

        // Lookup all the keys in parallel, and make sure to apply their resulting values in the same order that their keys
        // where provided to us.
        TimeoutTimer timer = new TimeoutTimer(timeout);
        PageCollection pageCollection = new PageCollection();
        return Futures.allOfWithResults(
                keys.stream()
                    .map(key -> locatePage(key, pageCollection, timer).thenApplyAsync(page -> page.getPage().searchExact(key), this.executor))
                    .collect(Collectors.toList()));
    }

    public CompletableFuture<Void> insert(@NonNull Collection<PageEntry> entries, @NonNull Duration timeout) {
        TimeoutTimer timer = new TimeoutTimer(timeout);
        return insertIntoPages(entries, timer)
                .thenApply(this::processModifiedPages)
                .thenComposeAsync(pageCollection -> this.dataSource.writePages(pageCollection, timer.getRemaining()), this.executor);
    }

    public CompletableFuture<Void> delete(@NonNull Collection<ByteArraySegment> keys, @NonNull Duration timeout) {
        TimeoutTimer timer = new TimeoutTimer(timeout);
        return deleteFromPages(keys, timer)
                .thenApply(this::processModifiedPages)
                .thenComposeAsync(pageCollection -> this.dataSource.writePages(pageCollection, timer.getRemaining()), this.executor);
    }

    //endregion

    //region Helpers

    private CompletableFuture<PageCollection> deleteFromPages(Collection<ByteArraySegment> keys, TimeoutTimer timer) {
        // Process the keys in sorted order; this makes the operation more efficient as we can batch-delete keys belonging
        // to the same page.
        val toDelete = keys.stream().sorted(KEY_COMPARATOR::compare).iterator();

        PageCollection pageCollection = new PageCollection();
        AtomicReference<PageWrapper> lastPage = new AtomicReference<>(null);
        val lastPageKeys = new ArrayList<ByteArraySegment>();
        return Futures.loop(
                toDelete::hasNext,
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

    private CompletableFuture<PageCollection> insertIntoPages(Collection<PageEntry> entries, TimeoutTimer timer) {
        // Process the Entries in sorted order (by key); this makes the operation more efficient as we can batch-insert
        // entries belonging to the same page.
        val toInsert = entries.stream()
                              .sorted((e1, e2) -> KEY_COMPARATOR.compare(e1.getKey(), e2.getKey()))
                              .iterator();

        PageCollection pageCollection = new PageCollection();
        AtomicReference<PageWrapper> lastPage = new AtomicReference<>(null);
        val lastPageEntries = new ArrayList<PageEntry>();
        return Futures.loop(
                toInsert::hasNext,
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
                val updatedChildPointers = new ArrayList<PagePointer>();
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
                            pageCollection.complete(p);
                            newOffset = p.getAssignedOffset();
                        } else {
                            // Insert the new pages and assign them new virtual offsets.
                            PageWrapper newPage = PageWrapper.wrapNew(page, p.getParent(), new PagePointer(pageKey, NO_OFFSET, page.getLength()));
                            pageCollection.insert(newPage);
                            pageCollection.complete(newPage);
                            newOffset = newPage.getAssignedOffset();
                        }

                        updatedChildPointers.add(new PagePointer(pageKey, newOffset, page.getLength()));
                    }
                } else {
                    // This page has been modified. Assign it a new virtual offset so that we can add a proper pointer
                    // to it from its parent. Do this to all pages except newly added index (root) pages, which have
                    // already been processed.
                    pageCollection.complete(p);
                    updatedChildPointers.add(new PagePointer(p.getPage().getKeyAt(0), p.getAssignedOffset(), p.getPage().getLength()));
                }

                // Make sure we process its parent as well.
                PageWrapper parentPage = p.getParent();
                if (parentPage == null && splitResult != null) {
                    // There was a split. Make sure that if this was the root, we create a new root.
                    parentPage = PageWrapper.wrapNew(createEmptyIndexPage(), null, null);
                    pageCollection.insert(parentPage);
                }

                if (parentPage != null) {
                    // Update child pointers for the parent.
                    val toUpdate = new ArrayList<PageEntry>(updatedChildPointers.size());
                    for (int i = 0; i < updatedChildPointers.size(); i++) {
                        PagePointer cp = updatedChildPointers.get(i);
                        ByteArraySegment key = cp.key;
                        if (i == 0 && parentPage.getPage().getCount() == 0) {
                            key = generateMinKey();
                        }
                        PageEntry pe = new PageEntry(key, serializePointer(cp));
                        toUpdate.add(pe);
                    }

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

    private CompletableFuture<PageWrapper> locatePage(ByteArraySegment key, PageCollection pageCollection, TimeoutTimer timer) {
        return this.dataSource
                .getState(timer.getRemaining())
                .thenComposeAsync(state -> {
                    if (pageCollection.isInitialized()) {
                        Preconditions.checkArgument(pageCollection.getIndexLength() == state.length, "Unexpected length.");
                    } else {
                        pageCollection.initialize(state.length);
                    }

                    if (state.rootPageOffset == NO_OFFSET && pageCollection.getCount() == 0) {
                        // No data. Return an empty (leaf) page, which will serve as the root for now.
                        return CompletableFuture.completedFuture(pageCollection.insert(PageWrapper.wrapNew(createEmptyLeafPage(), null, null)));
                    }

                    AtomicReference<PagePointer> toFetch = new AtomicReference<>(new PagePointer(null, state.rootPageOffset, state.rootPageLength));
                    CompletableFuture<PageWrapper> result = new CompletableFuture<>();
                    AtomicReference<PageWrapper> parentPage = new AtomicReference<>(null);
                    Futures.loop(
                            () -> !result.isDone(),
                            () -> fetchPage(toFetch.get(), parentPage.get(), pageCollection, timer.getRemaining())
                                    .thenAccept(page -> {
                                        if (page.isIndexPage()) {
                                            PagePointer childPointer = getPagePointer(key, page.getPage());
                                            toFetch.set(childPointer);
                                            parentPage.set(page);
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
                }, this.executor);
    }

    private CompletableFuture<PageWrapper> fetchPage(PagePointer pagePointer, PageWrapper parentPage, PageCollection pageCollection, Duration timeout) {
        PageWrapper fromCache = pageCollection.get(pagePointer.offset);
        if (fromCache != null) {
            return CompletableFuture.completedFuture(fromCache);
        }

        return this.dataSource
                .readPage(pagePointer.offset, pagePointer.length, timeout)
                .thenApply(data -> {
                    val pageConfig = BTreePage.isIndexPage(data) ? this.indexPageConfig : this.leafPageConfig;
                    return pageCollection.insert(PageWrapper.wrapExisting(new BTreePage(pageConfig, data), parentPage, pagePointer));
                });
    }

    private BTreePage createEmptyLeafPage() {
        return new BTreePage(this.leafPageConfig);
    }

    private BTreePage createEmptyIndexPage() {
        return new BTreePage(this.indexPageConfig);
    }

    private PagePointer getPagePointer(ByteArraySegment key, BTreePage page) {
        val searchResult = page.search(key, 0);
        int pos = searchResult.isExactMatch() ? searchResult.getPosition() : searchResult.getPosition() - 1;
        assert pos >= 0 : "negative pos";
        return deserializePointer(page.getValueAt(pos), page.getKeyAt(pos));
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

    private ByteArraySegment generateMinKey() {
        byte[] result = new byte[this.indexPageConfig.getKeyLength()];
        Arrays.fill(result, Byte.MIN_VALUE);
        return new ByteArraySegment(result);
    }

    //endregion

    //region Page Wrappers

    @RequiredArgsConstructor
    private static class PagePointer {
        final ByteArraySegment key;
        final long offset;
        final int length;

        @Override
        public String toString() {
            return String.format("Offset = %s, Length = %s", this.offset, this.length);
        }
    }

    private static class PageWrapper {
        private final AtomicReference<BTreePage> page;
        @Getter
        private final PageWrapper parent;
        @Getter
        private final PagePointer pointer;
        @Getter
        private final boolean newPage;
        private final AtomicLong assignedOffset;

        private PageWrapper(BTreePage page, PageWrapper parent, PagePointer pointer, boolean newPage) {
            this.page = new AtomicReference<>(page);
            this.parent = parent;
            this.pointer = pointer;
            this.newPage = newPage;
            this.assignedOffset = new AtomicLong(this.pointer == null ? NO_OFFSET : this.pointer.offset);
        }

        static PageWrapper wrapExisting(BTreePage page, PageWrapper parent, PagePointer pointer) {
            return new PageWrapper(page, parent, pointer, false);
        }

        static PageWrapper wrapNew(BTreePage page, PageWrapper parent, PagePointer pointer) {
            return new PageWrapper(page, parent, pointer, true);
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
            if (this.pointer != null && this.assignedOffset.get() != this.pointer.offset) {
                // We have already assigned an offset to this.
                throw new IllegalStateException("Cannot assign offset more than once.");
            }

            this.assignedOffset.set(value);
        }

        boolean isNewPage() {
            return this.pointer == null;
        }
    }

    /**
     * Volatile Page Cache for any tree Page that is loaded (and implicitly modified).
     */
    private static class PageCollection {
        private final HashMap<Long, PageWrapper> pageByOffset = new HashMap<>();
        private final AtomicLong incompleteNewPageOffset = new AtomicLong(NO_OFFSET);
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

        PageWrapper insert(PageWrapper page) {
            Preconditions.checkArgument(this.incompleteNewPageOffset.get() == NO_OFFSET, "Cannot insert new page while a new page is incomplete.");
            if (page.isNewPage()) {
                this.incompleteNewPageOffset.set(page.getAssignedOffset());
            }

            this.pageByOffset.put(page.getAssignedOffset(), page);
            return page;
        }

        void complete(PageWrapper page) {
            Preconditions.checkArgument(this.pageByOffset.containsKey(page.getAssignedOffset()), "Given page is not registered.");
            Preconditions.checkArgument(this.incompleteNewPageOffset.get() == NO_OFFSET || this.incompleteNewPageOffset.get() == page.getAssignedOffset(),
                    "Not expecting this page to be completed.");
            this.incompleteNewPageOffset.set(NO_OFFSET);
            long pageOffset = this.indexLength.getAndAdd(page.getPage().getLength());
            this.pageByOffset.remove(page.getAssignedOffset());
            page.setAssignedOffset(pageOffset);
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

    //region DataSource

    private static class DataSource {
        private static final int FOOTER_LENGTH = Long.BYTES;
        private final Read read;
        private final Write write;
        private final GetLength getLength;
        private final AtomicReference<State> state;

        DataSource(@NonNull Read read, @NonNull Write write, @NonNull GetLength getLength) {
            this.read = Preconditions.checkNotNull(read, "read");
            this.write = Preconditions.checkNotNull(write, "write");
            this.getLength = Preconditions.checkNotNull(getLength, "getLength");
            this.state = new AtomicReference<>();
        }

        CompletableFuture<ByteArraySegment> readPage(long offset, int length, Duration timeout) {
            return this.read.apply(offset, length, timeout);
        }

        CompletableFuture<Void> writePages(PageCollection pageCollection, Duration timeout) {
            //System.out.println("WritePages");
            State state = this.state.get();
            Preconditions.checkState(state != null, "Cannot write without fetching the state first.");

            // Collect the data to be written.
            val streams = new ArrayList<InputStream>();
            long offset = NO_OFFSET;
            AtomicInteger length = new AtomicInteger();
            PageWrapper lastPage = null;
            for (PageWrapper p : pageCollection.orderPagesByIndex()) {
                //System.out.println(String.format("\t%s %s", p.getAssignedOffset(), p.getPage().getContents().getLength()));
                if (offset >= 0) {
                    Preconditions.checkArgument(p.getAssignedOffset() == offset, "Expecting Page offset %s, found %s.", offset, p.getAssignedOffset());
                }

                streams.add(p.getPage().getContents().getReader());
                offset = p.getAssignedOffset() + p.getPage().getLength();
                length.addAndGet(p.getPage().getLength());
                lastPage = p;
            }

            // Write a footer with information about locating the root page.
            Preconditions.checkArgument(lastPage != null && lastPage.getParent() == null, "Last page to be written is not the root page");
            Preconditions.checkArgument(pageCollection.getIndexLength() == state.length + length.get(), "IndexLength mismatch.");
            streams.add(getFooter(lastPage.getAssignedOffset()));
            length.addAndGet(FOOTER_LENGTH);

            // Write it.
            val toWrite = new SequenceInputStream(Collections.enumeration(streams));
            long lastPageOffset = lastPage.getAssignedOffset();
            int lastPageLength = lastPage.getPage().getContents().getLength();
            return this.write.apply(this.state.get().length, toWrite, length.get(), timeout)
                             .thenRun(() -> setState(state.length + length.get(), lastPageOffset, lastPageLength));
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
                            return CompletableFuture.completedFuture(setState(length, NO_OFFSET, 0));
                        }

                        return this.read
                                .apply(length - FOOTER_LENGTH, FOOTER_LENGTH, timer.getRemaining())
                                .thenApply(footer -> {
                                    long rootPageOffset = getRootPageOffset(footer);
                                    return setState(length, rootPageOffset, (int) (length - FOOTER_LENGTH - rootPageOffset));
                                });
                    });
        }

        private State setState(long length, long rootPageOffset, int rootPageLength) {
            State s = new State(length, rootPageOffset, rootPageLength);
            this.state.set(s);
            return s;
        }

        private InputStream getFooter(long rootPageOffset) {
            byte[] result = new byte[FOOTER_LENGTH];
            BitConverter.writeLong(result, 0, rootPageOffset);
            return new ByteArrayInputStream(result);
        }

        private long getRootPageOffset(ByteArraySegment footer) {
            if (footer.getLength() != FOOTER_LENGTH) {
                throw new IllegalDataFormatException(String.format("Wrong footer length. Expected %s, actual %s.", FOOTER_LENGTH, footer.getLength()));
            }

            return BitConverter.readLong(footer, 0);
        }

        @RequiredArgsConstructor
        static class State {
            private final long length;
            private final long rootPageOffset;
            private final int rootPageLength;

            @Override
            public String toString() {
                return String.format("Length = %s, RootOffset = %s, RootLength = %s", this.length, this.rootPageOffset, this.rootPageLength);
            }
        }
    }

    //endregion

    //region Functional Interfaces

    @FunctionalInterface
    public interface GetLength {
        CompletableFuture<Long> apply(Duration timeout);
    }

    @FunctionalInterface
    public interface Read {
        CompletableFuture<ByteArraySegment> apply(long offset, int length, Duration timeout);
    }

    @FunctionalInterface
    public interface Write {
        CompletableFuture<Void> apply(long expectedOffset, InputStream data, int length, Duration timeout);
    }

    //endregion
}

