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

    /**
     * Creates a new instance of the BTreeIndex class.
     *
     * @param maxPageSize Maximum page size. No BTreeIndex Page will exceed this value.
     * @param keyLength   The length, in bytes, of the index Keys.
     * @param valueLength The length, in bytes, of the index Values.
     * @param readPage    A Function that reads the contents of a page from an external data source.
     * @param writePages  A Function that writes contents of one or more contiguous pages to an external data source.
     * @param getLength   A Function that returns the length of the index, in bytes, as stored in an external data source.
     * @param executor    Executor for async operations.
     */
    @Builder
    public BTreeIndex(int maxPageSize, int keyLength, int valueLength, @NonNull ReadPage readPage, @NonNull WritePages writePages,
                      @NonNull GetLength getLength, @NonNull Executor executor) {
        Preconditions.checkNotNull(readPage, "readPage");
        Preconditions.checkNotNull(writePages, "writePages");
        Preconditions.checkNotNull(getLength, "getLength");
        this.dataSource = new DataSource(readPage, writePages, getLength);
        this.executor = Preconditions.checkNotNull(executor, "executor");

        // BTreePage.Config validates the arguments so we don't need to.
        this.indexPageConfig = new BTreePage.Config(keyLength, INDEX_VALUE_LENGTH, maxPageSize, true);
        this.leafPageConfig = new BTreePage.Config(keyLength, valueLength, maxPageSize, false);
    }

    //endregion

    //region Operations

    /**
     * Looks up the value of a single key.
     *
     * @param key     A ByteArraySegment representing the key to look up.
     * @param timeout Timeout for the operation.
     * @return A CompletableFuture that, when completed normally, will contain the value associated with the given key.
     * If no value is associated with this key, the Future will complete with null. If the operation failed, the Future
     * will be completed with the appropriate exception.
     */
    public CompletableFuture<ByteArraySegment> get(@NonNull ByteArraySegment key, @NonNull Duration timeout) {
        TimeoutTimer timer = new TimeoutTimer(timeout);

        // Lookup the page where the Key should exist (if at all).
        PageCollection pageCollection = new PageCollection();
        return locatePage(key, pageCollection, timer)
                .thenApplyAsync(page -> page.getPage().searchExact(key), this.executor);
    }

    /**
     * Looks up the value of multiple keys.
     *
     * @param keys    A list of ByteArraySegments representing the keys to look up.
     * @param timeout Timeout for the operation.
     * @return A CompletableFuture that, when completed normally, will contain a List with ByteArraySegments representing
     * the values associated with the given keys. The values in this list will be in the same order as the given Keys, so
     * they can be matched to their sought keys by their index. If the operation failed, the Future
     * will be completed with the appropriate exception.
     */
    public CompletableFuture<List<ByteArraySegment>> get(@NonNull List<ByteArraySegment> keys, @NonNull Duration timeout) {
        if (keys.size() == 1) {
            // Shortcut for single key.
            return get(keys.get(0), timeout).thenApply(Collections::singletonList);
        }

        // Lookup all the keys in parallel, and make sure to apply their resulting values in the same order that their keys
        // where provided to us.
        TimeoutTimer timer = new TimeoutTimer(timeout);
        PageCollection pageCollection = new PageCollection();
        return this.dataSource
                .getState(timer.getRemaining())
                .thenApplyAsync(state -> {
                    pageCollection.initialize(state.length);
                    return keys.stream()
                            .map(key -> locatePage(key, pageCollection, timer).thenApplyAsync(page -> page.getPage().searchExact(key), this.executor))
                            .collect(Collectors.toList());
                }, this.executor)
                .thenCompose(Futures::allOfWithResults);
    }

    /**
     * Inserts the given Page Entries into the index.
     *
     * @param entries A Collection of Page Entries to insert. The collection need not be sorted.
     * @param timeout Timeout for the operation.
     * @return A CompletableFuture that, when completed normally, will indicate that the entries have been inserted
     * successfully and will contain the current version of the index (any modifications to the index will result in a
     * larger version value). If the operation failed, the Future will be completed with the appropriate exception.
     */
    public CompletableFuture<Long> insert(@NonNull Collection<PageEntry> entries, @NonNull Duration timeout) {
        TimeoutTimer timer = new TimeoutTimer(timeout);
        return insertIntoPages(entries, timer)
                .thenApply(this::processModifiedPages)
                .thenComposeAsync(pageCollection -> this.dataSource.writePages(pageCollection, timer.getRemaining()), this.executor);
    }

    /**
     * Removes the given Keys and their associated values from the Index.
     *
     * @param keys    A Collection of Keys to remove. The collection need not be sorted.
     * @param timeout Timeout for the operation.
     * @return A CompletableFuture that, when completed normally, will indicate that the entries have been removed
     * successfully and will contain the current version of the index (any modifications to the index will result in a
     * larger version value). If the operation failed, the Future will be completed with the appropriate exception.
     */
    public CompletableFuture<Long> delete(@NonNull Collection<ByteArraySegment> keys, @NonNull Duration timeout) {
        TimeoutTimer timer = new TimeoutTimer(timeout);
        return deleteFromPages(keys, timer)
                .thenApply(this::processModifiedPages)
                .thenComposeAsync(pageCollection -> this.dataSource.writePages(pageCollection, timer.getRemaining()), this.executor);
    }

    public CompletableFuture<Long> getCount(@NonNull Duration timeout) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    public Object iterateKeys(@NonNull ByteArraySegment fromKey, boolean fromInclusive, @NonNull ByteArraySegment toKey, boolean toInclusive) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    //endregion

    //region Helpers

    /**
     * Removes the given Keys and their associated values from the Index.
     *
     * @param keys  A Collection of Keys to remove.
     * @param timer Timer for the operation.
     * @return A CompletableFuture that will contain a PageCollection of touched pages.
     */
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

    /**
     * Inserts the given Page Entries into the Index.
     *
     * @param entries A Collection of Page Entries to insert.
     * @param timer   Timer for the operation.
     * @return A CompletableFuture that will contain a PageCollection of touched pages.
     */
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

    /**
     * Processes all the pages in the given PageCollection, beginning with the Leaf Pages (in order of offsets), then
     * moving on to their direct parents (in order of offsets), and so on until all pages in the Page Collection have
     * been processed.
     * <p>
     * When this method completes:
     * - All pages will have new offsets assigned (since they have all been touched).
     * - Any overflowing pages will be split into two or more pages (and their parents updated appropriately).
     * - Any empty pages will be removed (and their parents updated appropriately).
     * - All non-leaf pages will be updated to point to the same leaf pages, but with new offsets.
     * - A new root (index) page may be created.
     *
     * @param pageCollection A PageCollection containing pages to be processed.
     * @return The PageCollection passed in.
     */
    private PageCollection processModifiedPages(PageCollection pageCollection) {
        val currentBatch = new ArrayList<PageWrapper>();
        pageCollection.collectLeafPages(currentBatch);
        while (!currentBatch.isEmpty()) {
            val parents = new HashSet<Long>();
            for (PageWrapper p : currentBatch) {
                val updatedChildPointers = new ArrayList<PagePointer>();
                ByteArraySegment deletedPageKey = null;
                val splitResult = p.getPage().splitIfNecessary();
                if (splitResult != null) {
                    // This Page must be split. The first split page will replace the existing page, while the rest will
                    // need to be inserted as children into the parent.
                    for (int i = 0; i < splitResult.size(); i++) {
                        val page = splitResult.get(i);
                        ByteArraySegment newPageKey;
                        long newOffset;
                        if (i == 0) {
                            // The original page will get the first split. Nothing changes about its pointer key.
                            p.setPage(page);
                            newPageKey = p.getPageKey();
                            pageCollection.complete(p);
                            newOffset = p.getOffset();
                        } else {
                            // Insert the new pages and assign them new virtual offsets. Each page will use its first
                            // Key as a Page Key.
                            newPageKey = page.getKeyAt(0);
                            PageWrapper newPage = PageWrapper.wrapNew(page, p.getParent(), new PagePointer(newPageKey, NO_OFFSET, page.getLength()));
                            pageCollection.insert(newPage);
                            pageCollection.complete(newPage);
                            newOffset = newPage.getOffset();
                        }

                        updatedChildPointers.add(new PagePointer(newPageKey, newOffset, page.getLength()));
                    }
                } else {
                    // If this page became empty, we must remove it.
                    // Otherwise, we need to update its pointer in the parent.
                    int entryCount = p.getPage().getCount();
                    ByteArraySegment pageKey = p.getPageKey();
                    if (entryCount == 0 && p.parent != null) {
                        // This page is empty. Remove it from the PageCollection (so we don't write to our data source)
                        // and remember its Page Key so we can delete that from its parent.
                        pageCollection.remove(p);
                        deletedPageKey = pageKey;
                    } else {
                        // This page needs to be kept around.
                        pageCollection.complete(p);
                        updatedChildPointers.add(new PagePointer(pageKey, p.getOffset(), p.getPage().getLength()));
                    }
                }


                // Make sure we process its parent as well.
                PageWrapper parentPage = p.getParent();
                if (parentPage == null && splitResult != null) {
                    // There was a split. Make sure that if this was the root, we create a new root.
                    parentPage = PageWrapper.wrapNew(createEmptyIndexPage(), null, null);
                    pageCollection.insert(parentPage);
                }

                if (parentPage != null) {
                    if (deletedPageKey != null) {
                        parentPage.getPage().delete(Collections.singleton(deletedPageKey));
                    } else {
                        // Update child pointers for the parent.
                        val toUpdate = new ArrayList<PageEntry>(updatedChildPointers.size());
                        for (int i = 0; i < updatedChildPointers.size(); i++) {
                            PagePointer cp = updatedChildPointers.get(i);
                            ByteArraySegment key = cp.key;
                            if (i == 0 && parentPage.getPage().getCount() == 0) {
                                // Special case for index nodes. Since each Page Pointer points to a Page which contains all Keys
                                // larger than their Pointer Key (but smaller than the next Pointer Key), the first entry must
                                // be the minimum possible Key, otherwise we will not be able to insert any values smaller than
                                // this Pointer Key.
                                key = generateMinKey();
                            }
                            PageEntry pe = new PageEntry(key, serializePointer(cp));
                            toUpdate.add(pe);
                        }

                        // Update parent page's child pointer for modified pages.
                        parentPage.getPage().update(toUpdate);
                    }

                    // Then queue up the parent for processing.
                    parents.add(parentPage.getOffset());
                }
            }

            // We are done with this level. Move up one process the pages at that level.
            currentBatch.clear();
            pageCollection.collectPages(parents, currentBatch);
        }

        return pageCollection;
    }

    /**
     * Locates the Leaf Page that contains or should contain the given Key.
     *
     * @param key            A ByteArraySegment that represents the Key to look up the Leaf Page for.
     * @param pageCollection A PageCollection that contains already looked-up pages. Any newly looked up pages will be
     *                       inserted in this instance as well.
     * @param timer          Timer for the operation.
     * @return A CompletableFuture with a PageWrapper for the sought page.
     */
    private CompletableFuture<PageWrapper> locatePage(ByteArraySegment key, PageCollection pageCollection, TimeoutTimer timer) {
        Preconditions.checkArgument(key.getLength() == this.leafPageConfig.getKeyLength(), "Invalid key length.");
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

                    AtomicReference<PagePointer> pagePointer = new AtomicReference<>(new PagePointer(null, state.rootPageOffset, state.rootPageLength));
                    CompletableFuture<PageWrapper> result = new CompletableFuture<>();
                    AtomicReference<PageWrapper> parentPage = new AtomicReference<>(null);
                    Futures.loop(
                            () -> !result.isDone(),
                            () -> fetchPage(pagePointer.get(), parentPage.get(), pageCollection, timer.getRemaining())
                                    .thenAccept(page -> {
                                        if (page.isIndexPage()) {
                                            PagePointer childPointer = getPagePointer(key, page.getPage());
                                            pagePointer.set(childPointer);
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

    /**
     * Loads up a single Page.
     *
     * @param pagePointer    A PagePointer indicating the Page to load.
     * @param parentPage     The sought page's Parent Page. May be null for root pages only.
     * @param pageCollection A PageCollection that contains already looked up pages. If the sought page is already loaded
     *                       it will be served from here; otherwise it will be added here afterwards.
     * @param timeout        Timeout for the operation.
     * @return A CompletableFuture containing a PageWrapper for the sought page.
     */
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

    private PagePointer deserializePointer(ByteArraySegment serialization, ByteArraySegment pageKey) {
        long pageOffset = BitConverter.readLong(serialization, 0);
        int pageLength = BitConverter.readInt(serialization, Long.BYTES);
        return new PagePointer(pageKey, pageOffset, pageLength);
    }

    private ByteArraySegment generateMinKey() {
        byte[] result = new byte[this.indexPageConfig.getKeyLength()];
        Arrays.fill(result, Byte.MIN_VALUE);
        return new ByteArraySegment(result);
    }

    //endregion

    //region Page Wrappers

    /**
     * Pointer to a BTreePage.
     */
    private static class PagePointer {
        final ByteArraySegment key;
        final long offset;
        final int length;

        PagePointer(ByteArraySegment key, long offset, int length) {
            // Make a copy of the key. ByteArraySegments are views into another array (of the BTreePage). As such, if the
            // BTreePage is modified (in case the first key is deleted), this view may return a different set of bytes.
            this.key = key == null ? null : new ByteArraySegment(key.getCopy());
            this.offset = offset;
            this.length = length;
        }

        @Override
        public String toString() {
            return String.format("Offset = %s, Length = %s", this.offset, this.length);
        }
    }

    /**
     * Wraps a BTreePage by adding additional metadata, such as parent information and offset.
     */
    private static class PageWrapper {
        private final AtomicReference<BTreePage> page;
        @Getter
        private final PageWrapper parent;
        @Getter
        private final PagePointer pointer;
        @Getter
        private final boolean newPage;
        private final AtomicLong offset;

        private PageWrapper(BTreePage page, PageWrapper parent, PagePointer pointer, boolean newPage) {
            this.page = new AtomicReference<>(page);
            this.parent = parent;
            this.pointer = pointer;
            this.newPage = newPage;
            this.offset = new AtomicLong(this.pointer == null ? NO_OFFSET : this.pointer.offset);
        }

        /**
         * Creates a new instance of the PageWrapper class for an existing Page.
         *
         * @param page    Page to wrap.
         * @param parent  Page's Parent.
         * @param pointer Page Pointer.
         */
        static PageWrapper wrapExisting(BTreePage page, PageWrapper parent, PagePointer pointer) {
            return new PageWrapper(page, parent, pointer, false);
        }

        /**
         * Creates a new instance of the PageWrapper class for a new Page.
         *
         * @param page    Page to wrap.
         * @param parent  Page's Parent.
         * @param pointer Page Pointer.
         */
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

        long getOffset() {
            return this.offset.get();
        }

        void setOffset(long value) {
            if (this.pointer != null && this.offset.get() != this.pointer.offset) {
                // We have already assigned an offset to this.
                throw new IllegalStateException("Cannot assign offset more than once.");
            }

            this.offset.set(value);
        }

        boolean isNewPage() {
            return this.pointer == null;
        }

        ByteArraySegment getPageKey() {
            ByteArraySegment r = this.pointer == null ? null : this.pointer.key;
            if (r == null && getPage().getCount() > 0) {
                r = getPage().getKeyAt(0);
            }
            return r;
        }
    }

    /**
     * A Collection of BTreePages, indexed by Offset. This can serve as a cache for any operation (but should not be used
     * cross-operations).
     */
    private static class PageCollection {
        private final HashMap<Long, PageWrapper> pageByOffset = new HashMap<>();
        private final AtomicLong incompleteNewPageOffset = new AtomicLong(NO_OFFSET);
        private final AtomicLong indexLength = new AtomicLong(-1);

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
                this.incompleteNewPageOffset.set(page.getOffset());
            }

            this.pageByOffset.put(page.getOffset(), page);
            return page;
        }

        void remove(PageWrapper page) {
            this.pageByOffset.remove(page.getOffset());
            this.incompleteNewPageOffset.compareAndSet(page.getOffset(), NO_OFFSET);
            page.setOffset(NO_OFFSET);
        }

        void complete(PageWrapper page) {
            Preconditions.checkArgument(this.pageByOffset.containsKey(page.getOffset()), "Given page is not registered.");
            Preconditions.checkArgument(this.incompleteNewPageOffset.get() == NO_OFFSET || this.incompleteNewPageOffset.get() == page.getOffset(),
                    "Not expecting this page to be completed.");
            this.incompleteNewPageOffset.set(NO_OFFSET);
            long pageOffset = this.indexLength.getAndAdd(page.getPage().getLength());
            this.pageByOffset.remove(page.getOffset());
            page.setOffset(pageOffset);
            this.pageByOffset.put(page.getOffset(), page);
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
                    .sorted(Comparator.comparingLong(PageWrapper::getOffset))
                    .collect(Collectors.toList());
        }
    }

    //endregion

    //region DataSource

    /**
     * Data Source adapter for the BTreeIndex.
     */
    @RequiredArgsConstructor
    private static class DataSource {
        private static final int FOOTER_LENGTH = Long.BYTES;
        private final ReadPage read;
        private final WritePages write;
        private final GetLength getLength;
        private final AtomicReference<State> state = new AtomicReference<>();

        /**
         * Reads the contents of a single page from the external data source.
         *
         * @param offset  Page offset.
         * @param length  Page length.
         * @param timeout Timeout for the operation.
         * @return A CompletableFuture with a ByteArraySegment representing the contents of the page.
         */
        CompletableFuture<ByteArraySegment> readPage(long offset, int length, Duration timeout) {
            return this.read.apply(offset, length, timeout);
        }

        /**
         * Writes the contents of all the BTreePages in the given PageCollection to the external data source.
         *
         * @param pageCollection PageCollection with pages.
         * @param timeout        Timeout for the operation.
         * @return A CompletableFuture with a Long representing the current length of the index in the external data source.
         */
        CompletableFuture<Long> writePages(PageCollection pageCollection, Duration timeout) {
            State state = this.state.get();
            Preconditions.checkState(state != null, "Cannot write without fetching the state first.");

            // Collect the data to be written.
            val streams = new ArrayList<InputStream>();
            long offset = NO_OFFSET;
            AtomicInteger length = new AtomicInteger();
            PageWrapper lastPage = null;
            for (PageWrapper p : pageCollection.orderPagesByIndex()) {
                if (offset >= 0) {
                    Preconditions.checkArgument(p.getOffset() == offset, "Expecting Page offset %s, found %s.", offset, p.getOffset());
                }

                streams.add(p.getPage().getContents().getReader());
                offset = p.getOffset() + p.getPage().getLength();
                length.addAndGet(p.getPage().getLength());
                lastPage = p;
            }

            // Write a footer with information about locating the root page.
            Preconditions.checkArgument(lastPage != null && lastPage.getParent() == null, "Last page to be written is not the root page");
            Preconditions.checkArgument(pageCollection.getIndexLength() == state.length + length.get(), "IndexLength mismatch.");
            streams.add(getFooter(lastPage.getOffset()));
            length.addAndGet(FOOTER_LENGTH);

            // Write it.
            val toWrite = new SequenceInputStream(Collections.enumeration(streams));
            long lastPageOffset = lastPage.getOffset();
            int lastPageLength = lastPage.getPage().getContents().getLength();
            return this.write
                    .apply(this.state.get().length, toWrite, length.get(), timeout)
                    .thenApply(v -> {
                        long indexLength = state.length + length.get();
                        setState(indexLength, lastPageOffset, lastPageLength);
                        return indexLength;
                    });
        }

        /**
         * Fetches the state of the index from the external data source. This value may be cached.
         *
         * @param timeout Timeout for the operation.
         * @return A CompletableFuture with the State.
         */
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

    /**
     * Defines a method that, when invoked, returns the Length of the Index in the external data source.
     */
    @FunctionalInterface
    public interface GetLength {
        CompletableFuture<Long> apply(Duration timeout);
    }

    /**
     * Defines a method that, when invoked, reads the contents of a single BTreePage from the external data source.
     */
    @FunctionalInterface
    public interface ReadPage {
        CompletableFuture<ByteArraySegment> apply(long offset, int length, Duration timeout);
    }

    /**
     * Defines a method that, when invoked, writes the contents of contiguous BTreePages to the external data source.
     */
    @FunctionalInterface
    public interface WritePages {
        CompletableFuture<Void> apply(long expectedOffset, InputStream data, int length, Duration timeout);
    }

    //endregion
}

