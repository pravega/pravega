/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.util.btree;

import com.google.common.base.Preconditions;
import io.pravega.common.TimeoutTimer;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.AsyncIterator;
import io.pravega.common.util.BitConverter;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.common.util.IllegalDataFormatException;
import java.time.Duration;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import javax.annotation.concurrent.NotThreadSafe;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.val;

/**
 * B+Tree Index with fixed-length Keys and fixed-length Values operating on an append-only storage medium.
 *
 * Structure:
 * * The B+Tree Index is made up of Pages, not exceeding a specified length.
 * * Pages can be Index Pages (contain pointers to other Pages) or Leaf pages.
 * * Entries are made of Keys and Values, which are byte arrays, and the B+Tree index is oblivious to their meaning.
 * * Entries are sorted internally by Key using a byte array comparator. This sorting order may differ from the sorting
 * order of whatever the Keys represent externally.
 * * If a particular Page (Index or Leaf) exceeds the maximum allowed length, it is split in 2 or more pages of approximately
 * equal length, with the constraint that each new page will contain as many items as possible (i.e., split into the minimum
 * number of possible pages while each page is within the allotted bounds).
 * * If a particular Page becomes empty (as a result of an entry's deletion), the entire page will be deleted (except if
 * it is the root page).
 *
 * Storing data:
 * * Every modification to the B+Tree involves modifying one Leaf Page and all its parent Index Pages (up to, and including
 * the root page). Each such modification will result in all these "touched" pages to be rewritten to the external data source.
 * * Upon writing, the contents of the Index Pages will be updated to reflect the new offsets of their direct child pages.
 * * Upon writing, a fixed-length "footer" is written at the very end which contains information about where the most recent
 * root page is located. This eliminates the need for such a pointer to be stored externally or to have a fixed-length root
 * page, which would be wasteful.
 *
 * Read-write consistency and concurrency:
 * * Due to the append-only nature of the storage media, reads (including iterators) may occur concurrently with writes,
 * without having to worry about consistency. This is because when a read is initiated, it caches the root page (along
 * with any other sub-pages), which will always point to locations with smaller offsets (by construction). Any new writes
 * (concurrent or not) will always operate on higher offsets, thus not interfering with any ongoing reads.
 * * This implementation does not support concurrent writes. Writes will need to be serialized, either within the same
 * instance or across multiple instances. Concurrent writes will be guarded by means of "appending-at-offset" within the
 * external data source (when the BTreeIndex is first used, it gets a snapshot of the external data source which it then
 * uses for conditionally writing modifications).
 * * If two or more concurrent writes occur on the same BTreeIndex instance, only one will succeed (and the others may be
 * retried).
 * * If two or more concurrent writes occur on different BTreeIndex instances, only one will succeed. The "losing" instances
 * will become unavailable for writing and will present an outdated view of the data for reading. This mechanism allows
 * the caller to decide how to properly recover from the situation - the BTreeIndex has insufficient information to make
 * such a decision.
 */
@NotThreadSafe
public class BTreeIndex {
    //region Members

    private static final int INDEX_VALUE_LENGTH = Long.BYTES + Integer.BYTES;
    private static final int FOOTER_LENGTH = Long.BYTES;
    private static final ByteArrayComparator KEY_COMPARATOR = new ByteArrayComparator();
    private final BTreePage.Config indexPageConfig;
    private final BTreePage.Config leafPageConfig;
    private final ReadPage read;
    private final WritePages write;
    private final GetLength getLength;
    private final AtomicReference<IndexState> state;
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
        this.read = readPage;
        this.write = writePages;
        this.getLength = getLength;
        this.executor = executor;

        // BTreePage.Config validates the arguments so we don't need to.
        this.indexPageConfig = new BTreePage.Config(keyLength, INDEX_VALUE_LENGTH, maxPageSize, true);
        this.leafPageConfig = new BTreePage.Config(keyLength, valueLength, maxPageSize, false);
        this.state = new AtomicReference<>();
    }

    //endregion

    //region Operations

    /**
     * Gets a value indicating whether this BTreeIndex instance is initialized or not.
     *
     * @return True if initialized, false otherwise.
     */
    public boolean isInitialized() {
        return this.state.get() != null;
    }

    /**
     * Initializes the BTreeIndex by fetching metadata from the external data source. This method must be invoked (and
     * completed) prior to executing any other operation on this instance.
     *
     * @param timeout Timeout for the operation.
     * @return A CompletableFuture that, when completed, will indicate that the operation completed.
     */
    public CompletableFuture<Void> initialize(Duration timeout) {
        Preconditions.checkArgument(!isInitialized(), "BTreeIndex is already initialized.");
        TimeoutTimer timer = new TimeoutTimer(timeout);
        return this.getLength
                .apply(timer.getRemaining())
                .thenCompose(length -> {
                    if (length <= FOOTER_LENGTH) {
                        // Empty index.
                        setState(length, PagePointer.NO_OFFSET, 0);
                        return CompletableFuture.completedFuture(null);
                    }

                    long footerOffset = getFooterOffset(length);
                    return this.read
                            .apply(footerOffset, FOOTER_LENGTH, timer.getRemaining())
                            .thenAccept(footer -> {
                                long rootPageOffset = getRootPageOffset(footer);
                                setState(length, rootPageOffset, (int) (footerOffset - rootPageOffset));
                            });
                });
    }

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
        ensureInitialized();
        TimeoutTimer timer = new TimeoutTimer(timeout);

        // Lookup the page where the Key should exist (if at all).
        PageCollection pageCollection = new PageCollection(this.state.get().length);
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
        ensureInitialized();
        TimeoutTimer timer = new TimeoutTimer(timeout);
        PageCollection pageCollection = new PageCollection(this.state.get().length);
        val gets = keys.stream()
                .map(key -> locatePage(key, pageCollection, timer)
                        .thenApplyAsync(page -> page.getPage().searchExact(key), this.executor))
                .collect(Collectors.toList());
        return Futures.allOfWithResults(gets);
    }

    /**
     * Inserts or updates the given Page Entries into the index.
     *
     * @param entries A Collection of Page Entries to insert. The collection need not be sorted.
     * @param timeout Timeout for the operation.
     * @return A CompletableFuture that, when completed normally, will indicate that the entries have been inserted
     * successfully and will contain the current version of the index (any modifications to the index will result in a
     * larger version value). If the operation failed, the Future will be completed with the appropriate exception.
     */
    public CompletableFuture<Long> put(@NonNull Collection<PageEntry> entries, @NonNull Duration timeout) {
        ensureInitialized();
        TimeoutTimer timer = new TimeoutTimer(timeout);
        return insertIntoPages(entries, timer)
                .thenApply(this::processModifiedPages)
                .thenComposeAsync(pageCollection -> writePages(pageCollection, timer.getRemaining()), this.executor);
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
    public CompletableFuture<Long> remove(@NonNull Collection<ByteArraySegment> keys, @NonNull Duration timeout) {
        ensureInitialized();
        TimeoutTimer timer = new TimeoutTimer(timeout);
        return deleteFromPages(keys, timer)
                .thenApply(this::processModifiedPages)
                .thenComposeAsync(pageCollection -> writePages(pageCollection, timer.getRemaining()), this.executor);
    }

    /**
     * Returns an AsyncIterator that will iterate through all the keys within the specified bounds. All iterated keys will
     * be returned in order (smallest to largest).
     *
     * @param firstKey          A ByteArraySegment representing the lower bound of the iteration.
     * @param firstKeyInclusive If true, firstKey will be included in the iteration (if it exists in the index), otherwise
     *                          it will not.
     * @param lastKey           A ByteArraySegment representing the upper bound of the iteration.
     * @param lastKeyInclusive  If true, lastKey will be included in the iteration (if it exists in the index), otherwise
     *                          it will not.
     * @param fetchTimeout      Timeout for each invocation of AsyncIterator.getNext().
     * @return A new AsyncIterator instance.
     */
    public AsyncIterator<List<PageEntry>> iterator(@NonNull ByteArraySegment firstKey, boolean firstKeyInclusive,
                                                   @NonNull ByteArraySegment lastKey, boolean lastKeyInclusive, Duration fetchTimeout) {
        ensureInitialized();
        return new EntryIterator(firstKey, firstKeyInclusive, lastKey, lastKeyInclusive, this::locatePage, this.state.get().length, fetchTimeout);
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

        PageCollection pageCollection = new PageCollection(this.state.get().length);
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

        PageCollection pageCollection = new PageCollection(this.state.get().length);
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
                val context = new PageModificationContext(p, pageCollection);
                if (p.needsFirstKeyUpdate()) {
                    updateFirstKey(context);
                }

                val splitResult = p.getPage().splitIfNecessary();
                if (splitResult != null) {
                    processSplitPage(splitResult, context);
                } else {
                    processModifiedPage(context);
                }

                // Make sure we process its parent as well.
                PageWrapper parentPage = p.getParent();
                if (parentPage == null && splitResult != null) {
                    // There was a split. Make sure that if this was the root, we create a new root to act as parent.
                    parentPage = PageWrapper.wrapNew(createEmptyIndexPage(), null, null);
                    pageCollection.insert(parentPage);
                }

                if (parentPage != null) {
                    // Update the parent page with new or deleted Page Pointers.
                    processParentPage(parentPage, context);

                    // Queue up the parent for processing.
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
     * Handles a special case for Index Nodes. Since each Page Pointer points to a Page which contains all Keys equal to
     * or larger than their Pointer Key (but smaller than the next Pointer Key in the Page's parent), the first key in that
     * Page must be the minimum possible key, otherwise we will not be able to insert any values smaller than this Pointer Key,
     * but which would have to go in that Page.
     *
     * @param context Processing Context.
     */
    private void updateFirstKey(PageModificationContext context) {
        BTreePage page = context.getPageWrapper().getPage();
        assert page.getConfig().isIndexPage() : "expected index page";
        if (page.getCount() > 0) {
            page.setFirstKey(generateMinKey());
        }
    }

    /**
     * Processes a Page Split result. The first split page will replace the existing page, while the remaining pages
     * will need to be inserted as children into the parent.
     *
     * @param splitResult The result of the original BTreePage's splitIfNecessary() call.
     * @param context     Processing context.
     */
    private void processSplitPage(List<BTreePage> splitResult, PageModificationContext context) {
        PageWrapper originalPage = context.getPageWrapper();
        for (int i = 0; i < splitResult.size(); i++) {
            val page = splitResult.get(i);
            ByteArraySegment newPageKey;
            long newOffset;
            if (i == 0) {
                // The original page will be replaced by the first split. Nothing changes about its pointer key.
                originalPage.setPage(page);
                newPageKey = originalPage.getPageKey();
                context.getPageCollection().complete(originalPage);
                newOffset = originalPage.getOffset();
            } else {
                // Insert the new pages and assign them new virtual offsets. Each page will use its first
                // Key as a Page Key.
                newPageKey = page.getKeyAt(0);
                PageWrapper newPage = PageWrapper.wrapNew(page, originalPage.getParent(), new PagePointer(newPageKey, PagePointer.NO_OFFSET, page.getLength()));
                context.getPageCollection().insert(newPage);
                context.getPageCollection().complete(newPage);
                newOffset = newPage.getOffset();
            }

            context.updatePagePointer(new PagePointer(newPageKey, newOffset, page.getLength()));
        }
    }

    /**
     * Processes a BTreePage that has been modified (but not split).
     *
     * @param context Processing context.
     */
    private void processModifiedPage(PageModificationContext context) {
        PageWrapper page = context.getPageWrapper();
        boolean emptyPage = page.getPage().getCount() == 0;
        ByteArraySegment pageKey = page.getPageKey();
        if (emptyPage && page.getParent() != null) {
            // This page is empty. Remove it from the PageCollection (so we don't write it back to our data source)
            // and remember its Page Key so we can delete its pointer from its parent page.
            context.getPageCollection().remove(page);
            context.setDeletedPageKey(pageKey);
        } else {
            // This page needs to be kept around.
            if (emptyPage && page.getPage().getConfig().isIndexPage()) {
                // We have an empty Index Root Page. We must convert this to a Leaf Page before moving on.
                page.setPage(createEmptyLeafPage());
            }

            // Assign a new offset to the page and record its new Page Pointer.
            context.pageCollection.complete(page);
            context.updatePagePointer(new PagePointer(pageKey, page.getOffset(), page.getPage().getLength()));
        }
    }

    /**
     * Processes the parent BTreePage after modifying one of its child pages. This involves removing Pointers to deleted
     * child pages, updating pointers to existing pages and inserting pointers for new pages (results of splits).
     *
     * @param parentPage The parent page.
     * @param context    Processing Context.
     */
    private void processParentPage(PageWrapper parentPage, PageModificationContext context) {
        if (context.getDeletedPageKey() != null) {
            // We have a deleted page. Remove its pointer from the parent.
            parentPage.getPage().delete(Collections.singleton(context.getDeletedPageKey()));
            parentPage.markNeedsFirstKeyUpdate();
        } else {
            // Update parent page's child pointers for modified pages.
            val toUpdate = context.getUpdatedPagePointers().stream()
                                  .map(pp -> new PageEntry(pp.getKey(), serializePointer(pp)))
                                  .collect(Collectors.toList());
            parentPage.getPage().update(toUpdate);
        }
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
        Preconditions.checkArgument(pageCollection.getIndexLength() == this.state.get().length, "Unexpected length.");

        if (this.state.get().rootPageOffset == PagePointer.NO_OFFSET && pageCollection.getCount() == 0) {
            // No data. Return an empty (leaf) page, which will serve as the root for now.
            return CompletableFuture.completedFuture(pageCollection.insert(PageWrapper.wrapNew(createEmptyLeafPage(), null, null)));
        }

        AtomicReference<PagePointer> pagePointer = new AtomicReference<>(new PagePointer(null, this.state.get().rootPageOffset, this.state.get().rootPageLength));
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
        PageWrapper fromCache = pageCollection.get(pagePointer.getOffset());
        if (fromCache != null) {
            return CompletableFuture.completedFuture(fromCache);
        }

        return readPage(pagePointer.getOffset(), pagePointer.getLength(), timeout)
                .thenApply(data -> {
                    if (data.getLength() != pagePointer.getLength()) {
                        throw new IllegalDataFormatException(String.format("Requested page of length %s from offset %s, got a page of length %s.",
                                pagePointer.getLength(), pagePointer.getOffset(), data.getLength()));
                    }

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
        BitConverter.writeLong(result, 0, pointer.getOffset());
        BitConverter.writeInt(result, Long.BYTES, pointer.getLength());
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

    //region External Access

    /**
     * Reads the contents of a single page from the external data source.
     *
     * @param offset  Page offset.
     * @param length  Page length.
     * @param timeout Timeout for the operation.
     * @return A CompletableFuture with a ByteArraySegment representing the contents of the page.
     */
    private CompletableFuture<ByteArraySegment> readPage(long offset, int length, Duration timeout) {
        return this.read.apply(offset, length, timeout);
    }

    /**
     * Writes the contents of all the BTreePages in the given PageCollection to the external data source.
     *
     * @param pageCollection PageCollection with pages.
     * @param timeout        Timeout for the operation.
     * @return A CompletableFuture with a Long representing the current length of the index in the external data source.
     */
    private CompletableFuture<Long> writePages(PageCollection pageCollection, Duration timeout) {
        IndexState state = this.state.get();
        Preconditions.checkState(state != null, "Cannot write without fetching the state first.");

        // Collect the data to be written.
        val pages = new ArrayList<Map.Entry<Long, ByteArraySegment>>();
        val oldOffsets = new ArrayList<Long>();
        long offset = state.length;
        PageWrapper lastPage = null;
        for (PageWrapper p : pageCollection.getPagesSortedByOffset()) {
            if (offset >= 0) {
                Preconditions.checkArgument(p.getOffset() == offset, "Expecting Page offset %s, found %s.", offset, p.getOffset());
            }

            // Collect the page, as well as its previous offset.
            pages.add(new AbstractMap.SimpleImmutableEntry<>(offset, p.getPage().getContents()));
            if (p.getPointer() != null && p.getPointer().getOffset() >= 0) {
                oldOffsets.add(p.getPointer().getOffset());
            }

            offset = p.getOffset() + p.getPage().getLength();
            lastPage = p;
        }

        // Write a footer with information about locating the root page.
        Preconditions.checkArgument(lastPage != null && lastPage.getParent() == null, "Last page to be written is not the root page");
        Preconditions.checkArgument(pageCollection.getIndexLength() == offset, "IndexLength mismatch.");
        pages.add(new AbstractMap.SimpleImmutableEntry<>(offset, getFooter(lastPage.getOffset())));

        // Also collect the old footer's offset, as it will be replaced by a more recent value.
        long oldFooterOffset = getFooterOffset(state.length);
        if (oldFooterOffset >= 0) {
            oldOffsets.add(oldFooterOffset);
        }

        // Write it.
        long lastPageOffset = lastPage.getOffset();
        int lastPageLength = lastPage.getPage().getContents().getLength();
        return this.write.apply(pages, oldOffsets, timeout)
                         .thenApply(indexLength -> setState(indexLength, lastPageOffset, lastPageLength).length);
    }

    private IndexState setState(long length, long rootPageOffset, int rootPageLength) {
        IndexState s = new IndexState(length, rootPageOffset, rootPageLength);
        this.state.set(s);
        return s;
    }

    private long getFooterOffset(long indexLength) {
        return indexLength - FOOTER_LENGTH;
    }

    private ByteArraySegment getFooter(long rootPageOffset) {
        byte[] result = new byte[FOOTER_LENGTH];
        BitConverter.writeLong(result, 0, rootPageOffset);
        return new ByteArraySegment(result);
    }

    private long getRootPageOffset(ByteArraySegment footer) {
        if (footer.getLength() != FOOTER_LENGTH) {
            throw new IllegalDataFormatException(String.format("Wrong footer length. Expected %s, actual %s.", FOOTER_LENGTH, footer.getLength()));
        }

        return BitConverter.readLong(footer, 0);
    }

    private void ensureInitialized() {
        Preconditions.checkArgument(isInitialized(), "BTreeIndex is not initialized.");
    }

    //endregion

    //region PageModificationContext

    @RequiredArgsConstructor
    @Getter
    private static class PageModificationContext {
        private final PageWrapper pageWrapper;
        private final PageCollection pageCollection;
        private final List<PagePointer> updatedPagePointers = new ArrayList<>();
        @Setter
        private ByteArraySegment deletedPageKey;

        void updatePagePointer(PagePointer pp) {
            this.updatedPagePointers.add(pp);
        }
    }

    //endregion

    //region IndexState

    @RequiredArgsConstructor
    private static class IndexState {
        private final long length;
        private final long rootPageOffset;
        private final int rootPageLength;

        @Override
        public String toString() {
            return String.format("Length = %s, RootOffset = %s, RootLength = %s", this.length, this.rootPageOffset, this.rootPageLength);
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
        /**
         * Reads a single Page from an external data source.
         *
         * @param offset  The offset of the desired Page.
         * @param length  The length of the desired Page.
         * @param timeout Timeout for the operation.
         * @return A CompletableFuture that, when completed, will contain a ByteArraySegment that represents the contents
         * of the desired Page.
         */
        CompletableFuture<ByteArraySegment> apply(long offset, int length, Duration timeout);
    }

    /**
     * Defines a method that, when invoked, writes the contents of contiguous BTreePages to the external data source.
     */
    @FunctionalInterface
    public interface WritePages {
        /**
         * Persists the contents of multiple, contiguous Pages to an external data source.
         *
         * @param pageContents    An ordered List of Offset-ByteArraySegments pairs representing the contents of the
         *                        individual pages mapped to their assigned Offsets. The list is ordered by Offset (Keys).
         *                        All entries should be contiguous (an entry's offset is equal to the previous entry's
         *                        offset + the previous entry's length).
         * @param obsoleteOffsets A Collection of offsets for Pages that are no longer relevant. It should be safe to evict
         *                        these from any caching structure involved, however the data should not be removed from
         *                        the data source.
         * @param timeout         Timeout for the operation.
         * @return A CompletableFuture that, when completed, will contain the current length (in bytes) of the index in the
         * external data source.
         */
        CompletableFuture<Long> apply(List<Map.Entry<Long, ByteArraySegment>> pageContents, Collection<Long> obsoleteOffsets, Duration timeout);
    }

    //endregion
}