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
package io.pravega.common.util.btree;

import com.google.common.base.Preconditions;
import io.pravega.common.TimeoutTimer;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.AsyncIterator;
import io.pravega.common.util.BufferViewComparator;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.common.util.IllegalDataFormatException;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import javax.annotation.concurrent.NotThreadSafe;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
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
 *
 * Compaction:
 * * B+Trees on an append-only storage suffer from write amplification problems, which means every update will have to
 * rewrite the affected leaf page(s) and all their parent page(s), up to, and including the root. These updates cause the
 * index file to grow fast.
 * * This B+Tree implementation keeps track of the page with smallest offset within the index, and every time it performs
 * an update (insert, update, delete) which causes pages to be written to the data source, it moves the page with the
 * smallest offset to the tail of the index. This allows the external data source to truncate unused data out of the
 * index file (every update also recalculates the smallest such offset, which is communicated to the data source).
 *
 * Versioning:
 * * BTreePages have built-in versioning; please refer to the BTreePage class for details. It is possible to mix different
 * BTreePage versions in the same BTreeIndex structure.
 * * BTreeIndex has no built-in versioning, as we would not be able to mix different versions of the BTreeIndex in the same
 * data source - that is, we cannot begin writing at version X, then after a while we switch to version Y in the same file.
 * * For BTreeIndex versioning (when it will be needed), a suggested approach is to pass in the version via the constructor
 * which should tell the BTreeIndex how to interpret the data in the external data source. Once a BTreeIndex is written in
 * one version in a file, it can only be "upgraded" if it is bulk-loaded into a different file (such a feature is not yet
 * supported). This versioning would have to be maintained externally (i.e., in a Segment Core Attribute or by file naming
 * conventions).
 */
@NotThreadSafe
@Slf4j
public class BTreeIndex {
    //region Members

    private static final int INDEX_VALUE_LENGTH = Long.BYTES + Short.BYTES + Long.BYTES; // Offset, PageLength, MinOffset.
    private static final int FOOTER_LENGTH = Long.BYTES + Integer.BYTES;
    private static final BufferViewComparator KEY_COMPARATOR = BufferViewComparator.create();
    private final BTreePage.Config indexPageConfig;
    private final BTreePage.Config leafPageConfig;
    private final ReadPage read;
    private final WritePages write;
    private final GetLength getLength;
    private volatile IndexState state;
    private volatile boolean maintainStatistics;
    @Getter
    private volatile Statistics statistics;
    private final Executor executor;
    private final String traceObjectId;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the BTreeIndex class.
     *
     * @param maxPageSize        Maximum page size. No BTreeIndex Page will exceed this value.
     * @param keyLength          The length, in bytes, of the index Keys.
     * @param valueLength        The length, in bytes, of the index Values.
     * @param readPage           A Function that reads the contents of a page from an external data source.
     * @param writePages         A Function that writes contents of one or more contiguous pages to an external data source.
     * @param getLength          A Function that returns the length of the index, in bytes, as stored in an external data source.
     * @param maintainStatistics If true, the BTreeIndex will maintain {@link Statistics} about its contents.
     * @param executor           Executor for async operations.
     * @param traceObjectId      An identifier to add to all log entries.
     */
    @Builder
    public BTreeIndex(int maxPageSize, int keyLength, int valueLength, @NonNull ReadPage readPage, @NonNull WritePages writePages,
                      @NonNull GetLength getLength, boolean maintainStatistics, @NonNull Executor executor, String traceObjectId) {
        this.read = readPage;
        this.write = writePages;
        this.getLength = getLength;
        this.maintainStatistics = maintainStatistics;
        this.executor = executor;
        this.traceObjectId = traceObjectId;

        // BTreePage.Config validates the arguments so we don't need to.
        this.indexPageConfig = new BTreePage.Config(keyLength, INDEX_VALUE_LENGTH, maxPageSize, true);
        this.leafPageConfig = new BTreePage.Config(keyLength, valueLength, maxPageSize, false);
        this.state = null;
    }

    //endregion

    //region Operations

    /**
     * Gets a value indicating whether this BTreeIndex instance is initialized or not.
     *
     * @return True if initialized, false otherwise.
     */
    public boolean isInitialized() {
        return this.state != null;
    }

    /**
     * Gets a value indicating the presumed Index Length in the external Data Source.
     *
     * @return The Index Length.
     */
    public long getIndexLength() {
        IndexState s = this.state;
        return s == null ? -1 : s.length;
    }

    /**
     * Initializes the BTreeIndex by fetching metadata from the external data source. This method must be invoked (and
     * completed) prior to executing any other operation on this instance.
     *
     * @param timeout Timeout for the operation.
     * @return A CompletableFuture that, when completed, will indicate that the operation completed.
     */
    public CompletableFuture<Void> initialize(Duration timeout) {
        if (isInitialized()) {
            log.warn("{}: Reinitializing.", this.traceObjectId);
        }

        TimeoutTimer timer = new TimeoutTimer(timeout);
        return this.getLength
                .apply(timer.getRemaining())
                .thenCompose(indexInfo -> {
                    if (indexInfo.getIndexLength() <= FOOTER_LENGTH) {
                        // Empty index.
                        setState(indexInfo.getIndexLength(), PagePointer.NO_OFFSET, 0);
                        this.statistics = this.maintainStatistics ? Statistics.EMPTY : null;
                        return CompletableFuture.completedFuture(null);
                    }

                    long footerOffset = indexInfo.getRootPointer() >= 0 ? indexInfo.getRootPointer() : getFooterOffset(indexInfo.getIndexLength());
                    return this.read
                            .apply(footerOffset, FOOTER_LENGTH, false, timer.getRemaining())
                            .thenAcceptAsync(footer -> initialize(footer, footerOffset, indexInfo.getIndexLength()), this.executor)
                            .thenCompose(v -> loadStatistics(timer.getRemaining()))
                            .thenRun(() -> log.info("{}: Initialized. State = {}, Stats = {}.", this.traceObjectId, this.state, this.statistics));
                });
    }

    /**
     * Initializes the BTreeIndex using information from the given footer.
     *
     * @param footer       A ByteArraySegment representing the footer that was written with the last update.
     * @param footerOffset The offset within the data source where the footer is located at.
     * @param indexLength  The length of the index, in bytes, in the data source.
     */
    private void initialize(ByteArraySegment footer, long footerOffset, long indexLength) {
        if (footer.getLength() != FOOTER_LENGTH) {
            throw new IllegalDataFormatException(String.format("[%s] Wrong footer length. Expected %s, actual %s.",
                    this.traceObjectId, FOOTER_LENGTH, footer.getLength()));
        }

        long rootPageOffset = getRootPageOffset(footer);
        int rootPageLength = getRootPageLength(footer);
        if (rootPageOffset + rootPageLength > footerOffset) {
            throw new IllegalDataFormatException(String.format("[%s] Wrong footer information. RootPage Offset (%s) + Length (%s) exceeds Footer Offset (%s).",
                    this.traceObjectId, rootPageOffset, rootPageLength, footerOffset));
        }

        setState(indexLength, rootPageOffset, rootPageLength);
    }

    private CompletableFuture<Void> loadStatistics(Duration timeout) {
        if (!this.maintainStatistics) {
            // Disabled.
            return CompletableFuture.completedFuture(null);
        }

        val s = this.state;
        if (s.rootPageOffset == PagePointer.NO_OFFSET) {
            this.statistics = Statistics.EMPTY;
            log.debug("{}: Resetting stats due to index empty.", this.traceObjectId);
            return CompletableFuture.completedFuture(null);
        }

        long statsOffset = s.rootPageOffset + s.rootPageLength;
        int statsLength = (int) Math.min(s.length - FOOTER_LENGTH - statsOffset, Integer.MAX_VALUE);
        if (statsLength <= 0) {
            // The Maintain Stats option was set, however this particular index does not support stats (because it was
            // originally build with stats disabled or before stats were added).
            this.maintainStatistics = false;
            this.statistics = null;
            log.debug("{}: Not loading stats due to legacy index not supporting stats.", this.traceObjectId);
            return CompletableFuture.completedFuture(null);
        }

        return this.read.apply(statsOffset, statsLength, false, timeout)
                .thenAccept(data -> {
                    try {
                        this.statistics = Statistics.SERIALIZER.deserialize(data);
                    } catch (IOException ex) {
                        throw new CompletionException(ex);
                    }
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
        PageCollection pageCollection = new PageCollection(this.state.length);
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
        PageCollection pageCollection = new PageCollection(this.state.length);
        val gets = keys.stream()
                .map(key -> locatePage(key, pageCollection, timer)
                        .thenApplyAsync(page -> page.getPage().searchExact(key), this.executor))
                .collect(Collectors.toList());
        return Futures.allOfWithResults(gets);
    }

    /**
     * Inserts, updates or removes the given Page Entries into the index. If {@link PageEntry#getValue()} is null, then
     * the page entry is removed, otherwise it is added.
     *
     * @param entries A Collection of Page Entries to insert. The collection need not be sorted.
     * @param timeout Timeout for the operation.
     * @return A CompletableFuture that, when completed normally, will indicate that the index updates have been applied
     * successfully and will contain the current version of the index (any modifications to the index will result in a
     * larger version value). If the operation failed, the Future will be completed with the appropriate exception.
     */
    public CompletableFuture<Long> update(@NonNull Collection<PageEntry> entries, @NonNull Duration timeout) {
        ensureInitialized();
        TimeoutTimer timer = new TimeoutTimer(timeout);

        // Process the Entries in sorted order (by key); this makes the operation more efficient as we can batch-update
        // entries belonging to the same page.
        val toUpdate = entries.stream()
                .sorted((e1, e2) -> KEY_COMPARATOR.compare(e1.getKey(), e2.getKey()))
                .iterator();
        return applyUpdates(toUpdate, timer)
                .thenComposeAsync(pageCollection -> loadSmallestOffsetPage(pageCollection, timer)
                                .thenRun(() -> processModifiedPages(pageCollection))
                                .thenComposeAsync(v -> writePages(pageCollection, timer.getRemaining()), this.executor),
                        this.executor);
    }

    /**
     * Returns an {@link AsyncIterator} that will iterate through all the keys within the specified bounds. All iterated keys will
     * be returned in lexicographic order (smallest to largest). See {@link BufferViewComparator} for ordering details.
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
        return new EntryIterator(firstKey, firstKeyInclusive, lastKey, lastKeyInclusive, this::locatePage, this.state.length, fetchTimeout);
    }

    //endregion

    //region Helpers

    /**
     * Executes the given updates on the index. Loads up any necessary BTreePage instances in memory but does not persist
     * the changes to the external data source, nor does it reassign offsets to the modified pages, perform splits, etc.
     *
     * @param updates An Iterator of the PageEntry instances to insert, update or remove. The Iterator must return the
     *                updates in sorted order (by key).
     * @param timer   Timer for the operation.
     * @return A CompletableFuture that will contain a PageCollection with all touched pages.
     */
    private CompletableFuture<UpdateablePageCollection> applyUpdates(Iterator<PageEntry> updates, TimeoutTimer timer) {
        UpdateablePageCollection pageCollection = new UpdateablePageCollection(this.state.length);
        AtomicReference<PageWrapper> lastPage = new AtomicReference<>(null);
        val lastPageUpdates = new ArrayList<PageEntry>();
        return Futures.loop(
                updates::hasNext,
                () -> {
                    // Locate the page where the update is to be executed. Do not apply it yet as it is more efficient
                    // to bulk-apply multiple at once. Collect all updates for each Page, and only apply them once we have
                    // "moved on" to another page.
                    PageEntry next = updates.next();
                    return locatePage(next.getKey(), pageCollection, timer)
                            .thenAccept(page -> {
                                PageWrapper last = lastPage.get();
                                if (page != last) {
                                    // This key goes to a different page than the one we were looking at.
                                    if (last != null) {
                                        // Commit the outstanding updates.
                                        last.setEntryCountDelta(last.getPage().update(lastPageUpdates));
                                    }

                                    // Update the pointers.
                                    lastPage.set(page);
                                    lastPageUpdates.clear();
                                }

                                // Record the current update.
                                lastPageUpdates.add(next);
                            });
                },
                this.executor)
                .thenApplyAsync(v -> {
                    // We need not forget to apply the last batch of updates from the last page.
                    PageWrapper last = lastPage.get();
                    if (last != null) {
                        last.setEntryCountDelta(last.getPage().update(lastPageUpdates));
                    }
                    return pageCollection;
                }, this.executor);
    }

    /**
     * Loads the BTreePage with the smallest offset from the DataSource. The purpose of this is for incremental compaction.
     * The page with the smallest offset will be moved to the end of the index, which allows the external data source to
     * perform any truncation necessary in order to free up space.
     *
     * @param pageCollection A PageCollection containing all pages loaded so far. The new page (and its ancestors) will
     *                       also be loaded here.
     * @param timer          Timer for the operation.
     * @return A CompletableFuture that will indicate when the operation is complete.
     */
    private CompletableFuture<?> loadSmallestOffsetPage(PageCollection pageCollection, TimeoutTimer timer) {
        if (pageCollection.getCount() <= 1) {
            // We only modified at most one page. This means have at most one page in the index, so no point in re-loading it.
            return CompletableFuture.completedFuture(null);
        }

        long minOffset = calculateMinOffset(pageCollection.getRootPage());
        return locatePage(
                page -> getPagePointer(minOffset, page),
                page -> !page.isIndexPage() || page.getOffset() == minOffset,
                pageCollection,
                timer);
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
     * @param pageCollection An UpdateablePageCollection containing pages to be processed.
     */
    private void processModifiedPages(UpdateablePageCollection pageCollection) {
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
            long minOffset;
            PageWrapper processedPage;
            if (i == 0) {
                // The original page will be replaced by the first split. Nothing changes about its pointer key.
                originalPage.setPage(page);
                newPageKey = originalPage.getPageKey();
                context.getPageCollection().complete(originalPage);
                processedPage = originalPage;
            } else {
                // Insert the new pages and assign them new virtual offsets. Each page will use its first
                // Key as a Page Key.
                newPageKey = page.getKeyAt(0);
                processedPage = PageWrapper.wrapNew(page, originalPage.getParent(), new PagePointer(newPageKey, PagePointer.NO_OFFSET, page.getLength()));
                context.getPageCollection().insert(processedPage);
                context.getPageCollection().complete(processedPage);
            }

            // Fetch new offset, and update minimum offsets.
            newOffset = processedPage.getOffset();
            minOffset = calculateMinOffset(processedPage);
            processedPage.setMinOffset(minOffset);

            // Record changes.
            context.updatePagePointer(new PagePointer(newPageKey, newOffset, page.getLength(), minOffset));
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
            page.setMinOffset(calculateMinOffset(page));
            context.updatePagePointer(new PagePointer(pageKey, page.getOffset(), page.getPage().getLength(), page.getMinOffset()));
        }
    }

    /**
     * Calculates the Minimum Page Offset for this PageWrapper.
     * The Minimum Page Offset is the smallest of this PageWrapper's offset and the MinOffsets of all this PageWrapper's
     * direct children.
     *
     * @param pageWrapper The PageWrapper to calculate the Minimum Page Offset for.
     * @return The result.
     */
    private long calculateMinOffset(PageWrapper pageWrapper) {
        // Minimum within {PageOffset, All PagePointers' MinLength fields}.
        long min = pageWrapper.getOffset();
        if (!pageWrapper.isIndexPage()) {
            // Leaf pages do not have PagePointers, so the result is their actual offsets.
            return min;
        }

        BTreePage page = pageWrapper.getPage();
        int count = page.getCount();
        for (int pos = 0; pos < count; pos++) {
            long ppMinOffset = deserializePointerMinOffset(page.getValueAt(pos));
            min = Math.min(min, ppMinOffset);
        }

        return min;
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
            parentPage.getPage().update(Collections.singletonList(PageEntry.noValue(context.getDeletedPageKey())));
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
        // Verify the sought key has the expected length.
        Preconditions.checkArgument(key.getLength() == this.leafPageConfig.getKeyLength(), "Invalid key length.");

        // Sanity check that our pageCollection has a somewhat correct view of the data index state. It is OK for it to
        // think the index length is less than what it actually is (since a concurrent update may have increased it), but
        // it is not a good sign if it thinks it's longer than the actual length.
        Preconditions.checkArgument(pageCollection.getIndexLength() <= this.state.length, "Unexpected PageCollection.IndexLength.");

        if (this.state.rootPageOffset == PagePointer.NO_OFFSET && pageCollection.getCount() == 0) {
            // No data. Return an empty (leaf) page, which will serve as the root for now.
            return CompletableFuture.completedFuture(pageCollection.insert(PageWrapper.wrapNew(createEmptyLeafPage(), null, null)));
        }

        // Locate the page by searching on the Key (within the page).
        return locatePage(page -> getPagePointer(key, page), page -> !page.isIndexPage(), pageCollection, timer);
    }

    /**
     * Locates the BTreePage according to the given criteria.
     *
     * @param getChildPointer A Function that, when applied to a BTreePage, will return a PagePointer which can be used
     *                        to load up the next page.
     * @param found           A Predicate that, when applied to a PageWrapper, will indicate if this is the sought page.
     * @param pageCollection  A PageCollection to query for already loaded pages, as well as to store newly loaded ones.
     * @param timer           Timer for the operation.
     * @return A CompletableFuture with a PageWrapper for the sought page.
     */
    private CompletableFuture<PageWrapper> locatePage(Function<BTreePage, PagePointer> getChildPointer, Predicate<PageWrapper> found,
                                                      PageCollection pageCollection, TimeoutTimer timer) {
        AtomicReference<PagePointer> pagePointer = new AtomicReference<>(new PagePointer(null, this.state.rootPageOffset, this.state.rootPageLength));
        CompletableFuture<PageWrapper> result = new CompletableFuture<>();
        AtomicReference<PageWrapper> parentPage = new AtomicReference<>(null);
        Futures.loop(
                () -> !result.isDone(),
                () -> fetchPage(pagePointer.get(), parentPage.get(), pageCollection, timer.getRemaining())
                        .thenAccept(page -> {
                            if (found.test(page)) {
                                // We are done.
                                result.complete(page);
                            } else {
                                PagePointer childPointer = getChildPointer.apply(page.getPage());
                                pagePointer.set(childPointer);
                                parentPage.set(page);
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

    private PagePointer getPagePointer(long minOffset, BTreePage page) {
        int count = page.getCount();
        for (int pos = 0; pos < count; pos++) {
            long ppMinOffset = deserializePointerMinOffset(page.getValueAt(pos));
            if (ppMinOffset == minOffset) {
                return deserializePointer(page.getValueAt(pos), page.getKeyAt(pos));
            }
        }

        // Nothing was found.
        return null;
    }

    private ByteArraySegment serializePointer(PagePointer pointer) {
        assert pointer.getLength() <= Short.MAX_VALUE : "PagePointer.length too large";
        ByteArraySegment result = new ByteArraySegment(new byte[this.indexPageConfig.getValueLength()]);
        result.setLong(0, pointer.getOffset());
        result.setShort(Long.BYTES, (short) pointer.getLength());
        result.setLong(Long.BYTES + Short.BYTES, pointer.getMinOffset());
        return result;
    }

    private PagePointer deserializePointer(ByteArraySegment serialization, ByteArraySegment pageKey) {
        long pageOffset = serialization.getLong(0);
        int pageLength = serialization.getShort(Long.BYTES);
        long minOffset = deserializePointerMinOffset(serialization);
        return new PagePointer(pageKey, pageOffset, pageLength, minOffset);
    }

    private long deserializePointerMinOffset(ByteArraySegment serialization) {
        return serialization.getLong(Long.BYTES + Short.BYTES);
    }

    @SuppressWarnings("all")
    private ByteArraySegment generateMinKey() {
        byte[] result = new byte[this.indexPageConfig.getKeyLength()];
        if (BufferViewComparator.MIN_VALUE != 0) {
            Arrays.fill(result, BufferViewComparator.MIN_VALUE);
        }

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
        return this.read.apply(offset, length, true, timeout);
    }

    /**
     * Writes the contents of all the BTreePages in the given PageCollection to the external data source.
     *
     * @param pageCollection UpdateablePageCollection with pages.
     * @param timeout        Timeout for the operation.
     * @return A CompletableFuture with a Long representing the current length of the index in the external data source.
     */
    @SneakyThrows(IOException.class)
    private CompletableFuture<Long> writePages(UpdateablePageCollection pageCollection, Duration timeout) {
        IndexState state = this.state;
        Preconditions.checkState(state != null, "Cannot write without fetching the state first.");

        // Collect the data to be written.
        val pages = new ArrayList<WritePage>();
        val oldOffsets = new ArrayList<Long>();
        long offset = state.length;
        PageWrapper lastPage = null;
        for (PageWrapper p : pageCollection.getPagesSortedByOffset()) {
            if (offset >= 0) {
                Preconditions.checkArgument(p.getOffset() == offset, "Expecting Page offset %s, found %s.", offset, p.getOffset());
            }

            // Collect the page, as well as its previous offset.
            pages.add(new WritePage(offset, p.getPage().getContents(), true));
            if (p.getPointer() != null && p.getPointer().getOffset() >= 0) {
                oldOffsets.add(p.getPointer().getOffset());
                if (this.maintainStatistics && p.getParent() == null) {
                    // Stats are stored immediately after the root page; mark that as obsolete as well.
                    oldOffsets.add(p.getPointer().getOffset() + p.getPointer().getLength());
                }
            }

            offset = p.getOffset() + p.getPage().getLength();
            lastPage = p;
        }

        Preconditions.checkArgument(lastPage != null && lastPage.getParent() == null, "Last page to be written is not the root page");
        Preconditions.checkArgument(pageCollection.getIndexLength() == offset, "IndexLength mismatch.");

        // Update and store statistics - if required.
        Statistics newStats;
        if (this.maintainStatistics) {
            // Calculate and store stats here.
            newStats = this.statistics.update(pageCollection.getEntryCountDelta(), pageCollection.getPageCountDelta());
            val sp = new WritePage(offset, Statistics.SERIALIZER.serialize(newStats), false);
            pages.add(sp);
            offset += sp.getContents().getLength();
        } else {
            // Stats are not maintained.
            newStats = null;
        }

        // Write a footer with information about locating the root page.
        final long footerOffset = offset;
        pages.add(new WritePage(footerOffset, getFooter(lastPage.getOffset(), lastPage.getPage().getLength()), false));

        // Collect the old footer's offset, as it will be replaced by a more recent value.
        long oldFooterOffset = getFooterOffset(state.length);
        if (oldFooterOffset >= 0) {
            oldOffsets.add(oldFooterOffset);
        }

        // Collect the offsets of removed pages. These are no longer needed.
        pageCollection.collectRemovedPageOffsets(oldOffsets);

        // Write it.
        long rootOffset = lastPage.getOffset();
        int rootLength = lastPage.getPage().getContents().getLength();
        long rootMinOffset = lastPage.getMinOffset();
        assert rootMinOffset >= 0 : "root.MinOffset not set";
        return this.write.apply(pages, oldOffsets, rootMinOffset, timeout)
                .thenApply(indexLength -> {
                    this.statistics = newStats;
                    setState(indexLength, rootOffset, rootLength);
                    assert footerOffset == getFooterOffset(indexLength); // This should fail any unit tests.
                    return footerOffset;
                });
    }

    private void setState(long length, long rootPageOffset, int rootPageLength) {
        this.state = new IndexState(length, rootPageOffset, rootPageLength);
        log.debug("{}: IndexState: {}, Stats: {}.", this.traceObjectId, this.state, this.statistics);
    }

    private long getFooterOffset(long indexLength) {
        return indexLength - FOOTER_LENGTH;
    }

    private ByteArraySegment getFooter(long rootPageOffset, int rootPageLength) {
        ByteArraySegment result = new ByteArraySegment(new byte[FOOTER_LENGTH]);
        result.setLong(0, rootPageOffset);
        result.setInt(Long.BYTES, rootPageLength);
        return result;
    }

    private long getRootPageOffset(ByteArraySegment footer) {
        return footer.getLong(0);
    }

    private int getRootPageLength(ByteArraySegment footer) {
        return footer.getInt(Long.BYTES);
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
        private final UpdateablePageCollection pageCollection;
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
        CompletableFuture<IndexInfo> apply(Duration timeout);
    }

    /**
     * Information about the index.
     */
    @Data
    public static class IndexInfo {
        /**
         * An {@link IndexInfo} instance that indicates an empty index.
         */
        public static final IndexInfo EMPTY = new IndexInfo(0, PagePointer.NO_OFFSET);
        /**
         * The length of the Index, in bytes.
         */
        private final long indexLength;
        /**
         * The offset within the Index where the BTree root pointer (Footer) is located.
         */
        private final long rootPointer;

        @Override
        public String toString() {
            return String.format("IndexLength = %d, RootPointer = %d", this.indexLength, this.rootPointer);
        }
    }

    /**
     * Defines a method that, when invoked, reads the contents of a single BTreePage from the external data source.
     */
    @FunctionalInterface
    public interface ReadPage {
        /**
         * Reads a single Page from an external data source.
         *
         * @param offset      The offset of the desired Page.
         * @param length      The length of the desired Page.
         * @param cacheResult If true, the result of this operation should be cached if possible.
         * @param timeout     Timeout for the operation.
         * @return A CompletableFuture that, when completed, will contain a ByteArraySegment that represents the contents
         * of the desired Page.
         */
        CompletableFuture<ByteArraySegment> apply(long offset, int length, boolean cacheResult, Duration timeout);
    }

    /**
     * Defines a method that, when invoked, writes the contents of contiguous BTreePages to the external data source.
     */
    @FunctionalInterface
    public interface WritePages {
        /**
         * Persists the contents of multiple, contiguous Pages to an external data source.
         *
         * @param pageContents    An ordered List of {@link WritePage} representing the contents of the
         *                        individual pages mapped to their assigned Offsets. The list is ordered by Offset (Keys).
         *                        All entries should be contiguous (an entry's offset is equal to the previous entry's
         *                        offset + the previous entry's length).
         * @param obsoleteOffsets A Collection of offsets for Pages that are no longer relevant. It should be safe to evict
         *                        these from any caching structure involved, however the data should not be removed from
         *                        the data source.
         * @param truncateOffset  An offset within the external data source before which there is no useful data anymore.
         *                        Any data prior to this offset can be safely truncated out (as long as subsequent offsets
         *                        are preserved and do not "shift" downwards - i.e., we have consistent reads).
         * @param timeout         Timeout for the operation.
         * @return A CompletableFuture that, when completed, will contain the current length (in bytes) of the index in the
         * external data source.
         */
        CompletableFuture<Long> apply(List<WritePage> pageContents, Collection<Long> obsoleteOffsets, long truncateOffset, Duration timeout);
    }

    @Data
    public static class WritePage {
        private final long offset;
        private final ByteArraySegment contents;
        private final boolean cache;
    }

    //endregion
}