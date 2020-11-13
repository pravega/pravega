/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.util.btree.sets;

import com.google.common.annotations.Beta;
import com.google.common.base.Preconditions;
import io.pravega.common.TimeoutTimer;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.ArrayView;
import io.pravega.common.util.AsyncIterator;
import io.pravega.common.util.BufferViewComparator;
import java.time.Duration;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

/**
 * A B+Tree-backed Set. Stores all items in a B+Tree Structure using a {@link BufferViewComparator} for ordering them.
 *
 * NOTE: This component is in {@link Beta}. There are no guarantees about data or API compatibility with future versions.
 * Any component that is directly dependent on this one should either be in {@link Beta} as well.
 */
@NotThreadSafe
@Beta
@Slf4j
public class BTreeSet {
    //region Members

    public static final Comparator<ArrayView> COMPARATOR = BufferViewComparator.create()::compare;
    private static final Comparator<PagePointer> POINTER_COMPARATOR = PagePointer.getComparator(COMPARATOR);

    private final int maxPageSize;
    private final int maxItemSize;
    @NonNull
    private final ReadPage read;
    @NonNull
    private final PersistPages update;
    @NonNull
    private final Executor executor;
    @NonNull
    private final String traceLogId;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the {@link BTreeSet} class.
     *
     * @param maxPageSize The maximum size, in bytes, of any page.
     * @param maxItemSize The maximum size, in bytes, of any single item in the {@link BTreeSet}.
     * @param read        A {@link ReadPage} function that can be used to fetch a single {@link BTreeSet} page from an
     *                    external data source.
     * @param update      A {@link PersistPages} function that can be used to store and delete multiple {@link BTreeSet}
     *                    pages to/from an external data source.
     * @param executor    Executor for async operations.
     * @param traceLogId  Trace id for logging.
     */
    public BTreeSet(int maxPageSize, int maxItemSize, @NonNull ReadPage read, @NonNull PersistPages update,
                    @NonNull Executor executor, String traceLogId) {
        Preconditions.checkArgument(maxItemSize < maxPageSize / 2, "maxItemSize must be at most half of maxPageSize.");
        this.maxItemSize = maxItemSize;
        this.maxPageSize = maxPageSize;
        this.read = read;
        this.update = update;
        this.executor = executor;
        this.traceLogId = traceLogId == null ? "" : traceLogId;
    }

    //endregion

    //region Updates

    /**
     * Atomically inserts the items in 'toInsert' into the {@link BTreeSet} and removes the items in 'toRemove'
     * from the {@link BTreeSet}. No duplicates are allowed; the same item cannot exist multiple times in either 'toInsert'
     * or 'toRemove' or in both of them.
     *
     * @param toInsert      (Optional). A Collection of {@link ArrayView} instances representing the items to insert.
     *                      If an item is already present, it will not be reinserted (updates are idempotent).
     * @param toRemove      (Optional). A Collection of {@link ArrayView} instances representing the items to remove.
     * @param getNextPageId A Supplier that, when invoked, will return a unique number representing the Id of the next
     *                      {@link BTreeSet} page that has to be generated.
     * @param timeout       Timeout for the operation.
     * @return A CompletableFuture that, when completed normally, will indicate that the updates have been applied
     * successfully. If the operation failed, the Future will be completed with the appropriate exception.
     * @throws IllegalArgumentException If an item appears more than once across the union of toInsert and toDelete.
     */
    public CompletableFuture<Void> update(@Nullable Collection<? extends ArrayView> toInsert, @Nullable Collection<? extends ArrayView> toRemove,
                                          @NonNull Supplier<Long> getNextPageId, @NonNull Duration timeout) {
        TimeoutTimer timer = new TimeoutTimer(timeout);
        val updates = new ArrayList<UpdateItem>();
        int insertCount = collectUpdates(toInsert, false, updates);
        int removeCount = collectUpdates(toRemove, true, updates);
        updates.sort(UpdateItem::compareTo);
        log.debug("{}: Update (Insert={}, Remove={}).", this.traceLogId, insertCount, removeCount);
        if (updates.isEmpty()) {
            // Nothing to do.
            return CompletableFuture.completedFuture(null);
        }

        // The updates are sorted, so any empty items will be placed first.
        Preconditions.checkArgument(updates.get(0).getItem().getLength() > 0, "No empty items allowed.");
        return applyUpdates(updates.iterator(), timer)
                .thenApply(pageCollection -> processModifiedPages(pageCollection, getNextPageId))
                .thenComposeAsync(pageCollection -> writePages(pageCollection, timer), this.executor);
    }

    private int collectUpdates(Collection<? extends ArrayView> items, boolean isRemoval, List<UpdateItem> updates) {
        if (items == null) {
            return 0;
        }

        for (val i : items) {
            Preconditions.checkArgument(i.getLength() <= this.maxItemSize,
                    "Item exceeds maximum allowed length (%s).", this.maxItemSize);
            updates.add(new UpdateItem(i, isRemoval));
        }
        return items.size();
    }

    private CompletableFuture<PageCollection> applyUpdates(Iterator<UpdateItem> items, TimeoutTimer timer) {
        val pageCollection = new PageCollection();
        val lastPage = new AtomicReference<BTreeSetPage.LeafPage>(null);
        val lastPageUpdates = new ArrayList<UpdateItem>();
        return Futures.loop(
                items::hasNext,
                () -> {
                    // Locate the page where the update is to be executed. Do not apply it yet as it is more efficient
                    // to bulk-apply multiple at once. Collect all updates for each Page, and only apply them once we have
                    // "moved on" to another page.
                    val next = items.next();
                    return locatePage(next.getItem(), pageCollection, timer)
                            .thenAccept(page -> {
                                val last = lastPage.get();
                                if (page != last) {
                                    // This key goes to a different page than the one we were looking at.
                                    if (last != null) {
                                        // Commit the outstanding updates.
                                        last.update(lastPageUpdates);
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
                    if (lastPage.get() != null) {
                        lastPage.get().update(lastPageUpdates);
                    }
                    return pageCollection;
                }, this.executor);
    }

    private PageCollection processModifiedPages(PageCollection pageCollection, Supplier<Long> getNewPageId) {
        Collection<BTreeSetPage> candidates = pageCollection.getLeafPages();
        while (!candidates.isEmpty()) {
            // Process each candidate and determine if it should be deleted or split into multiple pages.
            val tmc = new TreeModificationContext(pageCollection);
            for (BTreeSetPage p : candidates) {
                if (p.getItemCount() == 0) {
                    deletePage(p, tmc);
                } else {
                    splitPageIfNecessary(p, getNewPageId, tmc);
                }
            }

            // Update those pages' parents.
            tmc.accept(BTreeSetPage.IndexPage::addChildren, BTreeSetPage.IndexPage::removeChildren, POINTER_COMPARATOR);
            candidates = tmc.getModifiedParents();
        }

        pageCollection.getIndexPages().forEach(p -> {
            if (p.isModified()) {
                p.seal();
            }
        });
        return pageCollection;
    }

    private void deletePage(BTreeSetPage p, TreeModificationContext context) {
        // Delete the page if it's empty, but only if it's not the root page.
        if (p.getPagePointer().hasParent()) {
            context.getPageCollection().pageDeleted(p);
            context.deleted(p.getPagePointer());
            log.debug("{}: Deleted empty page {}.", this.traceLogId, p.getPagePointer());
        } else if (p.isIndexPage()) {
            p = BTreeSetPage.emptyLeafRoot();
            p.markModified();
            context.getPageCollection().pageUpdated(p);
            log.debug("{}: Replaced empty Index Root with empty Leaf Root.", this.traceLogId);
        }
    }

    private void splitPageIfNecessary(BTreeSetPage p, Supplier<Long> getNewPageId, TreeModificationContext context) {
        val splits = p.split(this.maxPageSize, getNewPageId);
        if (splits == null) {
            // No split necessary
            return;
        }

        if (p.getPagePointer().hasParent()) {
            Preconditions.checkArgument(splits.get(0).getPagePointer().getPageId() == p.getPagePointer().getPageId(),
                    "First split result (%s) not current page (%s).", splits.get(0).getPagePointer(), p.getPagePointer());
        } else {
            // If we split the root, the new pages will already point to the root; we must create a blank
            // index root page, which will be updated in the next step.
            context.getPageCollection().pageUpdated(BTreeSetPage.emptyIndexRoot());
        }

        splits.forEach(splitPage -> {
            context.getPageCollection().pageUpdated(splitPage);
            context.created(splitPage.getPagePointer());
        });
        log.debug("{}: Page '{}' split into {}: {}.", this.traceLogId, p, splits.size(), splits);
    }

    private CompletableFuture<Void> writePages(@NonNull PageCollection pageCollection, TimeoutTimer timer) {
        // Order the pages from bottom up. The upstream code may have limitations in how much it can update atomically,
        // so it may commit this in multiple non-atomic operations. If the process is interrupted mid-way then we want
        // to ensure that parent pages aren't updated before leaf pages (which would cause index corruptions - i.e., by
        // pointing to inexistent pages).
        val processedPageIds = new HashSet<Long>();

        // First collect updates. Begin from the bottom (Leaf Pages).
        val toWrite = new ArrayList<Map.Entry<Long, ArrayView>>();
        collectWriteCandidates(pageCollection.getLeafPages(), toWrite, processedPageIds, pageCollection);

        // Newly split pages may not be reachable from any modified Leaf Pages. Collect them too.
        collectWriteCandidates(pageCollection.getIndexPages(), toWrite, processedPageIds, pageCollection);

        // Then collect deletions, making sure we also consider all their parents (which should be modified/deleted as well).
        collectWriteCandidates(pageCollection.getDeletedPagesParents(), toWrite, processedPageIds, pageCollection);
        log.debug("{}: Persist (Updates={}, Deletions={}).", this.traceLogId, toWrite.size(), pageCollection.getDeletedPageIds().size());
        return this.update.apply(toWrite, pageCollection.getDeletedPageIds(), timer.getRemaining());
    }

    private void collectWriteCandidates(Collection<BTreeSetPage> candidates, List<Map.Entry<Long, ArrayView>> toWrite,
                                        Set<Long> processedIds, PageCollection pageCollection) {
        while (!candidates.isEmpty()) {
            val next = new ArrayList<BTreeSetPage>();
            candidates.stream()
                    .filter(p -> p.isModified() && !processedIds.contains(p.getPagePointer().getPageId()))
                    .forEach(p -> {
                        toWrite.add(new AbstractMap.SimpleImmutableEntry<>(p.getPagePointer().getPageId(), p.getData()));
                        val parent = pageCollection.get(p.getPagePointer().getParentPageId());
                        assert p.getPagePointer().hasParent() == (parent != null);
                        processedIds.add(p.getPagePointer().getPageId());
                        if (parent != null) {
                            next.add(parent);
                        }
                    });
            candidates = next;
        }
    }

    //endregion

    //region Queries

    /**
     * Returns an {@link AsyncIterator} that will iterate through all the items in this {@link BTreeSet} within the
     * specified bounds. All iterated items will be returned in lexicographic order (smallest to largest).
     * See {@link BufferViewComparator} for ordering details.
     *
     * @param firstItem          An {@link ArrayView} indicating the first Item to iterate from. If null, the iteration
     *                           will begin with the first item in the index.
     * @param firstItemInclusive If true, firstItem will be included in the iteration (provided it exists), otherwise it
     *                           will be excluded. This argument is ignored if firstItem is null.
     * @param lastItem           An {@link ArrayView} indicating the last Item to iterate to. If null, the iteration will
     *                           end with the last item in the index.
     * @param lastItemInclusive  If true, lastItem will be included in the iteration (provided it exists), otherwise it
     *                           will be excluded. This argument is ignored if lastItem is null.
     * @param fetchTimeout       Timeout for each invocation of {@link AsyncIterator#getNext}.
     * @return A new {@link AsyncIterator} instance.
     */
    public AsyncIterator<List<ArrayView>> iterator(@Nullable ArrayView firstItem, boolean firstItemInclusive,
                                                   @Nullable ArrayView lastItem, boolean lastItemInclusive, @NonNull Duration fetchTimeout) {
        return new ItemIterator(firstItem, firstItemInclusive, lastItem, lastItemInclusive, this::locatePage, fetchTimeout);
    }

    /**
     * Locates the {@link BTreeSetPage.LeafPage} that contains or should contain the given Item.
     *
     * @param item           An {@link ArrayView} that represents the Item to look up the Leaf Page for.
     * @param pageCollection A {@link PageCollection} that contains already looked-up pages. Any newly looked up pages
     *                       will be inserted in this instance as well.
     * @param timer          Timer for the operation.
     * @return A CompletableFuture with a {@link BTreeSetPage.LeafPage} for the sought page.
     */
    private CompletableFuture<BTreeSetPage.LeafPage> locatePage(ArrayView item, PageCollection pageCollection, TimeoutTimer timer) {
        val pagePointer = new AtomicReference<>(PagePointer.root());
        val result = new CompletableFuture<BTreeSetPage.LeafPage>();
        val loop = Futures.loop(
                () -> !result.isDone(),
                () -> fetchPage(pagePointer.get(), pageCollection, timer.getRemaining())
                        .thenAccept(page -> {
                            if (page.isIndexPage()) {
                                pagePointer.set(((BTreeSetPage.IndexPage) page).getChildPage(item, 0));
                            } else {
                                // We are done.
                                result.complete((BTreeSetPage.LeafPage) page);
                            }
                        }),
                this.executor);
        Futures.exceptionListener(loop, result::completeExceptionally);
        return result;
    }

    /**
     * Loads up a single Page.
     *
     * @param pagePointer    A {@link PagePointer} indicating the Page to load.
     * @param pageCollection A {@link PageCollection} that contains already looked up pages. If the sought page is already
     *                       loaded it will be served from here; otherwise it will be added here afterwards.
     * @param timeout        Timeout for the operation.
     * @return A CompletableFuture containing a {@link BTreeSetPage} for the sought page.
     */
    private CompletableFuture<BTreeSetPage> fetchPage(PagePointer pagePointer,
                                                      PageCollection pageCollection, Duration timeout) {
        BTreeSetPage fromCache = pageCollection.get(pagePointer.getPageId());
        if (fromCache != null) {
            return CompletableFuture.completedFuture(fromCache);
        }

        return this.read.apply(pagePointer.getPageId(), timeout)
                .thenApply(data -> {
                    BTreeSetPage page;
                    if (data == null) {
                        // The only acceptable case when there's no data for a page is an empty BTreeSet (and when we ask
                        // for the root.
                        Preconditions.checkArgument(!pagePointer.hasParent(), "Missing page contents for %s.", pagePointer);
                        page = BTreeSetPage.emptyLeafRoot();
                        log.debug("{}: Initialized empty root.", this.traceLogId);
                    } else {
                        page = BTreeSetPage.parse(pagePointer, data);
                        log.debug("{}: Loaded page {}.", this.traceLogId, page);
                    }

                    pageCollection.pageLoaded(page);
                    return page;
                });
    }

    //endregion

    //region Helper classes

    /**
     * Defines a method that, when invoked, reads the contents of a single {@link BTreeSetPage} from the external data source.
     */
    @FunctionalInterface
    public interface ReadPage {
        /**
         * Reads a single Page from an external data source.
         *
         * @param pageId  The Page To read
         * @param timeout Timeout for the operation.
         * @return A CompletableFuture that, when completed, will contain a {@link ArrayView} that represents the contents
         * of the desired Page.
         */
        CompletableFuture<ArrayView> apply(long pageId, Duration timeout);
    }

    /**
     * Defines a method that, when invoked, writes the contents of a set of {@link BTreeSetPage} instances to the external
     * data source and removes others.
     */
    @FunctionalInterface
    public interface PersistPages {
        /**
         * Persists the contents of multiple, contiguous Pages to an external data source.
         *
         * @param toUpdate A List of Page Id to Page contents to write. The order should be such that if the list
         *                 has to be broken down in non-atomic commits and commits are executed in order, then if
         *                 only some commits are persisted, the state of the {@link BTreeSet} will be preserved upon
         *                 reload.
         * @param toDelete A Collection of Page Ids to remove.
         * @param timeout  Timeout for the operation.
         * @return A CompletableFuture that, when completed, will indicate that the operation completed.
         */
        CompletableFuture<Void> apply(List<Map.Entry<Long, ArrayView>> toUpdate, Collection<Long> toDelete, Duration timeout);
    }

    //endregion
}
