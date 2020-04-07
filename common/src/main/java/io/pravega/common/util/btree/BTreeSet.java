/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
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
import io.pravega.common.util.ArrayView;
import io.pravega.common.util.AsyncIterator;
import io.pravega.common.util.ByteArrayComparator;
import java.time.Duration;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;

/**
 * A B+Tree-backed Set.
 */
@NotThreadSafe
@RequiredArgsConstructor
public class BTreeSet {
    static final ByteArrayComparator COMPARATOR = new ByteArrayComparator();
    private final int maxPageSize;
    private final int maxItemSize;
    @NonNull
    private final ReadPage read;
    @NonNull
    private final BTreeSet.PersistPages update;
    @NonNull
    private final Executor executor;

    //region Updates

    /**
     * Atomically inserts the items in 'toInsert' into the {@link BTreeSet} and removes the items in 'toRemove'
     * from the {@link BTreeSet}.
     *
     * @param toInsert      (Optional). A Collection of {@link ArrayView} instances representing the items to insert.
     *                      If an item is already present, it will not be reinserted (updates are idempotent).
     * @param toRemove      (Optional). A Collection of {@link ArrayView} instances representing the items to remove.
     * @param getNextPageId A Supplier that, when invoked, will return a unique number representing the Id of the next
     *                      {@link BTreeSet} page that has to be generated.
     * @param timeout       Timeout for the operation.
     * @return A CompletableFuture that, when completed normally, will indicate that the updates have been applied
     * successfully. If the operation failed, the Future will be completed with the appropriate exception.
     */
    public CompletableFuture<Void> update(@Nullable Collection<ArrayView> toInsert, @Nullable Collection<ArrayView> toRemove,
                                          @NonNull Supplier<Long> getNextPageId, @NonNull Duration timeout) {
        TimeoutTimer timer = new TimeoutTimer(timeout);
        val updates = new ArrayList<BTreeSetPage.UpdateItem>();
        collectUpdates(toInsert, false, updates);
        collectUpdates(toRemove, true, updates);
        updates.sort(BTreeSetPage.UpdateItem::compareTo);
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

    private void collectUpdates(Collection<ArrayView> items, boolean isRemoval, List<BTreeSetPage.UpdateItem> updates) {
        if (items == null) {
            return;
        }

        for (val i : items) {
            Preconditions.checkArgument(i.getLength() <= this.maxItemSize,
                    "Item exceeds maximum allowed length (%s).", this.maxItemSize);
            updates.add(new BTreeSetPage.UpdateItem(i, isRemoval));
        }
    }

    private CompletableFuture<PageCollection> applyUpdates(Iterator<BTreeSetPage.UpdateItem> items, TimeoutTimer timer) {
        val pageCollection = new PageCollection();
        val lastPage = new AtomicReference<BTreeSetPage.LeafPage>(null);
        val lastPageUpdates = new ArrayList<BTreeSetPage.UpdateItem>();
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
            val splitContext = new SplitContext(pageCollection);
            for (BTreeSetPage p : candidates) {
                if (p.getItemCount() == 0) {
                    deletePage(p, splitContext);
                } else {
                    splitPageIfNecessary(p, getNewPageId, splitContext);
                }
            }

            // Update those pages' parents.
            splitContext.forEachDeleted(BTreeSetPage.IndexPage::removeChildren);
            splitContext.forEachInserted(BTreeSetPage.IndexPage::addChildren);
            candidates = splitContext.getModifiedParents();
        }

        return pageCollection;
    }

    private void deletePage(BTreeSetPage p, SplitContext splitContext) {
        // Delete the page if it's empty, but only if it's not the root page.
        if (p.getPagePointer().hasParent()) {
            splitContext.pageCollection.pageDeleted(p);
            splitContext.deleted(p.getPagePointer());
        }
    }

    private void splitPageIfNecessary(BTreeSetPage p, Supplier<Long> getNewPageId, SplitContext splitContext) {
        val splits = p.split(this.maxPageSize, getNewPageId);
        if(splits == null){
            // No split necessary
            return;
        }

        Preconditions.checkArgument(splits.get(0).getPagePointer() == p.getPagePointer(),
                "First split result (%s) not current page (%s).", splits.get(0).getPagePointer(), p.getPagePointer());
        if (!p.getPagePointer().hasParent()) {
            // If we split the root, the new pages will already point to the root; we must create a blank
            // index root page, which will be updated in the next step.
            splitContext.pageCollection.pageUpdated(BTreeSetPage.emptyIndexRoot());
        }

        splits.forEach(splitPage -> {
            splitContext.pageCollection.pageUpdated(splitPage);
            splitContext.created(splitPage.getPagePointer());
        });
    }

    private CompletableFuture<Void> writePages(@NonNull PageCollection pageCollection, TimeoutTimer timer) {
        // Order the pages from bottom up. The upstream code may have limitations in how much it can update atomically,
        // so it may commit this in multiple non-atomic operations. If the process is interrupted mid-way then we want
        // to ensure that parent pages aren't updated before leaf pages (which would cause index corruptions - i.e., by
        // pointing to inexistent pages).
        val processedPageIds = new HashSet<Long>();

        // First collect updates.
        val toWrite = new ArrayList<Map.Entry<Long, ArrayView>>();
        collectWriteCandidates(pageCollection.getLeafPages(), toWrite, processedPageIds, pageCollection);

        // Then collect deletions, making sure we also consider all their parents (which should be modified/deleted as well).
        val candidates = pageCollection.deletedPageIds.values().stream()
                .map(pageCollection::get)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
        collectWriteCandidates(candidates, toWrite, processedPageIds, pageCollection);
        return this.update.apply(toWrite, pageCollection.deletedPageIds.keySet(), timer.getRemaining());
    }

    private void collectWriteCandidates(List<BTreeSetPage> candidates, List<Map.Entry<Long, ArrayView>> toWrite,
                                        Set<Long> processedIds, PageCollection pageCollection) {
        while (!candidates.isEmpty()) {
            val next = new ArrayList<BTreeSetPage>();
            candidates.stream()
                    .filter(p -> p.isModified() && !processedIds.contains(p.getPagePointer().getPageId()))
                    .forEach(p -> {
                        toWrite.add(new AbstractMap.SimpleImmutableEntry<>(p.getPagePointer().getPageId(), p.getData()));
                        val parent = pageCollection.get(p.getPagePointer().getParentPageId());
                        assert parent != null;
                        next.add(parent);
                        processedIds.add(p.getPagePointer().getPageId());
                    });
            candidates = next;
        }
    }

    //endregion

    //region Queries

    /**
     * Returns an {@link AsyncIterator} that will iterate through all the items in this {@link BTreeSet} within the
     * specified bounds. All iterated items will be returned in lexicographic order (smallest to largest).
     * See {@link ByteArrayComparator} for ordering details.
     *
     * @param firstItem          An {@link ArrayView} representing the lower bound of the iteration.
     * @param firstItemInclusive If true, firstItem will be included in the iteration (if it exists in the {@link BTreeSet}),
     *                           otherwise it will not.
     * @param lastItem           An {@link ArrayView} representing the upper bound of the iteration.
     * @param lastItemInclusive  If true, lastKey will be included in the iteration (if it exists in the {@link BTreeSet})),
     *                           otherwise it will not.
     * @param fetchTimeout       Timeout for each invocation of {@link AsyncIterator#getNext}.
     * @return A new {@link AsyncIterator} instance.
     */
    public AsyncIterator<List<ArrayView>> iterator(@NonNull ArrayView firstItem, boolean firstItemInclusive,
                                                   @NonNull ArrayView lastItem, boolean lastItemInclusive, @NonNull Duration fetchTimeout) {
        return null;
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
        val pagePointer = new AtomicReference<>(BTreeSetPage.PagePointer.root());
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
     * @param pagePointer    A {@link BTreeSetPage.PagePointer} indicating the Page to load.
     * @param pageCollection A {@link PageCollection} that contains already looked up pages. If the sought page is already
     *                       loaded it will be served from here; otherwise it will be added here afterwards.
     * @param timeout        Timeout for the operation.
     * @return A CompletableFuture containing a {@link BTreeSetPage} for the sought page.
     */
    private CompletableFuture<BTreeSetPage> fetchPage(BTreeSetPage.PagePointer pagePointer,
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
                    } else {
                        page = BTreeSetPage.parse(pagePointer, data);
                    }

                    pageCollection.pageLoaded(page);
                    return page;
                });
    }


    //endregion

    //region Helper classes

    private static class PageCollection {
        private final HashMap<Long, BTreeSetPage> pages = new HashMap<>();
        private final HashMap<Long, Long> deletedPageIds = new HashMap<>();

        synchronized BTreeSetPage get(long pageId) {
            return pages.getOrDefault(pageId, null);
        }

        synchronized void pageLoaded(BTreeSetPage page) {
            this.pages.put(page.getPagePointer().getPageId(), page);
        }

        synchronized void pageUpdated(BTreeSetPage page) {
            assert !this.deletedPageIds.containsKey(page.getPagePointer().getPageId());
            this.pages.put(page.getPagePointer().getPageId(), page);
        }

        synchronized void pageDeleted(BTreeSetPage page) {
            this.deletedPageIds.put(page.getPagePointer().getPageId(), page.getPagePointer().getParentPageId());
            this.pages.remove(page.getPagePointer().getPageId());
        }

        synchronized List<BTreeSetPage> getLeafPages() {
            return this.pages.values().stream().filter(p -> !p.isIndexPage()).collect(Collectors.toList());
        }
    }

    @RequiredArgsConstructor
    private static class SplitContext {
        private final PageCollection pageCollection;
        private final Map<Long, BTreeSetPage> modifiedParents = new HashMap<>();
        private final Map<Long, List<BTreeSetPage.PagePointer>> deletionsByParent = new HashMap<>();
        private final Map<Long, List<BTreeSetPage.PagePointer>> insertionsByParent = new HashMap<>();

        void deleted(BTreeSetPage.PagePointer pagePointer) {
            this.deletionsByParent.computeIfAbsent(pagePointer.getParentPageId(), i -> new ArrayList<>()).add(pagePointer);
        }

        void created(BTreeSetPage.PagePointer pagePointer) {
            this.insertionsByParent.computeIfAbsent(pagePointer.getParentPageId(), i -> new ArrayList<>()).add(pagePointer);
        }

        void forEachDeleted(BiConsumer<BTreeSetPage.IndexPage, List<BTreeSetPage.PagePointer>> c) {
            forEachPage(this.deletionsByParent, c);
        }

        void forEachInserted(BiConsumer<BTreeSetPage.IndexPage, List<BTreeSetPage.PagePointer>> c) {
            forEachPage(this.insertionsByParent, c);
        }

        private void forEachPage(Map<Long, List<BTreeSetPage.PagePointer>> pages, BiConsumer<BTreeSetPage.IndexPage, List<BTreeSetPage.PagePointer>> c) {
            pages.forEach((parentId, pointers) -> {
                val parent = (BTreeSetPage.IndexPage) this.pageCollection.get(parentId);
                c.accept(parent, pointers);
                this.modifiedParents.put(parentId, parent);
            });
        }

        Collection<BTreeSetPage> getModifiedParents() {
            return this.modifiedParents.values();
        }
    }

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
