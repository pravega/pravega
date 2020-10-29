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

import com.google.common.base.Preconditions;
import io.pravega.common.TimeoutTimer;
import io.pravega.common.util.ArrayView;
import io.pravega.common.util.AsyncIterator;
import io.pravega.common.util.BufferViewComparator;
import io.pravega.common.util.ByteArraySegment;
import java.time.Duration;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import lombok.NonNull;
import lombok.val;

/**
 * {@link AsyncIterator} for items in a {@link BTreeSet}.
 */
class ItemIterator implements AsyncIterator<List<ArrayView>> {
    //region Members

    private static final Comparator<ArrayView> COMPARATOR = BTreeSet.COMPARATOR;
    private final ArrayView firstItem;
    private final boolean firstItemInclusive;
    private final ArrayView lastItem;
    private final boolean lastItemInclusive;
    private final LocatePage locatePage;
    private final Duration fetchTimeout;
    private final AtomicBoolean finished;
    private final PageCollection pageCollection;
    private final AtomicReference<BTreeSetPage.LeafPage> lastPage;
    private final AtomicInteger processedPageCount;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the {@link ItemIterator} class.
     *
     * @param firstItem          An {@link ArrayView} indicating the first Item to iterate from. If null, the iteration
     *                           will begin with the first item in the index.
     * @param firstItemInclusive If true, firstIem will be included in the iteration (provided it exists), otherwise it
     *                           will be excluded. This argument is ignored if firstItem is null.
     * @param lastItem           An {@link ArrayView} indicating the last Item to iterate to. If null, the iteration will
     *                           end with the last item in the index.
     * @param lastItemInclusive  If true, lastItem will be included in the iteration (provided it exists), otherwise it
     *                           will be excluded. This argument is ignored if lastItem is null.
     * @param locatePage         A Function that can be used to locate a specific {@link BTreeSetPage.LeafPage}.
     * @param fetchTimeout       Timeout for each invocation of locatePage.
     */
    ItemIterator(@Nullable ArrayView firstItem, boolean firstItemInclusive, @Nullable ArrayView lastItem, boolean lastItemInclusive,
                 @NonNull LocatePage locatePage, @NonNull Duration fetchTimeout) {
        if (firstItem == null) {
            // FirstItem is used to bootstrap the iterator, so if it's missing we need to replace it with our Min Value.
            this.firstItem = new ByteArraySegment(BufferViewComparator.getMinValue());
            this.firstItemInclusive = true;
        } else {
            this.firstItem = firstItem;
            this.firstItemInclusive = firstItemInclusive;
        }

        this.lastItem = lastItem;
        this.lastItemInclusive = lastItemInclusive;
        validateArgs();

        this.locatePage = locatePage;
        this.fetchTimeout = fetchTimeout;
        this.pageCollection = new PageCollection();
        this.lastPage = new AtomicReference<>(null);
        this.finished = new AtomicBoolean();
        this.processedPageCount = new AtomicInteger();
    }

    private void validateArgs() {
        if (this.firstItem == null || this.lastItem == null) {
            // We only need to validate if we have both of them.
            return;
        }

        int c = COMPARATOR.compare(this.firstItem, this.lastItem);
        if (this.firstItemInclusive && this.lastItemInclusive) {
            Preconditions.checkArgument(c <= 0, "firstKey must be smaller than or equal to lastKey.");
        } else {
            Preconditions.checkArgument(c < 0, "firstKey must be smaller than lastKey.");
        }
    }

    //endregion

    //region AsyncIterator Implementation

    /**
     * Attempts to get the next element in the iteration. Please refer to {@link AsyncIterator#getNext()} for details.
     * <p>
     * If this method is invoked concurrently (a second call is initiated prior to the previous call terminating) the
     * state of the {@link ItemIterator} will be corrupted. Consider using {@link AsyncIterator#asSequential}.
     *
     * @return A CompletableFuture that, when completed, will contain a List of {@link ArrayView} instances that are
     * next in the iteration, or null if no more items can be served.
     */
    @Override
    public CompletableFuture<List<ArrayView>> getNext() {
        if (this.finished.get()) {
            return CompletableFuture.completedFuture(null);
        }

        TimeoutTimer timer = new TimeoutTimer(this.fetchTimeout);
        return locateNextPage(timer)
                .thenApply(leafPage -> {
                    // Remember this page (for next time).
                    this.lastPage.set(leafPage);
                    List<ArrayView> result = null;
                    if (leafPage != null) {
                        // Extract the intermediate results from the page.
                        result = extractFromPage(leafPage);
                        this.processedPageCount.incrementAndGet();
                    }

                    // Check if we have reached the last page that could possibly contain some result.
                    if (result == null) {
                        this.finished.set(true);
                    }

                    return result;
                });
    }

    //endregion

    //region Helpers

    private CompletableFuture<BTreeSetPage.LeafPage> locateNextPage(TimeoutTimer timer) {
        if (this.lastPage.get() == null) {
            // This is our very first invocation. Find the page containing the first key.
            return this.locatePage.apply(this.firstItem, this.pageCollection, timer);
        } else {
            // We already have a pointer to a page; find next page.
            return getNextLeafPage(timer);
        }
    }

    private CompletableFuture<BTreeSetPage.LeafPage> getNextLeafPage(TimeoutTimer timer) {
        // Walk up the parent chain as long as the page's Key is the last key in that parent key list.
        // Once we found a Page which has a next key, look up the first Leaf page that exists down that path.
        BTreeSetPage lastPage = this.lastPage.get();
        assert lastPage != null;
        int pageKeyPos;
        do {
            BTreeSetPage.IndexPage parentPage = (BTreeSetPage.IndexPage) this.pageCollection.get(lastPage.getPagePointer().getParentPageId());
            if (parentPage == null) {
                // We have reached the end. No more pages.
                return CompletableFuture.completedFuture(null);
            }

            // Look up the current page's PageKey in the parent and make note of its position.
            val pageKey = lastPage.getPagePointer().getKey();
            val parentPos = parentPage.search(pageKey, 0);
            assert parentPos.isExactMatch() : "expecting exact match";
            pageKeyPos = parentPos.getPosition() + 1;

            // We no longer need this page. Remove it from the PageCollection.
            this.pageCollection.remove(lastPage);
            lastPage = parentPage;
        } while (pageKeyPos == lastPage.getItemCount());

        ArrayView referenceKey = lastPage.getItemAt(pageKeyPos);
        return this.locatePage.apply(referenceKey, this.pageCollection, timer);
    }

    private List<ArrayView> extractFromPage(BTreeSetPage.LeafPage page) {
        // Search for the first and last items' positions. Note that they may not exist in our Item collection.
        int firstIndex;
        if (this.processedPageCount.get() == 0) {
            // This is the first page we are searching in. The first Item we are looking for may be in the middle.
            val startPos = page.search(this.firstItem, 0);

            // Adjust first position if we were requested not to include the first Item. If we don't have an exact match,
            // then this is already pointing to the next Item.
            firstIndex = startPos.getPosition();
            if (startPos.isExactMatch() && !this.firstItemInclusive) {
                firstIndex++;
            }
        } else {
            // This is not the first page we are searching in. We should include any results from the very beginning.
            firstIndex = 0;
        }

        int lastIndex;
        if (this.lastItem == null) {
            // Include all the items on this page if we do not have a last Item defined.
            lastIndex = page.getItemCount() - 1;
        } else {
            // Adjust the last index if we were requested not to include the last Item.
            val endPos = page.search(this.lastItem, 0);
            lastIndex = endPos.getPosition();
            if (!endPos.isExactMatch() || endPos.isExactMatch() && !this.lastItemInclusive) {
                lastIndex--;
            }
        }

        if (firstIndex > lastIndex) {
            // Either the first Item is the last in this page but firstItemInclusive is false or the first Item would
            // have belonged in this page but it is not. Return an empty list to indicate that we should continue
            // iterating on next pages.
            return Collections.emptyList();
        } else if (lastIndex < 0) {
            // The last Item is not to be found in this page. We are done. Return null to indicate that we should stop.
            return null;
        } else {
            // Construct the result. Based on firstIndex and lastIndex, this may turn out to be empty.
            return page.getItems(firstIndex, lastIndex);
        }
    }
    //endregion


    @FunctionalInterface
    interface LocatePage {
        CompletableFuture<BTreeSetPage.LeafPage> apply(ArrayView key, PageCollection pageCollection, TimeoutTimer timeout);
    }
}
