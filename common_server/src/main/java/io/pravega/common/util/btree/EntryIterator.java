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
import io.pravega.common.util.AsyncIterator;
import io.pravega.common.util.BufferViewComparator;
import io.pravega.common.util.ByteArraySegment;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import lombok.NonNull;
import lombok.val;

/**
 * Iterator for keys in a BTreeIndex.
 */
class EntryIterator implements AsyncIterator<List<PageEntry>> {
    //region Members

    private static final BufferViewComparator KEY_COMPARATOR = BufferViewComparator.create();
    private final ByteArraySegment firstKey;
    private final boolean firstKeyInclusive;
    private final ByteArraySegment lastKey;
    private final boolean lastKeyInclusive;
    private final LocatePage locatePage;
    private final Duration fetchTimeout;
    private final AtomicBoolean finished;
    private final PageCollection pageCollection;
    private final AtomicReference<PageWrapper> lastPage;
    private final AtomicInteger processedPageCount;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the EntryIterator class.
     *
     * @param firstKey          A ByteArraySegment indicating the first Key to iterate from.
     * @param firstKeyInclusive If true, firstKey will be included in the iteration (provided it exists), otherwise it will
     *                          be excluded.
     * @param lastKey           A ByteArraySegment indicating the last Key to iterate to.
     * @param lastKeyInclusive  If true, lastKey will be included in the iteration (provided it exists), otherwise it will
     *                          be excluded.
     * @param locatePage        A Function that can be used to locate a specific BTreePage.
     * @param indexLength       The current index length.
     * @param fetchTimeout      Timeout for each invocation of locatePage.
     */
    EntryIterator(@NonNull ByteArraySegment firstKey, boolean firstKeyInclusive, @NonNull ByteArraySegment lastKey, boolean lastKeyInclusive,
                  @NonNull LocatePage locatePage, long indexLength, @NonNull Duration fetchTimeout) {
        // First, verify correctness.
        int c = KEY_COMPARATOR.compare(firstKey, lastKey);
        if (firstKeyInclusive && lastKeyInclusive) {
            Preconditions.checkArgument(c <= 0, "firstKey must be smaller than or equal to lastKey.");
        } else {
            Preconditions.checkArgument(c < 0, "firstKey must be smaller than lastKey.");
        }

        // firstKey and firstKeyInclusive will change as we make progress in our iteration.
        this.firstKey = firstKey;
        this.firstKeyInclusive = firstKeyInclusive;
        this.lastKey = lastKey;
        this.lastKeyInclusive = lastKeyInclusive;
        this.locatePage = locatePage;
        this.fetchTimeout = fetchTimeout;
        this.pageCollection = new PageCollection(indexLength);
        this.lastPage = new AtomicReference<>(null);
        this.finished = new AtomicBoolean();
        this.processedPageCount = new AtomicInteger();
    }

    //endregion

    //region AsyncIterator Implementation

    /**
     * Attempts to get the next element in the iteration. Please refer to {@link AsyncIterator#getNext()} for details.
     *
     * If this method is invoked concurrently (a second call is initiated prior to the previous call terminating) the
     * state of the {@link EntryIterator} will be corrupted. Consider using {@link AsyncIterator#asSequential}.
     *
     * @return A CompletableFuture that, when completed, will contain a List of {@link PageEntry} instances that are
     * next in the iteration, or null if no more items can be served.
     */
    @Override
    public CompletableFuture<List<PageEntry>> getNext() {
        if (this.finished.get()) {
            return CompletableFuture.completedFuture(null);
        }

        TimeoutTimer timer = new TimeoutTimer(this.fetchTimeout);
        return locateNextPage(timer)
                .thenApply(pageWrapper -> {
                    // Remember this page (for next time).
                    this.lastPage.set(pageWrapper);
                    List<PageEntry> result = null;
                    if (pageWrapper != null) {
                        // Extract the intermediate results from the page.
                        result = extractFromPage(pageWrapper);
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

    private CompletableFuture<PageWrapper> locateNextPage(TimeoutTimer timer) {
        if (this.lastPage.get() == null) {
            // This is our very first invocation. Find the page containing the first key.
            return this.locatePage.apply(this.firstKey, this.pageCollection, timer);
        } else {
            // We already have a pointer to a page; find next page.
            return getNextLeafPage(timer);
        }
    }

    private CompletableFuture<PageWrapper> getNextLeafPage(TimeoutTimer timer) {
        // Walk up the parent chain as long as the page's Key is the last key in that parent key list.
        // Once we found a Page which has a next key, look up the first Leaf page that exists down that path.
        PageWrapper lastPage = this.lastPage.get();
        assert lastPage != null;
        int pageKeyPos;
        do {
            PageWrapper parentPage = lastPage.getParent();
            if (parentPage == null) {
                // We have reached the end. No more pages.
                return CompletableFuture.completedFuture(null);
            }

            // Look up the current page's PageKey in the parent and make note of its position.
            ByteArraySegment pageKey = lastPage.getPointer().getKey();
            val pos = parentPage.getPage().search(pageKey, 0);
            assert pos.isExactMatch() : "expecting exact match";
            pageKeyPos = pos.getPosition() + 1;

            // We no longer need this page. Remove it from the PageCollection.
            this.pageCollection.remove(lastPage);
            lastPage = parentPage;
        } while (pageKeyPos == lastPage.getPage().getCount());

        ByteArraySegment referenceKey = lastPage.getPage().getKeyAt(pageKeyPos);
        return this.locatePage.apply(referenceKey, this.pageCollection, timer);
    }

    private List<PageEntry> extractFromPage(PageWrapper pageWrapper) {
        BTreePage page = pageWrapper.getPage();
        assert !page.getConfig().isIndexPage() : "expecting leaf page";

        // Search for the first and last keys' positions. Note that they may not exist in our Key collection.
        int firstIndex;
        if (this.processedPageCount.get() == 0) {
            // This is the first page we are searching in. The first Key we are looking for may be in the middle.
            val startPos = page.search(this.firstKey, 0);

            // Adjust first index if we were requested not to include the first key. If we don't have an exact match,
            // then this is already pointing to the next key.
            firstIndex = startPos.getPosition();
            if (startPos.isExactMatch() && !this.firstKeyInclusive) {
                firstIndex++;
            }
        } else {
            // This is not the first page we are searching in. We should include any results from the very beginning.
            firstIndex = 0;
        }

        // Adjust the last index if we were requested not to include the last key.
        val endPos = page.search(this.lastKey, 0);
        int lastIndex = endPos.getPosition();
        if (!endPos.isExactMatch() || endPos.isExactMatch() && !this.lastKeyInclusive) {
            lastIndex--;
        }

        if (firstIndex > lastIndex) {
            // Either the first key is the last in this page but firstKeyInclusive is false or the first key would
            // have belonged in this page but it is not. Return an empty list to indicate that we should continue
            // iterating on next pages.
            return Collections.emptyList();
        } else if (lastIndex < 0) {
            // The last key is not to be found in this page. We are done. Return null to indicate that we should stop.
            return null;
        } else {
            // Construct the result. Based on firstIndex and lastIndex, this may turn out to be empty.
            return page.getEntries(firstIndex, lastIndex);
        }
    }

    //endregion

    @FunctionalInterface
    interface LocatePage {
        CompletableFuture<PageWrapper> apply(ByteArraySegment key, PageCollection pageCollection, TimeoutTimer timeout);
    }
}
