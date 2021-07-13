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
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

/**
 * An updateable version of PageCollection. This can be used for any operation that handles the index, including those
 * that modify it.
 */
@ThreadSafe
class UpdateablePageCollection extends PageCollection {
    // region Members

    @GuardedBy("this")
    private long incompleteNewPageOffset;
    @GuardedBy("this")
    private final HashSet<Long> deletedPageOffsets;
    private final AtomicInteger entryCountDelta;
    private final AtomicInteger pageCountDelta;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the UpdateablePageCollection class.
     *
     * @param indexLength The current length of the index.
     */
    UpdateablePageCollection(long indexLength) {
        super(indexLength);
        this.incompleteNewPageOffset = PagePointer.NO_OFFSET;
        this.deletedPageOffsets = new HashSet<>();
        this.entryCountDelta = new AtomicInteger(0);
        this.pageCountDelta = new AtomicInteger(0);
    }

    //endregion

    //region Operations

    /**
     * Inserts a new PageWrapper into this PageCollection.
     *
     * @param page The PageWrapper to insert.
     * @return The inserted PageWrapper.
     * @throws IllegalArgumentException If this method was previously invoked with a PageWrapper having isNewPage() == true
     *                                  but complete() or remove() have not been called on that PageWrapper yet.
     */
    @Override
    synchronized PageWrapper insert(PageWrapper page) {
        Preconditions.checkArgument(this.incompleteNewPageOffset == PagePointer.NO_OFFSET, "Cannot insert new page while a new page is incomplete.");
        if (page.isNewPage()) {
            this.incompleteNewPageOffset = page.getOffset();
            this.pageCountDelta.incrementAndGet();
        }

        return super.insert(page);
    }

    /**
     * Removes the given PageWrapper from this PageCollection.
     *
     * @param page The PageWrapper to remove. This page will have its offset set to PagePointer.NO_OFFSET.
     */
    @Override
    synchronized void remove(PageWrapper page) {
        super.remove(page);
        if (this.incompleteNewPageOffset == page.getOffset()) {
            this.incompleteNewPageOffset = PagePointer.NO_OFFSET;
        }

        this.deletedPageOffsets.add(page.getOffset());
        page.setOffset(PagePointer.NO_OFFSET);
        this.pageCountDelta.decrementAndGet();
        if (!page.isIndexPage()) {
            this.entryCountDelta.addAndGet(page.getEntryCountDelta());
        }
    }

    /**
     * Indicates that any modifications to the given PageWrapper have completed.
     *
     * @param page The PageWrapper that has been completed. This instance's offset will be adjusted to the current value
     *             of getIndexLength(), and the stored index length will be incremented by this PageWrapper's length.
     */
    synchronized void complete(PageWrapper page) {
        Preconditions.checkArgument(this.pageByOffset.containsKey(page.getOffset()), "Given page is not registered.");
        Preconditions.checkArgument(this.incompleteNewPageOffset == PagePointer.NO_OFFSET || this.incompleteNewPageOffset == page.getOffset(),
                "Not expecting this page to be completed.");

        this.incompleteNewPageOffset = PagePointer.NO_OFFSET;
        long pageOffset = this.indexLength;
        this.indexLength += page.getPage().getLength();

        this.pageByOffset.remove(page.getOffset());
        page.setOffset(pageOffset);
        this.pageByOffset.put(page.getOffset(), page);

        if (!page.isIndexPage()) {
            this.entryCountDelta.addAndGet(page.getEntryCountDelta());
        }
    }

    /**
     * Collects the offsets of all removed (deleted) pages.
     *
     * @param target A Collection to collect the offsets into.
     */
    synchronized void collectRemovedPageOffsets(Collection<Long> target) {
        target.addAll(this.deletedPageOffsets);
    }

    /**
     * Collects all the leaf (isIndexPage() == false) PageWrappers into the given Collection.
     *
     * @param target The Collection to collect into.
     */
    synchronized void collectLeafPages(Collection<PageWrapper> target) {
        this.pageByOffset.values().stream().filter(p -> !p.isIndexPage()).forEach(target::add);
    }

    /**
     * Collects the PageWrappers with given offsets into the given Collection.
     *
     * @param offsets A Collection of offsets to collect PageWrappers for.
     * @param target  The Collection to collect into.
     */
    synchronized void collectPages(Collection<Long> offsets, Collection<PageWrapper> target) {
        offsets.forEach(offset -> {
            PageWrapper p = this.pageByOffset.getOrDefault(offset, null);
            if (p != null) {
                target.add(p);
            }
        });
    }

    /**
     * Gets a new List containing all the PageWrappers in this PageCollection, ordered by their offset.
     *
     * @return The List.
     */
    synchronized List<PageWrapper> getPagesSortedByOffset() {
        return this.pageByOffset
                .values().stream()
                .sorted(Comparator.comparingLong(PageWrapper::getOffset))
                .collect(Collectors.toList());
    }

    int getEntryCountDelta() {
        return this.entryCountDelta.get();
    }

    synchronized int getPageCountDelta() {
        return this.pageCountDelta.get();
    }

    //endregion
}
