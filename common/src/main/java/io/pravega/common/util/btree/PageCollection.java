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
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

/**
 * A Collection of BTreePages, indexed by Offset. This can serve as a cache for any operation (but should not be used
 * cross-operations).
 */
@ThreadSafe
class PageCollection {
    //region Private

    @GuardedBy("this")
    private final HashMap<Long, PageWrapper> pageByOffset;
    @GuardedBy("this")
    private long incompleteNewPageOffset;
    @GuardedBy("this")
    private long indexLength;

    //endregion

    /**
     * Creates a new instance of the PageCollection class.
     *
     * @param indexLength The current length of the index.
     */
    PageCollection(long indexLength) {
        Preconditions.checkArgument(indexLength >= 0, "indexLength must be a non-negative number.");
        this.indexLength = indexLength;
        this.pageByOffset = new HashMap<>();
        this.incompleteNewPageOffset = PagePointer.NO_OFFSET;
    }

    //region Operations

    /**
     * Gets a value indicating the length of the index. The value returned will change as new pages are marked as "completed".
     *
     * @return The Length of the index, in bytes.
     */
    synchronized long getIndexLength() {
        return this.indexLength;
    }

    /**
     * Gets a value indicating the number of Pages in this PageCollection (NOTE: this is not the total number of pages
     * in the index).
     *
     * @return The number of Pages in this PageCollection.
     */
    synchronized int getCount() {
        return this.pageByOffset.size();
    }

    /**
     * Gets the PageWrapper that begins at the given offset.
     *
     * @param offset The offset to look up the page at.
     * @return A PageWrapper instance or null, if no such page is registered.
     */
    synchronized PageWrapper get(long offset) {
        return this.pageByOffset.getOrDefault(offset, null);
    }

    /**
     * Inserts a new PageWrapper into this PageCollection.
     *
     * @param page The PageWrapper to insert.
     * @return The inserted PageWrapper.
     * @throws IllegalArgumentException If this method was previously invoked with a PageWrapper having isNewPage() == true
     *                                  but complete() or remove() have not been called on that PageWrapper yet.
     */
    synchronized PageWrapper insert(PageWrapper page) {
        Preconditions.checkArgument(this.incompleteNewPageOffset == PagePointer.NO_OFFSET, "Cannot insert new page while a new page is incomplete.");
        if (page.isNewPage()) {
            this.incompleteNewPageOffset = page.getOffset();
        }

        this.pageByOffset.put(page.getOffset(), page);
        return page;
    }

    /**
     * Removes the given PageWrapper from this PageCollection.
     *
     * @param page The PageWrapper to remove. This page will have its offset set to PagePointer.NO_OFFSET.
     */
    synchronized void remove(PageWrapper page) {
        this.pageByOffset.remove(page.getOffset());
        if (this.incompleteNewPageOffset == page.getOffset()) {
            this.incompleteNewPageOffset = PagePointer.NO_OFFSET;
        }

        page.setOffset(PagePointer.NO_OFFSET);
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
     * @return The List.
     */
    synchronized List<PageWrapper> getPagesSortedByOffset() {
        return this.pageByOffset
                .values().stream()
                .sorted(Comparator.comparingLong(PageWrapper::getOffset))
                .collect(Collectors.toList());
    }

    //endregion
}
