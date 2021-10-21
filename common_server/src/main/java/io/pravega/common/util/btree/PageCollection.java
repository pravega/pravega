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
import java.util.HashMap;
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
    protected final HashMap<Long, PageWrapper> pageByOffset;
    @GuardedBy("this")
    protected long indexLength;

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
    }

    /**
     * Gets a pointer to the Root Page.
     */
    synchronized PageWrapper getRootPage() {
        return this.pageByOffset.values().stream()
                                .filter(page -> page.getParent() == null)
                                .findFirst().orElse(null);
    }

    //endregion

}
