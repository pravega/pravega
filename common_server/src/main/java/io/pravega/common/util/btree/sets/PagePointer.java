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
package io.pravega.common.util.btree.sets;

import io.pravega.common.util.ArrayView;
import java.util.Comparator;
import lombok.Data;

/**
 * Pointer to a {@link BTreeSetPage}.
 */
@Data
class PagePointer {
    static final long ROOT_PAGE_ID = -1L;
    private static final long NO_PAGE_ID = Long.MIN_VALUE;
    /**
     * A routing key that represents the low bound for any item in the page pointed to by this.
     */
    private final ArrayView key;
    /**
     * The Id of the page pointed to by this.
     */
    private final long pageId;
    /**
     * The id of this page's parent.
     */
    private final long parentPageId;

    /**
     * Creates a {@link PagePointer} to the root.
     *
     * @return A Root {@link PagePointer}.
     */
    static PagePointer root() {
        return new PagePointer(null, ROOT_PAGE_ID, NO_PAGE_ID);
    }

    /**
     * Gets a value indicating whether this page has a parent or not.
     *
     * @return True if has parent (non-root), false if no parent (root).
     */
    boolean hasParent() {
        return this.parentPageId != NO_PAGE_ID;
    }

    /**
     * Gets a comparator for {@link PagePointer}s that orders based on {@link #getKey()}.
     *
     * @param keyComparator A {@link Comparator} that can be used to compare {@link #getKey()}.
     * @return A Comparator.
     */
    static Comparator<PagePointer> getComparator(Comparator<ArrayView> keyComparator) {
        return (p1, p2) -> keyComparator.compare(p1.getKey(), p2.getKey());
    }

    @Override
    public String toString() {
        return String.format("PageId=%s, ParentId=%s", this.pageId, this.parentPageId);
    }
}
