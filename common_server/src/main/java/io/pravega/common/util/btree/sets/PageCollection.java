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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.annotation.concurrent.ThreadSafe;

/**
 * {@link BTreeSetPage} cache.
 */
@ThreadSafe
class PageCollection {
    private final HashMap<Long, BTreeSetPage> pages = new HashMap<>();
    private final HashMap<Long, Long> deletedPageIds = new HashMap<>();

    /**
     * Gets a {@link BTreeSetPage} by id.
     *
     * @param pageId Page Id.
     * @return The Page, or null if the page hasn't been cached.
     */
    synchronized BTreeSetPage get(long pageId) {
        return this.pages.getOrDefault(pageId, null);
    }

    /**
     * Adds a new {@link BTreeSetPage} to the {@link PageCollection}.
     *
     * @param page The page to add.
     */
    synchronized void pageLoaded(BTreeSetPage page) {
        this.pages.put(page.getPagePointer().getPageId(), page);
    }

    /**
     * Updates a {@link BTreeSetPage}.
     *
     * @param page The page to update.
     */
    synchronized void pageUpdated(BTreeSetPage page) {
        assert !this.deletedPageIds.containsKey(page.getPagePointer().getPageId());
        this.pages.put(page.getPagePointer().getPageId(), page);
    }

    /**
     * Marks a {@link BTreeSetPage} as deleted.
     *
     * @param page The page that has been deleted.
     */
    synchronized void pageDeleted(BTreeSetPage page) {
        this.deletedPageIds.put(page.getPagePointer().getPageId(), page.getPagePointer().getParentPageId());
        this.pages.remove(page.getPagePointer().getPageId());
    }

    /**
     * Removes a {@link BTreeSetPage} from this {@link PageCollection}.
     *
     * @param page The page to remove.
     */
    synchronized void remove(BTreeSetPage page) {
        this.deletedPageIds.remove(page.getPagePointer().getPageId());
        this.pages.remove(page.getPagePointer().getPageId());
    }

    /**
     * Gets an unordered collection of leaf {@link BTreeSetPage}s that have been modified and not deleted.
     *
     * @return The modified, but not deleted Leaf pages in this {@link PageCollection}.
     */
    synchronized Collection<BTreeSetPage> getLeafPages() {
        return this.pages.values().stream().filter(p -> !p.isIndexPage()).collect(Collectors.toList());
    }

    synchronized Collection<BTreeSetPage> getIndexPages() {
        return this.pages.values().stream().filter(BTreeSetPage::isIndexPage).collect(Collectors.toList());
    }

    /**
     * Gets an unordered collection of index {@link BTreeSetPage}s that have been modified and not deleted.
     *
     * @return The modified, but not deleted Index pages in this {@link PageCollection}.
     */
    synchronized Collection<BTreeSetPage> getDeletedPagesParents() {
        return this.deletedPageIds.values().stream()
                .map(this::get)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    /**
     * Gets an unordered collection of Ids referring to pages that have been deleted.
     *
     * @return The ids of deleted pages.
     */
    synchronized Collection<Long> getDeletedPageIds() {
        return new ArrayList<>(this.deletedPageIds.keySet());
    }
}