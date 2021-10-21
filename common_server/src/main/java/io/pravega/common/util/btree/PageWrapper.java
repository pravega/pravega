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

import io.pravega.common.util.ByteArraySegment;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.concurrent.ThreadSafe;
import lombok.Getter;
import lombok.Setter;

/**
 * Wraps a BTreePage by adding additional metadata, such as parent information and offset.
 */
@ThreadSafe
class PageWrapper {
    //region Members

    private final AtomicReference<BTreePage> page;
    @Getter
    private final PageWrapper parent;
    @Getter
    private final PagePointer pointer;
    @Getter
    private final boolean newPage;
    private final AtomicLong offset;
    private final AtomicLong minOffset;
    private final AtomicBoolean needsFirstKeyUpdate;
    @Getter
    @Setter
    private volatile int entryCountDelta;

    //endregion

    //region Constructor

    private PageWrapper(BTreePage page, PageWrapper parent, PagePointer pointer, boolean newPage) {
        this.page = new AtomicReference<>(page);
        this.parent = parent;
        this.pointer = pointer;
        this.newPage = newPage;
        this.offset = new AtomicLong(this.pointer == null ? PagePointer.NO_OFFSET : this.pointer.getOffset());
        this.minOffset = new AtomicLong(this.pointer == null ? PagePointer.NO_OFFSET : this.pointer.getMinOffset());
        this.needsFirstKeyUpdate = new AtomicBoolean(false);
        this.entryCountDelta = 0;
    }

    /**
     * Creates a new instance of the PageWrapper class for an existing Page.
     *
     * @param page    Page to wrap.
     * @param parent  Page's Parent.
     * @param pointer Page Pointer.
     */
    static PageWrapper wrapExisting(BTreePage page, PageWrapper parent, PagePointer pointer) {
        return new PageWrapper(page, parent, pointer, false);
    }

    /**
     * Creates a new instance of the PageWrapper class for a new Page.
     *
     * @param page    Page to wrap.
     * @param parent  Page's Parent.
     * @param pointer Page Pointer.
     */
    static PageWrapper wrapNew(BTreePage page, PageWrapper parent, PagePointer pointer) {
        return new PageWrapper(page, parent, pointer, true);
    }

    //endregion

    //region Properties

    /**
     * Gets a pointer to the BTreePage wrapped by this instance.
     *
     * @return The BTreePage.
     */
    BTreePage getPage() {
        return this.page.get();
    }

    /**
     * Gets a value indicating whether the wrapped BTreePage is an index page or leaf page.
     */
    boolean isIndexPage() {
        return getPage().getConfig().isIndexPage();
    }

    /**
     * Gets a value indicating whether the wrapped BTreePage needs its Fist Key updated. Please refer to
     * BTreeIndex.updateFirstKey() for more information.
     *
     * @return True if this is an Index Page and either the First Key Update marker was set or this is a new page. False otherwise.
     */
    boolean needsFirstKeyUpdate() {
        return isIndexPage() && (this.needsFirstKeyUpdate.get() || isNewPage());
    }

    /**
     * Indicates that the wrapped BTreePage must have its first key updated.
     */
    void markNeedsFirstKeyUpdate() {
        this.needsFirstKeyUpdate.set(true);
    }

    /**
     * Updates the wrapped BTreePage with the given instance.
     *
     * @param page The BTreePage to wrap.
     */
    void setPage(BTreePage page) {
        this.page.set(page);
    }

    /**
     * Gets a value representing the offset of the wrapped BTreePage.
     */
    long getOffset() {
        return this.offset.get();
    }

    /**
     * Updates the offset of the wrapped BTreePage.
     *
     * @param value The offset to assign.
     */
    void setOffset(long value) {
        if (this.pointer != null && this.offset.get() != this.pointer.getOffset()) {
            // We have already assigned an offset to this.
            throw new IllegalStateException("Cannot assign offset more than once.");
        }

        this.offset.set(value);
    }

    /**
     * Gets a value indicating the Minimum Page Offset for this Page (which is the smallest Offset of this page and any
     * pages descending from it).
     */
    long getMinOffset() {
        return this.minOffset.get();
    }

    /**
     * Updates the Minimum Page Offset for this Page.
     *
     * @param value The offset to set.
     */
    void setMinOffset(long value) {
        this.minOffset.set(value);
    }

    /**
     * Gets a ByteArraySegment representing the Page Key. A Page Key is the Key that the Page with which the page is
     * registered in its parent. Most of the times this is the first Key in the Page, however if the first key is deleted,
     * there may be disagreement (but the page key cannot change).
     *
     * @return The Page Key.
     */
    ByteArraySegment getPageKey() {
        ByteArraySegment r = this.pointer == null ? null : this.pointer.getKey();
        if (r == null && getPage().getCount() > 0) {
            r = getPage().getKeyAt(0);
        }
        return r;
    }

    @Override
    public String toString() {
        return String.format("Offset = %s, Pointer = {%s}", this.offset, this.pointer);
    }

    //endregion
}