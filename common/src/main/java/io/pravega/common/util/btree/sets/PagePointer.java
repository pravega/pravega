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

import io.pravega.common.util.ArrayView;
import lombok.Data;

/**
 * Pointer to a {@link BTreeSetPage}.
 */
@Data
class PagePointer {
    static final long ROOT_PAGE_ID = 0L;
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
}
