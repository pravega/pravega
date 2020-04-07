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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import javax.annotation.concurrent.NotThreadSafe;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.val;

/**
 * Keeps track of Page Splits and associated modifications.
 */
@RequiredArgsConstructor
@NotThreadSafe
class SplitContext {
    @Getter
    private final PageCollection pageCollection;
    private final Map<Long, BTreeSetPage> modifiedParents = new HashMap<>();
    private final Map<Long, List<PagePointer>> deletionsByParent = new HashMap<>();
    private final Map<Long, List<PagePointer>> insertionsByParent = new HashMap<>();

    /**
     * Marks the page pointed to by the given {@link PagePointer} as deleted.
     *
     * @param pagePointer The {@link PagePointer}.
     */
    void deleted(PagePointer pagePointer) {
        this.deletionsByParent.computeIfAbsent(pagePointer.getParentPageId(), i -> new ArrayList<>()).add(pagePointer);
    }

    /**
     * Marks the page pointed to by the given {@link PagePointer} as having been created..
     *
     * @param pagePointer The {@link PagePointer}.
     */
    void created(PagePointer pagePointer) {
        this.insertionsByParent.computeIfAbsent(pagePointer.getParentPageId(), i -> new ArrayList<>()).add(pagePointer);
    }

    /**
     * Loops through all the deleted {@link PagePointer}s, grouped by their parent {@link BTreeSetPage.IndexPage}.
     *
     * @param c A {@link BiConsumer}. First argument is the parent {@link BTreeSetPage.IndexPage}, second argument is
     *          an ordered list of all {@link PagePointer}s within that page that have been deleted.
     */
    void forEachDeleted(BiConsumer<BTreeSetPage.IndexPage, List<PagePointer>> c) {
        forEachPage(this.deletionsByParent, c);
    }

    /**
     * Loops through all the newly created {@link PagePointer}s, grouped by their parent {@link BTreeSetPage.IndexPage}.
     *
     * @param c A {@link BiConsumer}. First argument is the parent {@link BTreeSetPage.IndexPage}, second argument is
     *          an ordered list of all {@link PagePointer}s within that page that have been created.
     */
    void forEachInserted(BiConsumer<BTreeSetPage.IndexPage, List<PagePointer>> c) {
        forEachPage(this.insertionsByParent, c);
    }

    private void forEachPage(Map<Long, List<PagePointer>> pages, BiConsumer<BTreeSetPage.IndexPage, List<PagePointer>> c) {
        pages.forEach((parentId, pointers) -> {
            val parent = (BTreeSetPage.IndexPage) this.pageCollection.get(parentId);
            c.accept(parent, pointers);
            this.modifiedParents.put(parentId, parent);
        });
    }

    /**
     * Gets a collection of {@link BTreeSetPage}s whose children have been modified.
     *
     * @return A Collection.
     */
    Collection<BTreeSetPage> getModifiedParents() {
        return this.modifiedParents.values();
    }
}
