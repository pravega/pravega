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
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import javax.annotation.concurrent.NotThreadSafe;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.val;

/**
 * Keeps track of Page Splits, Deletions and associated modifications.
 */
@RequiredArgsConstructor
@NotThreadSafe
class TreeModificationContext {
    @Getter
    private final PageCollection pageCollection;
    private final Map<Long, BTreeSetPage> modifiedParents = new HashMap<>();
    private final Map<Long, List<PagePointer>> deletedByParent = new HashMap<>();
    private final Map<Long, List<PagePointer>> createdByParent = new HashMap<>();

    /**
     * Marks the page pointed to by the given {@link PagePointer} as deleted.
     *
     * @param pagePointer The {@link PagePointer}.
     */
    void deleted(PagePointer pagePointer) {
        this.deletedByParent.computeIfAbsent(pagePointer.getParentPageId(), i -> new ArrayList<>()).add(pagePointer);
    }

    /**
     * Marks the page pointed to by the given {@link PagePointer} as having been created..
     *
     * @param pagePointer The {@link PagePointer}.
     */
    void created(PagePointer pagePointer) {
        this.createdByParent.computeIfAbsent(pagePointer.getParentPageId(), i -> new ArrayList<>()).add(pagePointer);
    }

    private void forEachPage(Map<Long, List<PagePointer>> pages, BiConsumer<BTreeSetPage.IndexPage, List<PagePointer>> c,
                             Comparator<PagePointer> pointerComparator) {
        pages.forEach((parentId, pointers) -> {
            val parent = (BTreeSetPage.IndexPage) this.pageCollection.get(parentId);
            pointers.sort(pointerComparator);
            c.accept(parent, pointers);
            this.modifiedParents.put(parentId, parent);
        });
    }

    /**
     * Applies all recorded changes to their parents.
     *
     * @param createdCallback   A callback to invoke for every Parent-ChildPointerList pair for created pages. First argument
     *                          is the parent {@link BTreeSetPage.IndexPage}, second argument is an ordered list of all
     *                          {@link PagePointer}s within that page that have been created.
     * @param deletedCallback   A callback to invoke for every Parent-ChildPointerList pair for deleted pages. First argument
     *                          is the parent {@link BTreeSetPage.IndexPage}, second argument is an ordered list of all
     *                          {@link PagePointer}s within that page that have been deleted
     * @param pointerComparator A Comparator to sort Pointers.
     */
    void accept(BiConsumer<BTreeSetPage.IndexPage, List<PagePointer>> createdCallback,
                BiConsumer<BTreeSetPage.IndexPage, List<PagePointer>> deletedCallback, Comparator<PagePointer> pointerComparator) {
        forEachPage(this.deletedByParent, deletedCallback, pointerComparator);
        forEachPage(this.createdByParent, createdCallback, pointerComparator);
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
