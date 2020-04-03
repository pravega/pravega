/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.util.btree;

import com.google.common.base.Preconditions;
import io.pravega.common.util.ArrayView;
import io.pravega.common.util.AsyncIterator;
import io.pravega.common.util.ByteArrayComparator;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import lombok.NonNull;
import lombok.val;

/**
 * A B+Tree-backed List.
 */
@NotThreadSafe
public class BTreeList {
    private final ByteArrayComparator COMPARATOR = new ByteArrayComparator();
    private final ReadPage read;
    private final UpdatePages update;

    public BTreeList(int maxPageSize, @NonNull ReadPage readPage, @NonNull UpdatePages updatePages) {
        this.read = readPage;
        this.update = updatePages;
    }

    public CompletableFuture<Void> update(@Nullable Collection<ArrayView> toInsert, @Nullable Collection<ArrayView> toRemove,
                                          @NonNull Supplier<Long> getNextPageId, @NonNull Duration timeout) {
        val updates = new ArrayList<BTreeListPage.UpdateItem>();
        if (toInsert != null) {
            toInsert.forEach(i -> updates.add(new BTreeListPage.UpdateItem(i, false)));
        }

        if (toRemove != null) {
            toRemove.forEach(i -> updates.add(new BTreeListPage.UpdateItem(i, true)));
        }

        updates.sort(BTreeListPage.UpdateItem::compareTo);
        if (updates.size() == 0) {
            // Nothing to do.
            return CompletableFuture.completedFuture(null);
        }

        // The updates are sorted, so any empty items will be placed first.
        Preconditions.checkArgument(updates.get(0).getItem().getLength() > 0, "No empty items allowed.");
        // TODO: verify that items do not exceed some predefined limit.

        return null;
    }

    public AsyncIterator<List<ArrayView>> iterator(@NonNull ArrayView first, boolean firstInclusive,
                                                   @NonNull ArrayView last, boolean lastInclusive, @NonNull Duration fetchTimeout) {
        return null;
    }


    private void split() {
        // TODO implement
        // TODO Index pages first keys:
        // Split Root Data node -> new node is Index; first child page has Key==MinKey
        // Delete Node: if (in parent) DeletedNode.Key==MinKey, set the next sibling's Key as MinKey.
        // Delete node: we cannot use BTreeListPage.GetInfo as that will provide wrong key; we must keep track of the Key
        // as we search down the tree. Tricky stuff.
//        private ArrayView generateMinKey() {
//            return new ByteArraySegment(new byte[]{ByteArrayComparator.MIN_VALUE});
//        }

    }

    private static class UpdateResult {

    }

    /**
     * Defines a method that, when invoked, reads the contents of a single BTreeList Page from the external data source.
     */
    @FunctionalInterface
    public interface ReadPage {
        /**
         * Reads a single Page from an external data source.
         *
         * @param pageId  The Page To read
         * @param timeout Timeout for the operation.
         * @return A CompletableFuture that, when completed, will contain a {@link ArrayView} that represents the contents
         * of the desired Page.
         */
        CompletableFuture<ArrayView> apply(long pageId, Duration timeout);
    }

    /**
     * Defines a method that, when invoked, writes the contents of BTreeList Page to the external data source.
     */
    @FunctionalInterface
    public interface UpdatePages {
        /**
         * Persists the contents of multiple, contiguous Pages to an external data source.
         *
         * @param updatedPages   A Map of Page Id to BTreeList Pages to write.
         * @param removedPageIds A Collection of Page Ids to remove.
         * @param timeout        Timeout for the operation.
         * @return A CompletableFuture that, when completed, will indicate that the operation completed.
         */
        CompletableFuture<Void> apply(Map<Long, ArrayView> updatedPages, Collection<Long> removedPageIds, Duration timeout);
    }

}
