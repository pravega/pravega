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

import io.pravega.common.util.ByteArraySegment;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import lombok.NonNull;

/**
 * A {@link BTreeIndex} that does not store its State internally (it relies on an outside storage for it).
 */
public class ExternalStateBTreeIndex extends BTreeIndex {
    //region Members

    private final GetState getState;
    private final WritePages writePages;

    //endregion

    private ExternalStateBTreeIndex(int maxPageSize, int keyLength, int valueLength, ReadPage readPage, @NonNull WritePages writePages,
                                    @NonNull GetState getState, Executor executor) {
        super(maxPageSize, keyLength, valueLength, readPage, executor);
        this.writePages = writePages;
        this.getState = getState;
    }

    //region BTreeIndex implementation

    @Override
    protected CompletableFuture<IndexState> fetchIndexState(Duration timeout) {
        return this.getState
                .apply(timeout)
                .thenApply(state -> state == null ? new IndexState(0, PagePointer.NO_OFFSET, 0) : state);
    }

    @Override
    protected CompletableFuture<Long> writeInternal(List<Map.Entry<Long, ByteArraySegment>> pageContents, Collection<Long> obsoleteOffsets,
                                                    long truncateOffset, long rootOffset, int rootLength, Duration timeout) {
        return this.writePages.apply(pageContents, obsoleteOffsets, truncateOffset, rootOffset, rootLength, timeout);
    }

    @Override
    protected ByteArraySegment createFooterPage(long rootPageOffset, int rootPageLength) {
        return null; // Footers do not apply in this case.
    }

    @Override
    protected long getFooterOffset(long indexLength) {
        throw new UnsupportedOperationException("getFooterOffset not supported on ExternalStateIndex.");
    }

    //endregion

    //region Builder

    /**
     * A Builder for the {@link ExternalStateBTreeIndex} class.
     */
    public static class ExternalBuilder extends Builder<ExternalBuilder> {
        private WritePages writePages;
        private GetState getState;

        /**
         * Associates the given {@link InternalStateBTreeIndex.WritePages} Function with the builder.
         *
         * @param writePages A Function that writes contents of one or more contiguous pages to an external data source.
         * @return This object.
         */
        public ExternalBuilder writePages(WritePages writePages) {
            this.writePages = writePages;
            return this;
        }

        /**
         * Associates the given {@link GetState} Function with the builder.
         *
         * @param getState A Function that returns the State of the index as stored in an external data source.
         * @return This object.
         */
        public ExternalBuilder getState(GetState getState) {
            this.getState = getState;
            return this;
        }

        @Override
        public BTreeIndex build() {
            return new ExternalStateBTreeIndex(this.maxPageSize, this.keyLength, this.valueLength, this.readPage, this.writePages,
                    this.getState, this.executor);
        }

        @Override
        protected ExternalBuilder getThis() {
            return this;
        }
    }

    //endregion

    //region Functional Interfaces

    /**
     * Defines a method that, when invoked, will fetch the current {@link BTreeIndex.IndexState} of the index from the
     * external data source.
     */
    @FunctionalInterface
    public interface GetState {
        CompletableFuture<BTreeIndex.IndexState> apply(Duration timeout);
    }

    /**
     * Defines a method that, when invoked, writes the contents of contiguous BTreePages to the external data source.
     */
    @FunctionalInterface
    public interface WritePages {
        /**
         * Persists the contents of multiple, contiguous Pages to an external data source.
         *
         * @param pageContents    An ordered List of Offset-ByteArraySegments pairs representing the contents of the
         *                        individual pages mapped to their assigned Offsets. The list is ordered by Offset (Keys).
         *                        All entries should be contiguous (an entry's offset is equal to the previous entry's
         *                        offset + the previous entry's length).
         * @param obsoleteOffsets A Collection of offsets for Pages that are no longer relevant. It should be safe to evict
         *                        these from any caching structure involved, however the data should not be removed from
         *                        the data source.
         * @param truncateOffset  An offset within the external data source before which there is no useful data anymore.
         *                        Any data prior to this offset can be safely truncated out (as long as subsequent offsets
         *                        are preserved and do not "shift" downwards - i.e., we have consistent reads).
         * @param rootOffset      The current offset of the root page, as contained in pageContents.
         * @param rootLength      The current length of the root page.
         * @param timeout         Timeout for the operation.
         * @return A CompletableFuture that, when completed, will contain the current length (in bytes) of the index in the
         * external data source.
         */
        CompletableFuture<Long> apply(List<Map.Entry<Long, ByteArraySegment>> pageContents, Collection<Long> obsoleteOffsets,
                                      long truncateOffset, long rootOffset, int rootLength, Duration timeout);
    }

    //endregion
}