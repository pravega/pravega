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

import io.pravega.common.TimeoutTimer;
import io.pravega.common.util.BitConverter;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.common.util.IllegalDataFormatException;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import lombok.NonNull;

/**
 * A {@link BTreeIndex} which stores its State internally, in the index file.
 */
public class InternalStateBTreeIndex extends BTreeIndex {
    //region Members

    private static final int FOOTER_LENGTH = Long.BYTES + Integer.BYTES;
    private final GetLength getLength;
    private final WritePages writePages;

    //endregion

    private InternalStateBTreeIndex(int maxPageSize, int keyLength, int valueLength, ReadPage readPage,
                                    @NonNull WritePages writePages, @NonNull GetLength getLength, Executor executor) {
        super(maxPageSize, keyLength, valueLength, readPage, executor);
        this.getLength = getLength;
        this.writePages = writePages;
    }

    //region BTreeIndex Implementation

    @Override
    protected CompletableFuture<IndexState> fetchIndexState(Duration timeout) {
        TimeoutTimer timer = new TimeoutTimer(timeout);
        return this.getLength
                .apply(timer.getRemaining())
                .thenCompose(length -> {
                    if (length <= FOOTER_LENGTH) {
                        // Empty index.
                        return CompletableFuture.completedFuture(new IndexState(length, PagePointer.NO_OFFSET, 0));
                    }

                    long footerOffset = getFooterOffset(length);
                    return this.read
                            .apply(footerOffset, FOOTER_LENGTH, timer.getRemaining())
                            .thenApply(footer -> readFooter(footer, footerOffset, length));
                });
    }

    @Override
    protected CompletableFuture<Long> writeInternal(List<Map.Entry<Long, ByteArraySegment>> pages, Collection<Long> obsoleteOffsets,
                                                    long truncateOffset, long rootOffset, int rootLength, Duration timeout) {
        return this.writePages.apply(pages, obsoleteOffsets, truncateOffset, timeout);
    }

    @Override
    protected ByteArraySegment createFooterPage(long rootPageOffset, int rootPageLength) {
        byte[] result = new byte[FOOTER_LENGTH];
        BitConverter.writeLong(result, 0, rootPageOffset);
        BitConverter.writeInt(result, Long.BYTES, rootPageLength);
        return new ByteArraySegment(result);
    }

    @Override
    protected long getFooterOffset(long length) {
        return length - FOOTER_LENGTH;
    }

    /**
     * Initializes the BTreeIndex using information from the given footer.
     *
     * @param footer       A ByteArraySegment representing the footer that was written with the last update.
     * @param footerOffset The offset within the data source where the footer is located at.
     * @param indexLength  The length of the index, in bytes, in the data source.
     */
    private IndexState readFooter(ByteArraySegment footer, long footerOffset, long indexLength) {
        if (footer.getLength() != FOOTER_LENGTH) {
            throw new IllegalDataFormatException(String.format("Wrong footer length. Expected %s, actual %s.", FOOTER_LENGTH, footer.getLength()));
        }

        long rootPageOffset = getRootPageOffset(footer);
        int rootPageLength = getRootPageLength(footer);
        if (rootPageOffset + rootPageLength > footerOffset) {
            throw new IllegalDataFormatException(String.format("Wrong footer information. RootPage Offset (%s) + Length (%s) exceeds Footer Offset (%s).",
                    rootPageOffset, rootPageLength, footerOffset));
        }

        return new IndexState(indexLength, rootPageOffset, rootPageLength);
    }

    private long getRootPageOffset(ByteArraySegment footer) {
        return BitConverter.readLong(footer, 0);
    }

    private int getRootPageLength(ByteArraySegment footer) {
        return BitConverter.readInt(footer, Long.BYTES);
    }

    //endregion

    //region Builder

    /**
     * A Builder for the {@link InternalStateBTreeIndex} class.
     */
    public static class InternalBuilder extends Builder<InternalBuilder> {
        private WritePages writePages;
        private GetLength getLength;

        /**
         * Associates the given {@link WritePages} Function with the builder.
         *
         * @param writePages A Function that writes contents of one or more contiguous pages to an external data source.
         * @return This object.
         */
        public InternalBuilder writePages(WritePages writePages) {
            this.writePages = writePages;
            return this;
        }

        /**
         * Associates the given {@link GetLength} Function with the builder.
         *
         * @param getLength   A Function that returns the length of the index, in bytes, as stored in an external data source.
         * @return This object.
         */
        public InternalBuilder getLength(GetLength getLength) {
            this.getLength = getLength;
            return this;
        }

        @Override
        public BTreeIndex build() {
            return new InternalStateBTreeIndex(this.maxPageSize, this.keyLength, this.valueLength, this.readPage, this.writePages,
                    this.getLength, this.executor);
        }

        @Override
        protected InternalBuilder getThis() {
            return this;
        }
    }

    //endregion

    //region Functional Interfaces

    /**
     * Defines a method that, when invoked, returns the Length of the Index in the external data source.
     */
    @FunctionalInterface
    public interface GetLength {
        CompletableFuture<Long> apply(Duration timeout);
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
         * @param timeout         Timeout for the operation.
         * @return A CompletableFuture that, when completed, will contain the current length (in bytes) of the index in the
         * external data source.
         */
        CompletableFuture<Long> apply(List<Map.Entry<Long, ByteArraySegment>> pageContents, Collection<Long> obsoleteOffsets,
                                      long truncateOffset, Duration timeout);
    }

    //endregion
}