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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.sun.istack.internal.NotNull;
import io.pravega.common.util.ArrayView;
import io.pravega.common.util.BitConverter;
import io.pravega.common.util.ByteArrayComparator;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.common.util.IllegalDataFormatException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.function.Supplier;
import lombok.Data;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;

abstract class BTreeListPage {
    //region Serialization format

    /**
     * Format Version related fields. The version itself is the first byte of the serialization. When we will have to
     * support multiple versions, we will need to read this byte and choose the appropriate deserialization approach.
     * We cannot use VersionedSerializer in here - doing so would prevent us from efficiently querying and modifying the
     * page contents itself, as it would force us to load everything in memory (as objects) and then reserialize them.
     * <p>
     * Serialization Format:
     * - Index Page: VFC|I[0]...I[C-1]|K[0]P[0]..K[C-1]P[C-1]|PID
     * - Leaf Page: VFC|I[0]...I[C-1]|K[0]..K[C-1]|PID
     * - Legend:
     * -- V: Version (1 byte)
     * -- F: Flags (1 byte)
     * -- C: Item Count (4 bytes)
     * -- I[i]: Index of Item with offset 'i' (4 bytes)
     * -- K[i]: Item i (I[i+1]-I[i] bytes)
     * -- P[i]: Page Id associated with K[i] (2 bytes)
     * -- PID: This page's ID (8 bytes)
     */
    private static final byte CURRENT_VERSION = 0;
    private static final int VERSION_OFFSET = 0;
    private static final int VERSION_LENGTH = 1; // Maximum 256 versions.

    /**
     * Page Flags.
     */
    private static final int FLAGS_OFFSET = VERSION_OFFSET + VERSION_LENGTH;
    private static final int FLAGS_LENGTH = 1; // Maximum 8 flags.
    private static final byte FLAG_NONE = 0;
    private static final byte FLAG_INDEX_PAGE = 0x1; // If set, indicates this is an Index Page; if not, it's a Leaf Page.

    /**
     * Item Count.
     */
    private static final int COUNT_OFFSET = FLAGS_OFFSET + FLAGS_LENGTH;
    private static final int COUNT_LENGTH = 4;

    /**
     * Item offsets (within the page).
     */
    private static final int ITEM_OFFSETS_OFFSET = COUNT_OFFSET + COUNT_LENGTH;
    private static final int ITEM_OFFSET_LENGTH = Integer.BYTES;
    private static final int PAGE_ID_LENGTH = Long.BYTES;
    @VisibleForTesting
    static final int HEADER_FOOTER_LENGTH = ITEM_OFFSETS_OFFSET + PAGE_ID_LENGTH;

    //endregion

    //region Members

    private static final ByteArrayComparator COMPARATOR = new ByteArrayComparator();
    @Getter
    private final PagePointer pagePointer;
    @Getter
    private final long parentPageId;
    @Getter
    private ArrayView contents;
    @Getter
    private int itemCount;

    //endregion

    //region Constructor

    private BTreeListPage(@NonNull BTreeListPage.PagePointer pagePointer, long parentPageId) {
        this.pagePointer = pagePointer;
        this.parentPageId = parentPageId;
        this.contents = newContents(0, 0, this.pagePointer.getPageId());
        this.itemCount = 0;
    }

    private BTreeListPage(@NonNull BTreeListPage.PagePointer pagePointer, long parentPageId, @NotNull ArrayView contents) {
        this.pagePointer = pagePointer;
        this.parentPageId = parentPageId;
        this.contents = loadContents(contents);
    }

    static BTreeListPage parse(PagePointer pagePointer, long parentPageId, @NotNull ArrayView contents) {
        byte version = contents.get(0);
        if (version != CURRENT_VERSION) {
            throw new IllegalDataFormatException("Unsupported version. PageId=%s, Expected=%s, Actual=%s.",
                    pagePointer.getPageId(), CURRENT_VERSION, version);
        }

        long serializedPageId = BitConverter.readLong(contents, contents.getLength() - PAGE_ID_LENGTH);
        if (pagePointer.getPageId() != serializedPageId) {
            throw new IllegalDataFormatException("Invalid serialized Page Id. Expected=%s, Actual=%s.",
                    pagePointer.getPageId(), serializedPageId);
        }

        byte flags = contents.get(1);
        boolean isIndex = (flags & FLAG_INDEX_PAGE) == FLAG_INDEX_PAGE;
        return isIndex
                ? new IndexPage(pagePointer, parentPageId, contents)
                : new LeafPage(pagePointer, parentPageId, contents);
    }

    private ArrayView newContents(int itemCount, int contentsSize, long pageId) {
        byte[] contents = new byte[HEADER_FOOTER_LENGTH + itemCount * ITEM_OFFSET_LENGTH + contentsSize];
        contents[0] = CURRENT_VERSION;
        contents[1] = getFlags();
        BitConverter.writeInt(contents, COUNT_OFFSET, itemCount);
        BitConverter.writeLong(contents, contents.length - PAGE_ID_LENGTH, pageId);
        return new ByteArraySegment(contents);
    }


    private ArrayView loadContents(ArrayView contents) {
        // Version, Flags and PageId are already validated in parse(Long, Long, ArrayView).
        int itemCount = BitConverter.readInt(contents, COUNT_OFFSET);
        if (itemCount < 0) {
            throw new IllegalDataFormatException("Invalid ItemCount. PageId=%s, Actual=%s.", this.pagePointer.getPageId(), itemCount);
        }

        this.itemCount = itemCount;
        return contents;
    }

    private byte getFlags() {
        byte result = FLAG_NONE;
        if (isIndexPage()) {
            result |= FLAG_INDEX_PAGE;
        }
        return result;
    }

    //endregion

    //region Operations

    /**
     * Gets a value indicating the size (in bytes) of the values associated with an item.
     *
     * @return The size.
     */
    abstract int getValueLength();

    /**
     * Gets a value indicating whether this is an Index page.
     *
     * @return True if Index page, false if Leaf Page.
     */
    abstract boolean isIndexPage();

    /**
     * Gets the size of this page's serialization, in bytes.
     *
     * @return The number of bytes in this page.
     */
    int size() {
        return this.contents.getLength();
    }

    /**
     * Gets the size of the contents of this page (without header or footer), in bytes.
     *
     * @return The number of bytes occupied by the data in this page.
     */
    int getContentSize() {
        return size() - HEADER_FOOTER_LENGTH - this.itemCount * ITEM_OFFSET_LENGTH;
    }

    @Override
    public String toString() {
        return String.format("%s: Size=%s, ContentSize=%s, Count=%s.", isIndexPage() ? "I" : "L", size(), getContentSize(), getItemCount());
    }

    /**
     * Gets the item at the given position.
     *
     * @param position The position to query.
     * @return An {@link ArrayView} representing the item at the given position.
     */
    ArrayView getItemAt(int position) {
        Preconditions.checkArgument(position >= 0 && position < this.itemCount,
                "position must be non-negative and smaller than the item count (%s). Given %s.", this.itemCount, position);
        int offset = getOffset(position);
        int length = getOffset(position + 1) - offset - getValueLength();
        return this.contents.slice(offset, length);
    }

    /**
     * Performs a (binary) search for the given Key in this BTreePage and returns its position.
     *
     * @param item     A ByteArraySegment that represents the key to search.
     * @param startPos The starting position (not array index) to begin the search at. Any positions prior to this one
     *                 will be ignored.
     * @return A SearchResult instance with the result of the search.
     */
    SearchResult search(@NonNull ArrayView item, int startPos) {
        // Positions here are not indices into "source", rather they are entry positions, which is why we always need
        // to adjust by using entryLength.
        int endPos = getItemCount();
        Preconditions.checkArgument(startPos >= 0 && startPos <= endPos,
                "startPos must be non-negative and smaller than the number of items.");
        while (startPos < endPos) {
            // Locate the Item in the middle.
            int midPos = startPos + (endPos - startPos) / 2;

            // Compare it to the sought Item.
            int c = COMPARATOR.compare(item, getItemAt(midPos));
            if (c == 0) {
                // Exact match.
                return new SearchResult(midPos, true);
            } else if (c < 0) {
                // Search again to the left.
                endPos = midPos;
            } else {
                // Search again to the right.
                startPos = midPos + 1;
            }
        }

        // Return an inexact search result with the position where the sought item would have been.
        return new SearchResult(startPos, false);
    }

    //endregion

    //region Updates

    private <T> void update(List<UpdateItem> updates, List<T> values, SerializeValue<T> serializeValue) {
        assert updates.size() > 0;
        val updateInfo = preProcessUpdate(updates, values);
        assert getValueLength() == 0 || updateInfo.inserts.isEmpty() || (values.size() == updates.size() && serializeValue != null);
        this.contents = applyUpdates(updateInfo, serializeValue);
        this.itemCount = updateInfo.newCount;
        assert this.getContentSize() == updateInfo.newContentSize;
    }

    private <T> UpdateInfo<T> preProcessUpdate(List<UpdateItem> updates, List<T> values) {
        Preconditions.checkArgument(updates.get(0).getItem().getLength() > 0, "No empty items allowed.");
        val removedPositions = new HashSet<Integer>(); // Positions in the BTreeListPage.
        val inserts = new ArrayList<InsertInfo<T>>(); // Indices in updates.
        int sizeDelta = 0;

        ArrayView lastItem = null;
        int lastPos = 0;
        for (int i = 0; i < updates.size(); i++) {
            val u = updates.get(i);
            if (lastItem != null) {
                Preconditions.checkArgument(COMPARATOR.compare(lastItem, u.getItem()) < 0,
                        "Items must be sorted and no duplicates are allowed.");
            }

            // Figure out if this entry exists already.
            val searchResult = search(u.getItem(), lastPos);
            int itemLength = u.getItem().getLength() + getValueLength();
            if (searchResult.isExactMatch()) {
                if (u.isRemoval()) {
                    // Entry exists and we were asked to remove it. Collect its position in the page.
                    removedPositions.add(searchResult.getPosition());
                    sizeDelta -= itemLength;
                }
            } else if (!u.isRemoval()) {
                // Entry does not exist and we were asked to insert it. Collect its index in the list.
                int newPos = searchResult.getPosition() - removedPositions.size() + inserts.size();
                inserts.add(new InsertInfo<>(updates.get(i).getItem(), values == null ? null : values.get(i), newPos));
                sizeDelta += itemLength;
            }

            lastPos = searchResult.getPosition();
            lastItem = u.getItem();
        }

        int newCount = getItemCount() + inserts.size() - removedPositions.size();
        int newContentSize = getContentSize() + sizeDelta;
        assert newContentSize >= 0;
        return new UpdateInfo<>(removedPositions, inserts, newCount, newContentSize);
    }

    private <T> ArrayView applyUpdates(UpdateInfo<T> updates, SerializeValue<T> serializeValue) {
        val newContent = newContents(updates.newCount, updates.newContentSize, this.pagePointer.getPageId());
        if (updates.newCount == 0) {
            // Nothing more to do.
            return newContent;
        }

        // We begin with the first position.
        int targetPos = 0;

        // The absolute offset to write at.
        int targetOffset = ITEM_OFFSETS_OFFSET + updates.newCount * ITEM_OFFSET_LENGTH;
        int insertIndex = 0;
        for (int sourcePos = 0; sourcePos < this.itemCount; sourcePos++) {
            val sourceItem = getItemAt(sourcePos);

            // Add all insertions prior to this one.
            while (insertIndex < updates.inserts.size()
                    && COMPARATOR.compare(sourceItem, updates.inserts.get(insertIndex).item) > 0) {
                InsertInfo<T> insert = updates.inserts.get(insertIndex);
                insert(insert, newContent, targetPos, targetOffset, serializeValue);

                targetOffset += insert.item.getLength() + getValueLength();
                insertIndex++;
                targetPos++;
            }

            if (!updates.removedPositions.contains(sourcePos)) {
                // Then add this one (if not deleted).
                // Record it's offset.
                setOffset(newContent, targetPos, targetOffset);

                // Copy it from the source to target.
                ArrayView sourceSlice = this.contents.slice(getOffset(sourcePos), sourceItem.getLength() + getValueLength());
                ArrayView targetSlice = newContent.slice(targetOffset, sourceSlice.getLength());
                copyData(sourceSlice, targetSlice);

                targetOffset += targetSlice.getLength();
                targetPos++;
            }
        }

        // Don't forget to add remaining insertions.
        while (insertIndex < updates.inserts.size()) {
            InsertInfo<T> insert = updates.inserts.get(insertIndex);
            insert(insert, newContent, targetPos, targetOffset, serializeValue);

            targetOffset += insert.item.getLength() + getValueLength();
            insertIndex++;
            targetPos++;
        }

        return newContent;
    }

    private <T> void insert(InsertInfo<T> insert, ArrayView newContent, int targetPos, int targetOffset, SerializeValue<T> serializeValue) {
        // Record the Item's offset.
        setOffset(newContent, targetPos, targetOffset);

        // Insert Item.
        copyData(insert.item, newContent.slice(targetOffset, insert.item.getLength()));

        // Insert value (if any).
        if (insert.value != null) {
            serializeValue.accept(
                    newContent.slice(targetOffset + insert.item.getLength(), getValueLength()), 0, insert.value);
        }
    }

    //endregion

    /**
     * Splits this page in multiple pages (if necessary), where each page's serialization will not exceed the given page size.
     * If no split is necessary, returns null.
     *
     * @param getNewPageId
     * @return
     */
    List<BTreeListPage> split(int maxPageSize, @NotNull Supplier<Long> getNewPageId) {
        if (size() <= maxPageSize) {
            // No split necessary.
            return null;
        }

        // Include the offsets for each item.
        int itemCount = getItemCount();
        int adjustedContentLength = size() - HEADER_FOOTER_LENGTH;

        // Estimate the number of splits.
        int maxContentLength = maxPageSize - HEADER_FOOTER_LENGTH;
        int splitCount = adjustedContentLength / maxContentLength;
        if (adjustedContentLength % maxContentLength != 0) {
            splitCount++;
        }

        // Calculate a threshold for each new page. Once we exceed this we can add items to the next page.
        int splitThreshold = Math.min(maxContentLength, adjustedContentLength / splitCount);
        val result = new ArrayList<BTreeListPage>();
        int sourcePos = 0;
        int currentPageSouceIndex = getOffset(sourcePos);
        while (sourcePos < itemCount) {
            // Each iteration of the outer loop is creating a new page.
            val newOffsets = new ArrayList<Integer>();
            ArrayView firstKey = null;
            int itemSourceIndex = currentPageSouceIndex;
            int newContentSize = 0;

            // Consider a new item for this page as long as we haven't processed all items and we can still add items
            // to this page.
            while (sourcePos < itemCount && newContentSize < splitThreshold) {
                int nextIndex = getOffset(sourcePos + 1);
                int itemLength = nextIndex - itemSourceIndex + ITEM_OFFSET_LENGTH;
                if (firstKey == null) {
                    firstKey = getItemAt(sourcePos);
                } else if (newContentSize + itemLength > maxContentLength) {
                    // Even though we haven't exceeded the threshold, we would be exceeding the max size for this page.
                    //    sourceIndex = nextIndex;
                    break;
                }

                newContentSize += itemLength; // Add the length of the next item.
                newOffsets.add(itemSourceIndex - currentPageSouceIndex);
                itemSourceIndex = nextIndex;
                sourcePos++;
            }

            // Sanity checks...
            assert firstKey != null;
            assert newOffsets.size() > 0;
            assert newContentSize <= maxContentLength;

            // Create the new page and add it to the result.
            result.add(createSplitPage(new PagePointer(firstKey, getNewPageId.get()), currentPageSouceIndex, newContentSize, newOffsets));
            currentPageSouceIndex = itemSourceIndex;

            // TODO: when splitting Index Pages: PageInfo.Key == FirstKey of each page; FirstKey == MinKey for each page.
        }

        return result;
    }

    private BTreeListPage createSplitPage(PagePointer pointer, int fromIndex, int length, List<Integer> relativeOffsets) {
        // Adjust offsets to the new page (they will be shifted over to the beginning).
        val offsetsLength = ITEM_OFFSET_LENGTH * relativeOffsets.size(); // Number of bytes occupied by the offsets part.
        val offsetAdjustment = ITEM_OFFSETS_OFFSET + offsetsLength;
        for (int i = 0; i < relativeOffsets.size(); i++) {
            relativeOffsets.set(i, relativeOffsets.get(i) + offsetAdjustment);
        }

        // Allocate a new buffer for the new page and format it.
        val actualContentLength = length - offsetsLength;
        val newPageContents = newContents(relativeOffsets.size(), actualContentLength, pointer.getPageId());
        for (int i = 0; i < relativeOffsets.size(); i++) {
            setOffset(newPageContents, i, relativeOffsets.get(i));
        }

        // Copy over data.
        val sourceSlice = this.contents.slice(fromIndex, actualContentLength);
        val targetSlice = newPageContents.slice(relativeOffsets.get(0), actualContentLength);
        copyData(sourceSlice, targetSlice);
        return BTreeListPage.parse(pointer, this.parentPageId, newPageContents);
    }

    //region Helpers

    /**
     * Gets the first item in this page, or null if empty.
     *
     * @return The first item.
     */
    private ArrayView getFirstItem() {
        if (this.itemCount == 0) {
            return null;
        }

        return getItemAt(0);
    }

    private ArrayView getValueAt(int position) {
        Preconditions.checkArgument(position >= 0 && position < this.itemCount,
                "position must be non-negative and smaller than the item count (%s). Given %s.", this.itemCount, position);
        int offset = getOffset(position + 1) - getValueLength();
        return this.contents.slice(offset, getValueLength());
    }

    private int getOffset(int position) {
        assert position >= 0 && position <= this.itemCount;
        if (position == this.itemCount) {
            return this.contents.getLength() - PAGE_ID_LENGTH;
        } else {
            return BitConverter.readInt(this.contents, ITEM_OFFSETS_OFFSET + position * ITEM_OFFSET_LENGTH);
        }
    }

    private void setOffset(ArrayView contents, int position, int offset) {
        BitConverter.writeInt(contents, ITEM_OFFSETS_OFFSET + position * ITEM_OFFSET_LENGTH, offset);
    }

    private void copyData(ArrayView from, ArrayView to) {
        from.copyTo(to.array(), to.arrayOffset(), from.getLength());
    }

    //endregion

    //region IndexPage

    static class IndexPage extends BTreeListPage {
        IndexPage(PagePointer pagePointer, long parentPageId) {
            super(pagePointer, parentPageId);
        }

        IndexPage(PagePointer pagePointer, long parentPageId, ArrayView contents) {
            super(pagePointer, parentPageId, contents);
        }

        @Override
        int getValueLength() {
            return PAGE_ID_LENGTH;
        }

        @Override
        boolean isIndexPage() {
            return true;
        }

        /**
         * Adds the given {@link BTreeListPage} as a child.
         *
         * @param pages A list of {@link PagePointer} instances, sorted by {@link PagePointer#getKey()}.
         */
        void addChildren(@NotNull List<PagePointer> pages) {
            val updates = new ArrayList<UpdateItem>();
            val values = new ArrayList<Long>();
            pages.forEach(p -> {
                updates.add(new UpdateItem(p.getKey(), false));
                values.add(p.getPageId());
            });

            super.update(updates, values, BitConverter::writeLong);
        }

        /**
         * Removes the given {@link PagePointer} as a child.
         *
         * @param pages A list of {@link PagePointer} instances, sorted by {@link PagePointer#getKey()}.
         */
        void removeChildren(@NotNull List<PagePointer> pages) {
            val updates = new ArrayList<UpdateItem>();
            pages.forEach(p -> {
                updates.add(new UpdateItem(p.getKey(), true));
            });

            super.update(updates, null, null);
        }

        PagePointer getChildPage(@NonNull ArrayView forItem, int startPos) {
            val searchResult = search(forItem, startPos);
            int position = searchResult.getPosition();
            if (!searchResult.isExactMatch()) {
                position--;
            }

            if (position >= 0) {
                // Found something.
                val serializedValue = super.getValueAt(position);
                val pageId = BitConverter.readLong(serializedValue, 0);
                return new PagePointer(getItemAt(position), pageId);
            }

            // Page is either empty or the sought item is less than the first item.
            return null;
        }
    }

    //endregion

    //region LeafPage

    static class LeafPage extends BTreeListPage {
        LeafPage(PagePointer pagePointer, long parentPageId) {
            super(pagePointer, parentPageId);
        }

        LeafPage(PagePointer pagePointer, long parentPageId, ArrayView contents) {
            super(pagePointer, parentPageId, contents);
        }

        @Override
        int getValueLength() {
            return 0; // There is no value associated with keys in leaf pages.
        }

        @Override
        boolean isIndexPage() {
            return false;
        }

        /**
         * Applies the given {@link UpdateItem}s to this page.
         *
         * @param updates The updates to apply.
         */
        void update(List<UpdateItem> updates) {
            super.update(updates, null, null);
        }
    }

    //endregion

    //region Helper Classes

    @Data
    static class PagePointer {
        private final ArrayView key;
        private final long pageId;
    }

    @Data
    static class UpdateItem implements Comparable<UpdateItem> {
        private final ArrayView item;
        private final boolean removal;

        @Override
        public int compareTo(UpdateItem other) {
            return COMPARATOR.compare(this.item, other.item);
        }
    }

    @RequiredArgsConstructor
    private static class UpdateInfo<T> {
        /**
         * Positions removed from the BTreeListPage.
         */
        final Collection<Integer> removedPositions;
        /**
         * List of Index-to-Position mappings. Indices are in the UpdateList, Positions are from the BTreeListPage.
         */
        final List<InsertInfo<T>> inserts;
        final int newCount;
        final int newContentSize;
    }

    @RequiredArgsConstructor
    private static class InsertInfo<T> {
        final ArrayView item;
        final T value;
        final int targetPos;
    }

    @FunctionalInterface
    interface SerializeValue<T> {
        void accept(ArrayView target, int offset, T value);
    }

    //endregion
}
