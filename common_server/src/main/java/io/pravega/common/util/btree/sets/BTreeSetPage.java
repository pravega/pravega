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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.pravega.common.util.ArrayView;
import io.pravega.common.util.BufferViewComparator;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.common.util.IllegalDataFormatException;
import io.pravega.common.util.StructuredWritableBuffer;
import io.pravega.common.util.btree.SearchResult;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.function.Supplier;
import javax.annotation.concurrent.NotThreadSafe;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;

/**
 * Represents a Page (Node) within a {@link BTreeSet}. Pages can be of type {@link IndexPage} or {@link LeafPage}.
 */
@NotThreadSafe
abstract class BTreeSetPage {
    //region Serialization format

    /**
     * Format Version related fields. The version itself is the first byte of the serialization. When we will have to
     * support multiple versions, we will need to read this byte and choose the appropriate deserialization approach.
     * We cannot use VersionedSerializer in here - doing so would prevent us from efficiently querying and modifying the
     * page contents itself, as it would force us to load everything in memory (as objects) and then reserialize them.
     *
     * Serialization Format:
     * - Index Page: VFSC|I[0]...I[C-1]|K[0]P[0]..K[C-1]P[C-1]|PID
     * - Leaf Page: VFSC|I[0]...I[C-1]|K[0]..K[C-1]|PID
     * - Legend:
     * -- V: Version (1 byte)
     * -- F: Flags (1 byte)
     * -- S: Total Size of the buffer (4 bytes)
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

    private static final int TOTAL_SIZE_OFFSET = FLAGS_OFFSET + FLAGS_LENGTH;
    private static final int TOTAL_SIZE_LENGTH = 4;

    /**
     * Item Count.
     */
    private static final int COUNT_OFFSET = TOTAL_SIZE_OFFSET + TOTAL_SIZE_LENGTH;
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

    private static final Comparator<ArrayView> COMPARATOR = BTreeSet.COMPARATOR;
    @Getter
    private final PagePointer pagePointer;
    @Getter
    private ArrayView data;
    @Getter
    private int itemCount;
    @Getter
    private boolean modified;

    //endregion

    //region Constructor

    /**
     * Creates a new, empty {@link BTreeSetPage}.
     *
     * @param pagePointer {@link PagePointer} to the page.
     */
    private BTreeSetPage(@NonNull PagePointer pagePointer) {
        this.pagePointer = pagePointer;
        this.data = newContents(0, 0, this.pagePointer.getPageId());
        this.itemCount = 0;
        this.modified = false;
    }

    /**
     * Creates a new instance of a {@link BTreeSetPage} from an existing serialization.
     *
     * @param pagePointer {@link PagePointer} to the page.
     * @param data        The contents of a {@link BTreeSetPage} to wrap.
     */
    private BTreeSetPage(@NonNull PagePointer pagePointer, @NonNull ArrayView data) {
        this.pagePointer = pagePointer;
        loadContents(data);
        this.modified = false;
    }

    /**
     * Creates a new instance of a {@link BTreeSetPage} from an existing serialization.
     *
     * @param pagePointer {@link PagePointer} to the page.
     * @param contents    The contents of the {@link BTreeSetPage} to wrap.
     * @return A {@link IndexPage} or {@link LeafPage}, depending on what the serialization indicates.
     * @throws IllegalDataFormatException If the serialization is invalid.
     */
    static BTreeSetPage parse(PagePointer pagePointer, @NonNull ArrayView contents) {
        byte version = contents.get(0);
        if (version != CURRENT_VERSION) {
            throw new IllegalDataFormatException("Unsupported version. PageId=%s, Expected=%s, Actual=%s.",
                    pagePointer.getPageId(), CURRENT_VERSION, version);
        }

        int size = contents.getInt(TOTAL_SIZE_OFFSET);
        if (size < HEADER_FOOTER_LENGTH || size > contents.getLength()) {
            throw new IllegalDataFormatException("Invalid size. PageId=%s, Expected in range [%s, %s], actual=%s.",
                    pagePointer.getPageId(), TOTAL_SIZE_OFFSET, contents.getLength(), size);
        }

        if (size < contents.getLength()) {
            contents = contents.slice(0, size);
        }

        long serializedPageId = contents.getLong(contents.getLength() - PAGE_ID_LENGTH);
        if (pagePointer.getPageId() != serializedPageId) {
            throw new IllegalDataFormatException("Invalid serialized Page Id. Expected=%s, Actual=%s.",
                    pagePointer.getPageId(), serializedPageId);
        }

        byte flags = contents.get(1);
        boolean isIndex = (flags & FLAG_INDEX_PAGE) == FLAG_INDEX_PAGE;
        return isIndex
                ? new IndexPage(pagePointer, contents)
                : new LeafPage(pagePointer, contents);
    }

    /**
     * Creates a new instance of the {@link BTreeSetPage.LeafPage} with an empty contents and {@link #getPagePointer()}
     * configured as a Root Page.
     *
     * @return A new {@link BTreeSetPage.LeafPage}.
     */
    static BTreeSetPage.LeafPage emptyLeafRoot() {
        return new BTreeSetPage.LeafPage(PagePointer.root());
    }

    /**
     * Creates a new instance of the {@link BTreeSetPage.IndexPage} with an empty contents and {@link #getPagePointer()}
     * configured as a Root Page.
     *
     * @return A new {@link BTreeSetPage.IndexPage}.
     */
    static BTreeSetPage.IndexPage emptyIndexRoot() {
        return new BTreeSetPage.IndexPage(PagePointer.root());
    }

    /**
     * Creates a new {@link BTreeSetPage} buffer with the given args.
     *
     * @param itemCount    The number of items in the page.
     * @param contentsSize The total size of the items in the page.
     * @param pageId       The page Id.
     * @return The buffer.
     */
    private ArrayView newContents(int itemCount, int contentsSize, long pageId) {
        val contents = new ByteArraySegment(new byte[HEADER_FOOTER_LENGTH + itemCount * ITEM_OFFSET_LENGTH + contentsSize]);
        contents.setShort(0, (short) ((CURRENT_VERSION << 8) | getFlags()));
        setSize(contents, contents.getLength());
        contents.setInt(COUNT_OFFSET, itemCount);
        contents.setLong(contents.getLength() - PAGE_ID_LENGTH, pageId);
        return contents;
    }

    /**
     * Loads the given buffer into this page, replacing what it already contained.
     *
     * @param contents The new contents.
     */
    private void loadContents(ArrayView contents) {
        // Version, Flags and PageId are already validated in parse(Long, Long, ArrayView).
        int itemCount = contents.getInt(COUNT_OFFSET);
        if (itemCount < 0) {
            throw new IllegalDataFormatException("Invalid ItemCount. PageId=%s, Actual=%s.", this.pagePointer.getPageId(), itemCount);
        }

        this.itemCount = itemCount;
        this.data = contents;
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
     * Indicates that modifications to this page have concluded and performs any maintenance tasks that may be required.
     */
    abstract void seal();

    /**
     * Gets the size of this page's serialization, in bytes.
     *
     * @return The number of bytes in this page.
     */
    int size() {
        return this.data.getLength();
    }

    /**
     * Gets the size of the contents of this page (without header or footer), in bytes.
     *
     * @return The number of bytes occupied by the data in this page.
     */
    int getContentSize() {
        return size() - HEADER_FOOTER_LENGTH - this.itemCount * ITEM_OFFSET_LENGTH;
    }

    /**
     * Indicates that this page has been modified.
     */
    void markModified() {
        this.modified = true;
    }

    @Override
    public String toString() {
        return String.format("%s: %s, Size=%s, ContentSize=%s, Count=%s.", isIndexPage() ? "I" : "L",
                this.pagePointer, size(), getContentSize(), getItemCount());
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
        return this.data.slice(offset, length);
    }

    /**
     * Sets the item at the given position.
     *
     * @param position The position to set the item at.
     * @param newItem  The new item. The new item must be shorter than or equal to in length to the current item at that
     *                 position.
     */
    private void setItemAt(int position, ArrayView newItem) {
        Preconditions.checkArgument(position >= 0 && position < this.itemCount,
                "position must be non-negative and smaller than the item count (%s). Given %s.", this.itemCount, position);
        int offset = getOffset(position);
        int length = getOffset(position + 1) - offset - getValueLength();
        int delta = newItem.getLength() - length;
        Preconditions.checkArgument(delta <= 0, "Cannot replace an item (%s) with a bigger one (%s).", length, newItem.getLength());

        // Copy new item data.
        copyData(newItem, this.data.slice(offset, newItem.getLength()));

        if (delta != 0) {
            // Update remaining offsets with delta
            for (int pos = position + 1; pos < this.itemCount; pos++) {
                setOffset(this.data, pos, getOffset(pos) + delta);
            }

            // Shift the remainder of the array down.
            byte[] array = this.data.array();
            int arrayOffset = this.data.arrayOffset();
            for (int index = offset + length; index < this.data.getLength(); index++) {
                array[arrayOffset + index + delta] = array[arrayOffset + index];
            }

            this.data = this.data.slice(0, this.data.getLength() + delta);
            setSize(this.data, this.data.getLength());
        }
    }

    /**
     * Gets an ordered list of {@link ArrayView}s representing the items between the two positions.
     *
     * @param firstPos The first position, inclusive.
     * @param lastPos  The last position, inclusive.
     * @return An ordered list of {@link ArrayView}s representing the items between the given positions.
     */
    List<ArrayView> getItems(int firstPos, int lastPos) {
        Preconditions.checkArgument(firstPos <= lastPos, "firstPos must be smaller than or equal to lastPos.");
        Preconditions.checkArgument(firstPos >= 0, "firstPos must be a non-negative integer.");
        Preconditions.checkArgument(lastPos < this.itemCount, "lastPos must be less than %s.", this.itemCount);

        val result = new ArrayList<ArrayView>();
        int offset = getOffset(firstPos);
        for (int pos = firstPos; pos <= lastPos; pos++) {
            int nextOffset = getOffset(pos + 1);
            result.add(this.data.slice(offset, nextOffset - offset - getValueLength()));
            offset = nextOffset;
        }

        return result;
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

    /**
     * Updates the {@link BTreeSetPage} with the given items.
     *
     * @param updates        A list of {@link UpdateItem} to process.
     * @param values         A list of values to associate with the new items. The values will be matched by index to
     *                       their updates. This parameter is not required for removals or leaf pages.
     * @param serializeValue A Function that will serialize a value to a sequence of bytes. This parameter is not required
     *                       for removals or leaf pages.
     * @param <T>            Type for values.
     */
    private <T> void update(List<UpdateItem> updates, List<T> values, SerializeValue<T> serializeValue) {
        assert updates.size() > 0;
        val updateInfo = preProcessUpdate(updates, values);
        assert getValueLength() == 0 || updateInfo.inserts.isEmpty() || (values.size() == updates.size() && serializeValue != null);
        this.data = applyUpdates(updateInfo, serializeValue);
        this.itemCount = updateInfo.newCount;
        assert this.getContentSize() == updateInfo.newContentSize;
        markModified();
    }

    /**
     * Pre-processes updates. Segregates into insertions and removals and excludes item updates.
     *
     * @param updates The updates to pre-process
     * @param values  The values to associate with the updates.
     * @param <T>     Type for values.
     * @return An {@link UpdateInfo} containing all insertions and removals.
     */
    private <T> UpdateInfo<T> preProcessUpdate(List<UpdateItem> updates, List<T> values) {
        Preconditions.checkArgument(updates.get(0).getItem().getLength() > 0, "No empty items allowed.");
        val removedPositions = new HashSet<Integer>(); // Positions in the BTreeSetPage.
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
                inserts.add(new InsertInfo<T>(updates.get(i).getItem(), (T) (values == null ? null : values.get(i)), newPos));
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

    /**
     * Applies the given updates to this {@link BTreeSetPage} and returns a buffer with the changes. Does not alter the
     * current {@link BTreeSetPage}.
     *
     * @param updates        The {@link UpdateInfo} to apply.
     * @param serializeValue A Function to serialize values for insertions. This is not required for removals or leaf pages.
     * @param <T>            Type for values.
     * @return A new buffer that contains the updates. This buffer is constructed from the data in the current {@link BTreeSetPage},
     * to which all updates are applied.
     */
    private <T> ArrayView applyUpdates(UpdateInfo<T> updates, SerializeValue<T> serializeValue) {
        val newData = newContents(updates.newCount, updates.newContentSize, this.pagePointer.getPageId());
        if (updates.newCount == 0) {
            // Nothing more to do.
            return newData;
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
                insert(insert, newData, targetPos, targetOffset, serializeValue);

                targetOffset += insert.item.getLength() + getValueLength();
                insertIndex++;
                targetPos++;
            }

            if (!updates.removedPositions.contains(sourcePos)) {
                // Then add this one (if not deleted).
                // Record it's offset.
                setOffset(newData, targetPos, targetOffset);

                // Copy it from the source to target.
                ArrayView sourceSlice = this.data.slice(getOffset(sourcePos), sourceItem.getLength() + getValueLength());
                ArrayView targetSlice = newData.slice(targetOffset, sourceSlice.getLength());
                copyData(sourceSlice, targetSlice);

                targetOffset += targetSlice.getLength();
                targetPos++;
            }
        }

        // Don't forget to add remaining insertions.
        while (insertIndex < updates.inserts.size()) {
            InsertInfo<T> insert = updates.inserts.get(insertIndex);
            insert(insert, newData, targetPos, targetOffset, serializeValue);

            targetOffset += insert.item.getLength() + getValueLength();
            insertIndex++;
            targetPos++;
        }

        return newData;
    }

    /**
     * Inserts an item.
     *
     * @param insert         The item to insert.
     * @param dataBuffer     The buffer to insert into.
     * @param targetPos      Position to insert at.
     * @param targetOffset   Index to insert at.
     * @param serializeValue A function to serialize values.
     * @param <T>            Type for values.
     */
    private <T> void insert(InsertInfo<T> insert, ArrayView dataBuffer, int targetPos, int targetOffset, SerializeValue<T> serializeValue) {
        // Record the Item's offset.
        setOffset(dataBuffer, targetPos, targetOffset);

        // Insert Item.
        copyData(insert.item, dataBuffer.slice(targetOffset, insert.item.getLength()));

        // Insert value (if any).
        if (insert.value != null) {
            serializeValue.accept(dataBuffer, targetOffset + insert.item.getLength(), insert.value);
        }
    }

    //endregion

    //region Splitting

    /**
     * Splits this page in multiple pages (if necessary), where each page's serialization will not exceed the given page size.
     * If no split is necessary, returns null.
     *
     * @param getNewPageId A Supplier that, when invoked, will return a new, unique page id.
     * @return A List of {@link BTreeSetPage} of the same type as this one. If null, then no split is necessary. If non-null,
     * then the current page
     */
    List<BTreeSetPage> split(int maxPageSize, @NonNull Supplier<Long> getNewPageId) {
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
        val result = new ArrayList<BTreeSetPage>();

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
            // First page in split will replace this one, so we need to set the correct page pointer.
            val pagePointer = getSplitPagePointer(firstKey, getNewPageId, result.isEmpty());
            result.add(createSplitPage(pagePointer, currentPageSouceIndex, newContentSize, newOffsets));
            currentPageSouceIndex = itemSourceIndex;
        }

        return result;
    }

    private PagePointer getSplitPagePointer(ArrayView firstKey, Supplier<Long> getNewPageId, boolean isFirstItem) {
        if (isFirstItem) {
            firstKey = this.pagePointer.getKey() == null ? firstKey : this.pagePointer.getKey();
        }

        long parentId;
        long pageId;
        if (this.pagePointer.hasParent()) {
            // If we don't split the root, all new pages will branch out from this page's parent.
            parentId = this.pagePointer.getParentPageId();

            // ... and the first page in the split will replace this page (so preserve its id).
            pageId = isFirstItem ? this.pagePointer.getPageId() : getNewPageId.get();
        } else {
            // If we split the root, all new items will have the root as parent, and the root Id never changes.
            parentId = PagePointer.ROOT_PAGE_ID;

            // .. and all new pages get new ids.
            pageId = getNewPageId.get();
        }

        return new PagePointer(firstKey, pageId, parentId);
    }

    private BTreeSetPage createSplitPage(PagePointer pointer, int fromIndex, int length, List<Integer> relativeOffsets) {
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
        val sourceSlice = this.data.slice(fromIndex, actualContentLength);
        val targetSlice = newPageContents.slice(relativeOffsets.get(0), actualContentLength);
        copyData(sourceSlice, targetSlice);
        BTreeSetPage result = BTreeSetPage.parse(pointer, newPageContents);
        Preconditions.checkState(result.getItemCount() > 0, "Page split resulted in empty page.");
        result.markModified();
        result.seal();
        return result;
    }

    //endregion

    //region Helpers

    private ArrayView getValueAt(int position) {
        Preconditions.checkArgument(position >= 0 && position < this.itemCount,
                "position must be non-negative and smaller than the item count (%s). Given %s.", this.itemCount, position);
        int offset = getOffset(position + 1) - getValueLength();
        return this.data.slice(offset, getValueLength());
    }

    private int getOffset(int position) {
        assert position >= 0 && position <= this.itemCount;
        if (position == this.itemCount) {
            return this.data.getLength() - PAGE_ID_LENGTH;
        } else {
            return this.data.getInt(ITEM_OFFSETS_OFFSET + position * ITEM_OFFSET_LENGTH);
        }
    }

    private void setOffset(ArrayView contents, int position, int offset) {
        contents.setInt(ITEM_OFFSETS_OFFSET + position * ITEM_OFFSET_LENGTH, offset);
    }

    private void setSize(ArrayView contents, int size) {
        contents.setInt(TOTAL_SIZE_OFFSET, size);
    }

    private void copyData(ArrayView from, ArrayView to) {
        from.copyTo(to.array(), to.arrayOffset(), from.getLength());
    }

    //endregion

    //region IndexPage

    /**
     * {@link BTreeSetPage} that contains pointers to other {@link BTreeSetPage}s.
     */
    static class IndexPage extends BTreeSetPage {
        IndexPage(PagePointer pagePointer) {
            super(pagePointer);
        }

        IndexPage(PagePointer pagePointer, ArrayView contents) {
            super(pagePointer, contents);
        }

        @Override
        int getValueLength() {
            return PAGE_ID_LENGTH;
        }

        @Override
        boolean isIndexPage() {
            return true;
        }

        @Override
        void seal() {
            // Index pages need to have their first item point to Min-Value. Otherwise we will not be able to include
            // items that are between this page's pointer to this page and this pages's first item.
            if (getItemCount() > 0) {
                super.setItemAt(0, new ByteArraySegment(BufferViewComparator.getMinValue()));
            }
        }

        /**
         * Adds the given {@link BTreeSetPage} as a child.
         *
         * @param pages A list of {@link PagePointer} instances, sorted by {@link PagePointer#getKey()}.
         */
        void addChildren(@NonNull List<PagePointer> pages) {
            val updates = new ArrayList<UpdateItem>();
            val values = new ArrayList<Long>();
            pages.forEach(p -> {
                updates.add(new UpdateItem(p.getKey(), false));
                values.add(p.getPageId());
            });

            super.update(updates, values, StructuredWritableBuffer::setLong);
        }

        /**
         * Removes the given {@link PagePointer} as a child.
         *
         * @param pages A list of {@link PagePointer} instances, sorted by {@link PagePointer#getKey()}.
         */
        void removeChildren(@NonNull List<PagePointer> pages) {
            val updates = new ArrayList<UpdateItem>();
            pages.forEach(p -> {
                updates.add(new UpdateItem(p.getKey(), true));
            });

            super.update(updates, null, null);
        }

        /**
         * Gets a {@link PagePointer} for a child page that meets the following conditions:
         * - The {@link PagePointer#getKey()} is smaller than or equal to 'forItem' (using {@link #COMPARATOR}.
         * - The {@link PagePointer} immediately succeeding this one (if any) will have its {@link PagePointer#getKey()}
         * larger than 'forItem' (using {@link #COMPARATOR}.
         *
         * @param forItem  The item to get the {@link PagePointer} for.
         * @param startPos The first position (in this page) to search at.
         * @return A {@link PagePointer}, or null if this page is empty or 'forItem' is smaller than the first {@link PagePointer#getKey()}.
         */
        PagePointer getChildPage(@NonNull ArrayView forItem, int startPos) {
            val searchResult = search(forItem, startPos);
            int position = searchResult.getPosition();
            if (!searchResult.isExactMatch()) {
                position--;
            }

            if (position >= 0) {
                // Found something.
                val serializedValue = super.getValueAt(position);
                val pageId = serializedValue.getLong(0);
                return new PagePointer(getItemAt(position), pageId, getPagePointer().getPageId());
            }

            // Page is either empty or the sought item is less than the first item.
            return null;
        }
    }

    //endregion

    //region LeafPage

    /**
     * {@link BTreeSetPage} that contains items from the {@link BTreeSet}.
     */
    static class LeafPage extends BTreeSetPage {
        LeafPage(PagePointer pagePointer) {
            super(pagePointer);
        }

        LeafPage(PagePointer pagePointer, ArrayView contents) {
            super(pagePointer, contents);
        }

        @Override
        int getValueLength() {
            return 0; // There is no value associated with keys in leaf pages.
        }

        @Override
        boolean isIndexPage() {
            return false;
        }

        @Override
        void seal() {
            Preconditions.checkState(getItemCount() > 0, "Leaf Page split resulted in empty page.");
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

    @RequiredArgsConstructor
    private static class UpdateInfo<T> {
        /**
         * Positions removed from the BTreeSetPage.
         */
        final Collection<Integer> removedPositions;
        /**
         * List of Index-to-Position mappings. Indices are in the UpdateList, Positions are from the BTreeSetPage.
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
    private interface SerializeValue<T> {
        void accept(ArrayView target, int offset, T value);
    }

    //endregion
}
