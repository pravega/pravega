/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.util.collect;

import com.google.common.base.Preconditions;
import io.pravega.common.util.BitConverter;
import io.pravega.common.util.ByteArraySegment;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.Data;
import lombok.Getter;
import lombok.val;

/**
 * Layout: Version(1)|Flags(1)|List{Key(KL)|Value(VL)}
 */
abstract class BTreePageLayout {
    //region Layout Config

    private static final byte CURRENT_VERSION = 0;
    private static final int VERSION_OFFSET = 0;
    private static final int VERSION_LENGTH = 1; // Maximum 256 versions.
    private static final int FLAGS_OFFSET = VERSION_OFFSET + VERSION_LENGTH;
    private static final int FLAGS_LENGTH = 1; // Maximum 8 flags.
    private static final int COUNT_OFFSET = FLAGS_OFFSET + FLAGS_LENGTH;
    private static final int COUNT_LENGTH = 2; // Maximum 65536 items per page.
    private static final int DATA_OFFSET = COUNT_OFFSET + COUNT_LENGTH;
    private static final byte FLAG_NONE = 0;
    private static final byte FLAG_INDEX_PAGE = 1;
    private static final ByteArrayComparator KEY_COMPARATOR = new ByteArrayComparator();

    //endregion

    //region Members

    @Getter
    private final int keyLength;
    @Getter
    private final int valueLength;
    @Getter
    private final int entryLength;
    @Getter
    private final int maxPageSize;
    private final int splitSize;

    //endregion

    //region Constructor

    BTreePageLayout(int keyLength, int valueLength, int maxPageSize) {
        this.keyLength = keyLength;
        this.valueLength = valueLength;
        this.entryLength = this.keyLength + this.valueLength;
        this.maxPageSize = maxPageSize;
        this.splitSize = calculateSplitSize();
    }

    private int calculateSplitSize() {
        // Calculate the maximum number of elements that fit in a full page.
        int maxCount = (this.maxPageSize - DATA_OFFSET) / this.entryLength;

        // A Split page will have about half the original number of elements.
        return (maxCount / 2 + 1) * this.entryLength;
    }

    //endregion

    boolean mustSplit(ByteArraySegment pageContents) {
        // True if exceeding capacity, false otherwise.
        return pageContents.getLength() > this.maxPageSize;
    }

    List<ArrayTuple> split(ByteArraySegment pageContents) {
        val result = new ArrayList<ArrayTuple>();
        Preconditions.checkArgument(mustSplit(pageContents), "Given argument does not require splitting.");

        int index = 0;
        val sourceData = getData(pageContents);
        while (index < sourceData.getLength()) {
            int length = Math.min(this.splitSize, sourceData.getLength() - index);
            assert length % this.entryLength == 0 : "entry misaligned";

            // Fetch data.
            val splitPageData = sourceData.subSegment(index, splitSize);

            // Extract key.
            val splitKey = getKeyAt(0, splitPageData);

            // Compose new page.
            val splitPage = new ByteArraySegment(new byte[DATA_OFFSET + splitPageData.getLength()]);
            writeHeader(splitPage, (short) (length / this.entryLength));
            result.add(new ArrayTuple(splitKey, splitPage));
            index += this.splitSize;
        }

        return result;
    }

    boolean isIndexPage(ByteArraySegment pageContents) {
        int flags = pageContents.get(FLAGS_OFFSET);
        return (flags & FLAG_INDEX_PAGE) == FLAG_INDEX_PAGE;
    }

    ByteArraySegment createEmptyRoot() {
        ByteArraySegment result = new ByteArraySegment(new byte[DATA_OFFSET]);
        writeHeader(result, (short) 0);
        // TODO: do we need to insert dummy entry???
        return result;
    }

    ByteArraySegment update(Collection<ArrayTuple> entries, ByteArraySegment pageContents) {
        // entries need not be sorted
        if (entries.isEmpty()) {
            // Nothing to do.
            return pageContents;
        }

        // Keep track of new keys to be added along with the offset (in the original page) where they would have belonged.
        val newEntries = new ArrayList<Map.Entry<Integer, ArrayTuple>>();

        // Process all the Entries, in order (by Key).
        int lastPos = 0;
        val entryIterator = entries.stream().sorted((e1, e2) -> KEY_COMPARATOR.compare(e1.getLeft(), e2.getLeft())).iterator();
        ByteArraySegment sourceData = getData(pageContents);
        while (entryIterator.hasNext()) {
            val e = entryIterator.next();
            val pos = binarySearch(sourceData, e.getLeft(), lastPos);
            if (pos.exactMatch) {
                // Keys already exists: Update in-place.
                setValueAt(pos.value, sourceData, e.getRight());
            } else {
                // This entry's key does not exist. We need to remember it for later. Since this was not an exact match,
                // binary search returned the position right before where it should be
                int dataIndex = (pos.value + 1) * this.entryLength;
                newEntries.add(new AbstractMap.SimpleImmutableEntry<>(dataIndex, e));
            }
        }

        if (newEntries.isEmpty()) {
            // Nothing else to change. We've already updated the keys in-place.
            return pageContents;
        }

        int newCount = getCount(pageContents) + newEntries.size();
        Preconditions.checkArgument(newCount < Short.MAX_VALUE, "Too many entries.");

        // If we have extra entries: allocate new buffer of the correct size and start copying from the old one.
        val result = new ByteArraySegment(new byte[pageContents.getLength() + newEntries.size() * this.entryLength]);
        writeHeader(result, (short) newCount);
        val newData = getData(result);
        int readIndex = 0;
        int writeIndex = 0;
        for (val e : newEntries) {
            if (e.getKey() > readIndex) {
                // Copy from source
                int length = e.getKey() - readIndex;
                newData.copyFrom(sourceData, writeIndex, length);
                writeIndex += length;
            }

            // Write Key.
            newData.copyFrom(e.getValue().getLeft(), writeIndex, e.getValue().getLeft().getLength());
            writeIndex += e.getValue().getLeft().getLength();

            // Write Value.
            newData.copyFrom(e.getValue().getRight(), writeIndex, e.getValue().getRight().getLength());
            writeIndex += e.getValue().getRight().getLength();

            readIndex = e.getKey();
        }

        if (readIndex < sourceData.getLength()) {
            // Copy the last part that we may have missed.
            int length = sourceData.getLength() - readIndex;
            newData.copyFrom(sourceData, writeIndex, length);
        }

        return result;
    }

    /**
     * Deletes the given Keys from the given page contents.
     *
     * @param keys
     * @param pageContents
     * @return
     */
    ByteArraySegment delete(Collection<byte[]> keys, ByteArraySegment pageContents) {
        if (keys.isEmpty()) {
            // Nothing to do.
            return pageContents;
        }

        // Keep track of an ordered list of Sub-Segments that will survive. These will be obtained by cutting away
        // the ranges corresponding to the deleted keys.
        val remainingSegments = new ArrayList<ByteArraySegment>();

        // The number of bytes remaining.
        int remainingLength = 0;

        // The number of items remaining.
        short remainingCount = getCount(pageContents);

        // Process all Keys, in order, and for any match, cut away the corresponding byte range from the original
        // page contents by keeping track of the remaining ranges.
        int lastPos = 0;
        int lastIndex = 0;
        val keyIterator = keys.stream().sorted(KEY_COMPARATOR).iterator();
        ByteArraySegment data = getData(pageContents);
        while (keyIterator.hasNext() && remainingCount > 0) {
            val key = new ByteArraySegment(keyIterator.next());
            val pos = binarySearch(data, key, lastPos);
            if (!pos.exactMatch) {
                // Key does not exist.
                continue;
            }

            int dataIndex = pos.value * this.entryLength;
            if (pos.value > lastPos) {
                // Record the sub-segment between the last deleted key and this one.
                remainingSegments.add(data.subSegment(lastIndex, dataIndex - lastIndex));
                remainingLength += this.entryLength;
            }

            // Remember where we left off.
            lastPos = pos.value + 1;
            lastIndex = dataIndex;
            remainingCount--;
        }

        if (lastIndex < data.getLength()) {
            // Don't forget to add the last sub-segment.
            remainingSegments.add(data.subSegment(lastIndex, data.getLength() - lastIndex));
            remainingLength += this.entryLength;
        }

        if (remainingLength == data.getLength()) {
            // No change.
            return pageContents;
        } else {
            // Create a new buffer with only the remaining items.
            ByteArraySegment result = new ByteArraySegment(new byte[DATA_OFFSET + remainingLength]);
            writeHeader(result, remainingCount);
            int copyOffset = DATA_OFFSET;
            for (ByteArraySegment toCopy : remainingSegments) {
                result.copyFrom(toCopy, copyOffset, toCopy.getLength());
                copyOffset += toCopy.getLength();
            }

            return result;
        }
    }

    protected abstract boolean isIndexLayout();

    protected short getCount(ByteArraySegment pageContents) {
        // TODO: let's make this int. It will be much more flexible in the future.
        return BitConverter.readShort(pageContents, COUNT_OFFSET);
    }

    protected void writeHeader(ByteArraySegment pageContents, short itemCount) {
        pageContents.set(VERSION_OFFSET, CURRENT_VERSION);
        pageContents.set(FLAGS_OFFSET, getFlags(isIndexLayout() ? FLAG_INDEX_PAGE : FLAG_NONE));
        BitConverter.writeShort(pageContents, COUNT_OFFSET, itemCount);
    }

    protected byte getFlags(byte... flags) {
        byte result = 0;
        for (byte f : flags) {
            result |= f;
        }
        return result;
    }

    protected ByteArraySegment getData(ByteArraySegment pageContents) {
        return pageContents.subSegment(DATA_OFFSET, pageContents.getLength() - DATA_OFFSET);
    }

    protected ByteArraySegment getValueAt(int pos, ByteArraySegment source) {
        return source.subSegment(pos * this.entryLength + this.keyLength, this.valueLength);
    }

    private void setValueAt(int pos, ByteArraySegment source, ByteArraySegment value) {
        source.copyFrom(value, pos * this.entryLength + this.keyLength, value.getLength());
    }

    protected ByteArraySegment getKeyAt(int pos, ByteArraySegment source) {
        return source.subSegment(pos * this.entryLength, this.keyLength);
    }

    protected BinarySearchResult binarySearch(ByteArraySegment source, byte[] key) {
        return binarySearch(source, new ByteArraySegment(key), 0);
    }

    protected BinarySearchResult binarySearch(ByteArraySegment source, ByteArraySegment key, int startPos) {
        // Positions here are not indices into "source", rather they are entry positions, which is why we always need
        // to adjust by using entryLength.
        int endPos = source.getLength() / this.entryLength;
        Preconditions.checkArgument(startPos <= endPos, "Cannot perform binary search because startPos (%s) is greater than endPos(%s).", startPos, endPos);
        while (startPos < endPos) {
            // Locate the Key in the middle.
            int midPos = startPos + (endPos - startPos) / 2;
            int c = KEY_COMPARATOR.compare(key.array(), key.arrayOffset(), source.array(), source.arrayOffset() + startPos * this.entryLength, this.keyLength);
            if (c == 0) {
                // Exact match.
                return new BinarySearchResult(midPos, true);
            } else if (c < 0) {
                // Search again to the left.
                endPos = midPos;
            } else {
                // Search again to the right.
                startPos = midPos + 1;
            }
        }

        // Return the index that contains the highest value that's smaller than the sought key.
        return new BinarySearchResult(startPos, false);
    }

    //region Index Layout

    static class Index extends BTreePageLayout {
        private static final int VALUE_LENGTH = Long.BYTES + Integer.BYTES;

        Index(int keyLength, int maxPageSize) {
            super(keyLength, VALUE_LENGTH, maxPageSize);
        }

        BTreePagePointer getPagePointer(byte[] key, ByteArraySegment pageContents) {
            ByteArraySegment data = getData(pageContents);
            val pos = binarySearch(data, key);
            assert pos.value >= 0;

            ByteArraySegment ptr = getValueAt(pos.value, data);
            long pageOffset = BitConverter.readLong(ptr, 0);
            int pageLength = BitConverter.readInt(ptr, Long.BYTES);
            return new BTreePagePointer(getKeyAt(pos.value, data), pageOffset, pageLength);
        }

        ByteArraySegment serializePointer(BTreePagePointer pointer) {
            ByteArraySegment result = new ByteArraySegment(new byte[Long.BYTES + Integer.BYTES]);
            BitConverter.writeLong(result, 0, pointer.getOffset());
            BitConverter.writeInt(result, Long.BYTES, pointer.getLength());
            return result;
        }

        ByteArraySegment updatePointers(Collection<BTreePagePointer> pointers, ByteArraySegment pageContents) {
            val toUpdate = pointers.stream()
                    .map(p -> new ArrayTuple(p.getKey(), serializePointer(p)))
                                   .collect(Collectors.toList());
            return update(toUpdate, pageContents);
        }

        @Override
        protected boolean isIndexLayout() {
            return true;
        }
    }

    //endregion

    //region Leaf Layout

    static class Leaf extends BTreePageLayout {
        Leaf(int keyLength, int valueLength, int maxPageSize) {
            super(keyLength, valueLength, maxPageSize);
        }

        @Override
        protected boolean isIndexLayout() {
            return false;
        }

        ByteArraySegment getValue(byte[] key, ByteArraySegment pageContents) {
            ByteArraySegment data = getData(pageContents);
            val pos = binarySearch(data, key);
            if (!pos.exactMatch) {
                // Nothing found
                return null;
            }

            return getValueAt(pos.value, data);
        }
    }

    //endregion

    @Data
    private static class BinarySearchResult {
        final int value;
        final boolean exactMatch;
    }
}

