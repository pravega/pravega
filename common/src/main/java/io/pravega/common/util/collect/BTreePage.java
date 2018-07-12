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
import java.util.Random;
import javax.annotation.concurrent.NotThreadSafe;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.val;

/**
 * Format: Header|Data|Footer
 * * Header: Version(1)|Flags(1)|Id(4)|Count(4)
 * * Data: List{Key(KL)|Value(VL)}
 * * Footer: Id(4)
 */
@NotThreadSafe
class BTreePage {
    //region Format

    private static final byte CURRENT_VERSION = 0;
    private static final int VERSION_OFFSET = 0;
    private static final int VERSION_LENGTH = 1; // Maximum 256 versions.

    private static final int FLAGS_OFFSET = VERSION_OFFSET + VERSION_LENGTH;
    private static final int FLAGS_LENGTH = 1; // Maximum 8 flags.

    private static final int ID_OFFSET = FLAGS_OFFSET + FLAGS_LENGTH;
    private static final int ID_LENGTH = 4; //

    private static final int COUNT_OFFSET = ID_OFFSET + ID_LENGTH;
    private static final int COUNT_LENGTH = 4; // Allows overflowing, but needed in order to do splits.

    private static final int DATA_OFFSET = COUNT_OFFSET + COUNT_LENGTH; // Also doubles for Header Length.
    private static final int FOOTER_LENGTH = ID_LENGTH;

    private static final byte FLAG_NONE = 0;
    private static final byte FLAG_INDEX_PAGE = 1;

    private static final ByteArrayComparator KEY_COMPARATOR = new ByteArrayComparator();
    private static final Random ID_GENERATOR = new Random();

    //endregion

    //region Members

    @Getter
    private ByteArraySegment contents;
    private ByteArraySegment header;
    @Getter
    private ByteArraySegment data;
    private ByteArraySegment footer;
    @Getter
    private final Config config;

    //endregion

    //region Constructor

    BTreePage(Config config, int count) {
        this(config, new ByteArraySegment(new byte[DATA_OFFSET + count * config.entryLength + FOOTER_LENGTH]), false);
        formatHeaderAndFooter(count, ID_GENERATOR.nextInt());
    }

    BTreePage(Config config, ByteArraySegment contents) {
        this(config, contents, true);
    }

    private BTreePage(Config config, int count, ByteArraySegment data) {
        this(config, new ByteArraySegment(new byte[DATA_OFFSET + data.getLength() + FOOTER_LENGTH]), false);
        Preconditions.checkArgument(count * config.entryLength == data.getLength(), "Unexpected data length given the count.");
        formatHeaderAndFooter(count, ID_GENERATOR.nextInt());
        this.data.copyFrom(data, 0, data.getLength());
    }

    private BTreePage(Config config, ByteArraySegment contents, boolean validate) {
        this.config = Preconditions.checkNotNull(config, "config");
        this.contents = Preconditions.checkNotNull(contents, "contents");
        this.header = contents.subSegment(0, DATA_OFFSET);
        this.data = contents.subSegment(DATA_OFFSET, contents.getLength() - DATA_OFFSET - FOOTER_LENGTH);
        this.footer = contents.subSegment(contents.getLength() - FOOTER_LENGTH, FOOTER_LENGTH);
        if (validate) {
            int headerId = getHeaderId();
            int footerId = BitConverter.readInt(this.footer, 0);
            Preconditions.checkArgument(headerId == footerId, "Invalid Page (id mismatch). HeaderId=%s, FooterId=%s.", headerId, footerId);
        }
    }

    private void formatHeaderAndFooter(int itemCount, int id) {
        // Header.
        this.header.set(VERSION_OFFSET, CURRENT_VERSION);
        this.header.set(FLAGS_OFFSET, getFlags(this.config.isIndexPage ? FLAG_INDEX_PAGE : FLAG_NONE));
        BitConverter.writeInt(this.header, ID_OFFSET, id);
        setCount(itemCount);

        // Matching footer.
        setFooterId(id);
    }

    //endregion

    //region Operations

    static boolean isIndexPage(ByteArraySegment pageContents) {
        int flags = pageContents.get(FLAGS_OFFSET);
        return (flags & FLAG_INDEX_PAGE) == FLAG_INDEX_PAGE;
    }

    void setCount(int itemCount) {
        BitConverter.writeInt(this.header, COUNT_OFFSET, itemCount);
    }

    int getCount() {
        return BitConverter.readInt(this.header, COUNT_OFFSET);
    }

    int getLength(){
        return this.contents.getLength();
    }

    ByteArraySegment getValue(ByteArraySegment key) {
        val pos = search(key, 0);
        if (!pos.isExactMatch()) {
            // Nothing found
            return null;
        }

        return getValueAt(pos.getValue());
    }

    ByteArraySegment getValueAt(int pos) {
        return this.data.subSegment(pos * this.config.entryLength + this.config.keyLength, this.config.valueLength);
    }

    void setValueAt(int pos, ByteArraySegment value) {
        this.data.copyFrom(value, pos * this.config.entryLength + this.config.keyLength, value.getLength());
    }

    ByteArraySegment getKeyAt(int pos) {
        return this.data.subSegment(pos * this.config.entryLength, this.config.keyLength);
    }

    List<BTreePage> splitIfNecessary() {
        if (this.contents.getLength() <= this.config.getMaxPageSize()) {
            // Nothing to do.
            return null;
        }

        val result = new ArrayList<BTreePage>();
        int index = 0;
        int dataLength = this.data.getLength();
        while (index < dataLength) {
            int length = Math.min(this.config.getSplitSize(), dataLength - index);
            assert length % this.config.getEntryLength() == 0 : "entry misaligned";

            // Fetch data and compose new page.
            val splitPageData = this.data.subSegment(index, length);
            val splitPage = new BTreePage(this.config, splitPageData.getLength() / this.config.getEntryLength(), splitPageData);
            result.add(splitPage);
            index += length;
        }

        return result;
    }

    void update(Collection<PageEntry> entries) {
        if (entries.isEmpty()) {
            // Nothing to do.
            return;
        }

        // Keep track of new keys to be added along with the offset (in the original page) where they would have belonged.
        val newEntries = new ArrayList<Map.Entry<Integer, PageEntry>>();

        // Process all the Entries, in order (by Key).
        int lastPos = 0;
        val entryIterator = entries.stream().sorted((e1, e2) -> KEY_COMPARATOR.compare(e1.getKey(), e2.getKey())).iterator();
        while (entryIterator.hasNext()) {
            val e = entryIterator.next();
            val pos = search(e.getKey(), lastPos);
            if (pos.isExactMatch()) {
                // Keys already exists: Update in-place.
                setValueAt(pos.getValue(), e.getValue());
            } else {
                // This entry's key does not exist. We need to remember it for later. Since this was not an exact match,
                // binary search returned the position right before where it should be
                int dataIndex = (pos.getValue() + 1) * this.config.getEntryLength();
                newEntries.add(new AbstractMap.SimpleImmutableEntry<>(dataIndex, e));
            }
        }

        if (newEntries.isEmpty()) {
            // Nothing else to change. We've already updated the keys in-place.
            return;
        }

        int newCount = getCount() + newEntries.size();

        // If we have extra entries: allocate new buffer of the correct size and start copying from the old one.
        val newPage = new BTreePage(this.config, newCount);
        int readIndex = 0;
        int writeIndex = 0;
        for (val e : newEntries) {
            if (e.getKey() > readIndex) {
                // Copy from source
                int length = e.getKey() - readIndex;
                newPage.data.copyFrom(this.data, readIndex, writeIndex, length);
                writeIndex += length;
            }

            // Write Key.
            newPage.data.copyFrom(e.getValue().getKey(), writeIndex, e.getValue().getKey().getLength());
            writeIndex += e.getValue().getKey().getLength();

            // Write Value.
            newPage.data.copyFrom(e.getValue().getValue(), writeIndex, e.getValue().getValue().getLength());
            writeIndex += e.getValue().getValue().getLength();

            readIndex = e.getKey();
        }

        if (readIndex < this.data.getLength()) {
            // Copy the last part that we may have missed.
            int length = this.data.getLength() - readIndex;
            newPage.data.copyFrom(this.data, readIndex, writeIndex, length);
        }

        setFrom(newPage);
    }

    /**
     * Deletes the given Keys from the page.
     *
     * @param keys
     */
    void delete(Collection<byte[]> keys) {
        if (keys.isEmpty()) {
            // Nothing to do.
            return;
        }

        // Process all Keys, in order, record the position of any matches.
        int lastPos = 0;
        int initialCount = getCount();
        val removedPositions = new ArrayList<Integer>();
        val keyIterator = keys.stream().sorted(KEY_COMPARATOR).iterator();
        while (keyIterator.hasNext() && removedPositions.size() < initialCount) {
            val key = new ByteArraySegment(keyIterator.next());
            val pos = search(key, lastPos);
            if (!pos.exactMatch) {
                // Key does not exist.
                continue;
            }

            removedPositions.add(pos.getValue());
            lastPos = pos.getValue() + 1;
        }

        if (removedPositions.size() > 0) {
            // Remember the new count now, before we mess around with things.
            int newCount = initialCount - removedPositions.size();

            // Trim away the data buffer, move the footer back and trim the contents buffer.
            int writePos = removedPositions.get(0);
            removedPositions.add(initialCount); // Add a sentinel at the end to make this easier.
            for (int i = 1; i < removedPositions.size(); i++) {
                int removedPos = removedPositions.get(i);
                int prevRemovedPos = removedPositions.get(i - 1);
                int copyStartIndex = (prevRemovedPos + 1) * this.config.entryLength;
                int copyLength = removedPos * this.config.entryLength - copyStartIndex;
                if (copyLength == 0) {
                    // Nothing to do now.
                    continue;
                }

                // Copy the data.
                this.data.copyFrom(this.data, copyStartIndex, writePos, copyLength);
                writePos += copyLength;
            }

            assert writePos == (initialCount - removedPositions.size() - 1) * this.config.entryLength : "unexpected number of bytes remaining";
            resize(newCount);
        }
    }

    SearchResult search(ByteArraySegment key, int startPos) {
        // Positions here are not indices into "source", rather they are entry positions, which is why we always need
        // to adjust by using entryLength.
        int endPos = this.data.getLength() / this.config.entryLength;
        Preconditions.checkArgument(startPos <= endPos, "Cannot perform binary search because startPos (%s) is greater than endPos(%s).", startPos, endPos);
        while (startPos < endPos) {
            // Locate the Key in the middle.
            int midPos = startPos + (endPos - startPos) / 2;
            int c = KEY_COMPARATOR.compare(key.array(), key.arrayOffset(),
                    this.data.array(), this.data.arrayOffset() + startPos * this.config.entryLength, this.config.keyLength);
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

        // Return the index that contains the highest value that's smaller than the sought key.
        return new SearchResult(startPos, false);
    }

    private void setFrom(BTreePage toSet) {
        this.data = toSet.data;
        this.contents = toSet.contents;
        this.footer = toSet.footer;
    }

    private void resize(int itemCount) {
        assert itemCount <= getCount() : "cannot resize up";
        int dataLength = itemCount * this.config.entryLength;
        this.data = new ByteArraySegment(this.contents.array(), this.data.arrayOffset(), dataLength);
        this.footer = new ByteArraySegment(this.contents.array(), this.data.arrayOffset() + this.data.getLength(), FOOTER_LENGTH);
        this.contents = new ByteArraySegment(this.contents.array(), this.contents.arrayOffset(), this.footer.arrayOffset() + this.footer.getLength());
        setCount(itemCount);
        setFooterId(getHeaderId());
    }

    private byte getFlags(byte... flags) {
        byte result = 0;
        for (byte f : flags) {
            result |= f;
        }
        return result;
    }

    private int getHeaderId() {
        return BitConverter.readInt(this.header, ID_OFFSET);
    }

    private void setFooterId(int id) {
        BitConverter.writeInt(this.footer, 0, id);
    }

    //endregion

    //region Config

    @RequiredArgsConstructor
    @Getter
    static class Config {
        private final int keyLength;
        private final int valueLength;
        private final int entryLength;
        private final int maxPageSize;
        private final int splitSize;
        private final boolean isIndexPage;

        Config(int keyLength, int valueLength, int maxPageSize, boolean isIndexPage) {
            Preconditions.checkArgument(keyLength > 0, "keyLength must be a positive integer.");
            Preconditions.checkArgument(valueLength > 0, "valueLength must be a positive integer.");
            Preconditions.checkArgument(keyLength + valueLength + DATA_OFFSET + FOOTER_LENGTH >= maxPageSize,
                    "maxPageSize must be able to fit at least one entry.");
            this.keyLength = keyLength;
            this.valueLength = valueLength;
            this.entryLength = this.keyLength + this.valueLength;
            this.maxPageSize = maxPageSize;
            this.isIndexPage = isIndexPage;
            this.splitSize = calculateSplitSize();
        }

        private int calculateSplitSize() {
            // Calculate the maximum number of elements that fit in a full page.
            int maxCount = (this.maxPageSize - DATA_OFFSET) / this.entryLength;

            // A Split page will have about half the original number of elements.
            return (maxCount / 2 + 1) * this.entryLength;
        }
    }

    //endregion

    //region SearchResult

    @Getter
    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    static class SearchResult {
        private final int value;
        private final boolean exactMatch;
    }

    //endregion
}
