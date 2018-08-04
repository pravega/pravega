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
 * A Page (Node) within a B+Tree Index.
 *
 * Format: Header|Data|Footer
 * * Header: Version(1)|Flags(1)|Id(4)|Count(4)
 * * Data: List{Key(KL)|Value(VL)}
 * * Footer: Id(4)
 */
@NotThreadSafe
class BTreePage {
    //region Format

    /**
     * Format Version related fields.
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
    private static final byte FLAG_INDEX_PAGE = 1;

    /**
     * Page Id.
     */
    private static final int ID_OFFSET = FLAGS_OFFSET + FLAGS_LENGTH;
    private static final int ID_LENGTH = 4; //

    /**
     * Element Count.
     */
    private static final int COUNT_OFFSET = ID_OFFSET + ID_LENGTH;
    private static final int COUNT_LENGTH = 4; // Allows overflowing, but needed in order to do splits.

    /**
     * Data (contents).
     */
    private static final int DATA_OFFSET = COUNT_OFFSET + COUNT_LENGTH; // Also doubles for Header Length.
    private static final int FOOTER_LENGTH = ID_LENGTH;

    //endregion

    //region Members

    private static final ByteArrayComparator KEY_COMPARATOR = new ByteArrayComparator();
    private static final Random ID_GENERATOR = new Random();

    /**
     * The entire ByteArraySegment that makes up this BTreePage. This includes header, data and footer.
     */
    @Getter
    private ByteArraySegment contents;
    private ByteArraySegment header;
    private ByteArraySegment data;
    private ByteArraySegment footer;
    private final Config config;

    //endregion

    //region Constructor

    /**
     * Creates a new empty instance of the BTreePage class with given configuration.
     *
     * @param config BTreePage Configuration.
     */
    BTreePage(Config config) {
        this(config, new ByteArraySegment(new byte[DATA_OFFSET + FOOTER_LENGTH]), false);
        formatHeaderAndFooter(0, ID_GENERATOR.nextInt());
    }

    /**
     * Creates a new instance of the BTreePage class from the given ByteArraySegment.
     *
     * @param config   BTreePage Configuration.
     * @param contents The ByteArraySegment to wrap. This may be modified based on changes applied to this BTreePage.
     */
    BTreePage(Config config, ByteArraySegment contents) {
        this(config, contents, true);
    }

    /**
     * Creates a new instance of the BTreePage class using the given Configuration, existing data and count.
     *
     * @param config BTreePage Configuration.
     * @param count  Number of elements in data.
     * @param data   Source Element data. This will be copied into a new buffer and will not be modified based on changes
     *               applied to this BTreePage.
     */
    private BTreePage(Config config, int count, ByteArraySegment data) {
        this(config, new ByteArraySegment(new byte[DATA_OFFSET + data.getLength() + FOOTER_LENGTH]), false);
        Preconditions.checkArgument(count * config.entryLength == data.getLength(), "Unexpected data length given the count.");
        formatHeaderAndFooter(count, ID_GENERATOR.nextInt());
        this.data.copyFrom(data, 0, data.getLength());
    }

    /**
     * Creates a new instance of the BTreePage class using the given Configuration and existing Contents.
     *
     * @param config   BTreePage Configuration.
     * @param contents A ByteArraySegment to wrap/parse. This may be modified based on changes applied to this BTreePage.
     * @param validate If true, the contents will be validated for consistency.
     */
    private BTreePage(Config config, ByteArraySegment contents, boolean validate) {
        this.config = Preconditions.checkNotNull(config, "config");
        this.contents = Preconditions.checkNotNull(contents, "contents");
        this.header = contents.subSegment(0, DATA_OFFSET);
        this.data = contents.subSegment(DATA_OFFSET, contents.getLength() - DATA_OFFSET - FOOTER_LENGTH);
        this.footer = contents.subSegment(contents.getLength() - FOOTER_LENGTH, FOOTER_LENGTH);
        if (validate) {
            int headerId = getHeaderId();
            int footerId = getFooterId();
            Preconditions.checkArgument(headerId == footerId, "Invalid Page (id mismatch). HeaderId=%s, FooterId=%s.", headerId, footerId);
        }
    }

    /**
     * Formats the Header and Footer of this BTreePage with the given information.
     *
     * @param itemCount The number of items in this BTreePage.
     * @param id        The Id of this BTreePage.
     */
    private void formatHeaderAndFooter(int itemCount, int id) {
        Preconditions.checkArgument(itemCount >= 0, "Count must be a non-negative integer.");

        this.header.set(VERSION_OFFSET, CURRENT_VERSION);
        this.header.set(FLAGS_OFFSET, getFlags(this.config.isIndexPage ? FLAG_INDEX_PAGE : FLAG_NONE));
        setCount(itemCount);
        setHeaderId(id);
        setFooterId(id);
    }

    //endregion

    //region Operations

    /**
     * Determines whether the given ByteArraySegment is an Index Page or not.
     *
     * @param pageContents A ByteArraySegment representing a BTreePage.
     * @return True if pageContents is an Index Page, false otherwise.
     */
    static boolean isIndexPage(ByteArraySegment pageContents) {
        return (pageContents.get(FLAGS_OFFSET) & FLAG_INDEX_PAGE) == FLAG_INDEX_PAGE;
    }

    /**
     * Updates the header of this BTreePage to reflect the given item count. This does not perform any resizing.
     *
     * @param itemCount The new item count.
     */
    void setCount(int itemCount) {
        Preconditions.checkArgument(itemCount >= 0, "itemCount must be a non-negative number.");
        BitConverter.writeInt(this.header, COUNT_OFFSET, itemCount);
    }

    /**
     * Gets a value indicating the number of items in this BTreePage, as reflected in its header.
     *
     * @return The number of items on this BTreePage.
     */
    int getCount() {
        return BitConverter.readInt(this.header, COUNT_OFFSET);
    }

    /**
     * Gets a value indicating the total number of bytes of this BTreePage (including header, data and footer).
     *
     * @return The length of this BTreePage.
     */
    int getLength(){
        return this.contents.getLength();
    }

    /**
     * Gets a ByteArraySegment representing the Value at the given position (not including the Key).
     *
     * @param pos The position to get the value at.
     * @return A ByteArraySegment.
     */
    ByteArraySegment getValueAt(int pos) {
        // Only validate lower bound; upper bound is checked implicitly when calling subSegment().
        Preconditions.checkArgument(pos >= 0, "pos must be a non-negative number");
        return this.data.subSegment(pos * this.config.entryLength + this.config.keyLength, this.config.valueLength);
    }

    /**
     * Updates the value at the given position.
     *
     * @param pos   The position to set the value at.
     * @param value A ByteArraySegment representing the value.
     */
    private void setValueAt(int pos, ByteArraySegment value) {
        // Only validate lower bound; upper bound is checked implicitly when calling subSegment().
        Preconditions.checkArgument(pos >= 0, "pos must be a non-negative number");
        Preconditions.checkArgument(value.getLength() == this.config.getValueLength(), "Unexpected value length.");
        this.data.copyFrom(value, pos * this.config.entryLength + this.config.keyLength, value.getLength());
    }

    /**
     * Gets a ByteArraySegment representing the Key at the given position (not including the Value).
     *
     * @param pos The position to get the Key at.
     * @return A ByteArraySegment.
     */
    ByteArraySegment getKeyAt(int pos) {
        // Only validate lower bound; upper bound is checked implicitly when calling subSegment().
        Preconditions.checkArgument(pos >= 0, "pos must be a non-negative number");
        return this.data.subSegment(pos * this.config.entryLength, this.config.keyLength);
    }

    /**
     * Splits this BTreePage into two or more BTreePages, if necessary. No changes are made to this BTreePage. A split is
     * needed only if this BTreePage's length exceeds the maximum page size, as defined in the Configuration.
     *
     * @return An ordered list of BTreePages containing about the same number of items, in the same order as in this BTreePage.
     * If no split is necessary, returns null.
     */
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

    /**
     * Updates the contents of this BTreePage to also include the given entries. Any existing keys will be overridden with
     * the new entry values.
     *
     * Note: this may cause the BTreePage to overflow (exceed maximum page size). It will not auto-split if that happens.
     *
     * @param entries A Collection of ArrayTuples, where Left is Key, and Right is Value.
     */
    void update(Collection<ArrayTuple> entries) {
        if (entries.isEmpty()) {
            // Nothing to do.
            return;
        }

        // Keep track of new keys to be added along with the offset (in the original page) where they would have belonged.
        val newEntries = new ArrayList<Map.Entry<Integer, ArrayTuple>>();

        // Process all the Entries, in order (by Key).
        int lastPos = 0;
        val entryIterator = entries.stream().sorted((e1, e2) -> KEY_COMPARATOR.compare(e1.getLeft(), e2.getLeft())).iterator();
        while (entryIterator.hasNext()) {
            val e = entryIterator.next();
            Preconditions.checkArgument(e.getLeft().getLength() == this.config.getKeyLength() && e.getRight().getLength() == this.config.getValueLength(),
                    "Found an entry with unexpected Key or Value length.");

            val sr = search(e.getLeft(), lastPos);
            if (sr.isExactMatch()) {
                // Key already exists; update in-place.
                setValueAt(sr.getPosition(), e.getRight());
            } else {
                // This entry's key does not exist. We need to remember it for later. Since this was not an exact match,
                // binary search returned the position where it should have been.
                int dataIndex = sr.getPosition() * this.config.getEntryLength();
                newEntries.add(new AbstractMap.SimpleImmutableEntry<>(dataIndex, e));
            }
        }

        if (newEntries.isEmpty()) {
            // Nothing else to change. We've already updated the keys in-place.
            return;
        }

        int newCount = getCount() + newEntries.size();

        // If we have extra entries: allocate new buffer of the correct size and start copying from the old one.
        val newPage = new BTreePage(this.config, new ByteArraySegment(new byte[DATA_OFFSET + newCount * this.config.entryLength + FOOTER_LENGTH]), false);
        newPage.formatHeaderAndFooter(newCount, ID_GENERATOR.nextInt());
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
            newPage.data.copyFrom(e.getValue().getLeft(), writeIndex, e.getValue().getLeft().getLength());
            writeIndex += e.getValue().getLeft().getLength();

            // Write Value.
            newPage.data.copyFrom(e.getValue().getRight(), writeIndex, e.getValue().getRight().getLength());
            writeIndex += e.getValue().getRight().getLength();

            readIndex = e.getKey();
        }

        if (readIndex < this.data.getLength()) {
            // Copy the last part that we may have missed.
            int length = this.data.getLength() - readIndex;
            newPage.data.copyFrom(this.data, readIndex, writeIndex, length);
        }

        // Re-point to use the new page's elements.
        this.header = newPage.header;
        this.data = newPage.data;
        this.contents = newPage.contents;
        this.footer = newPage.footer;
    }

    /**
     * Deletes the given Keys from the BTreePage.
     *
     * @param keys A Collection of Keys to delete.
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
            Preconditions.checkArgument(key.getLength() == this.config.getKeyLength(), "Found a key with unexpected length.");
            val sr = search(key, lastPos);
            if (!sr.exactMatch) {
                // Key does not exist.
                continue;
            }

            removedPositions.add(sr.getPosition());
            lastPos = sr.getPosition() + 1;
        }

        if (removedPositions.size() > 0) {
            // Remember the new count now, before we mess around with things.
            int newCount = initialCount - removedPositions.size();

            // Trim away the data buffer, move the footer back and trim the contents buffer.
            int writeIndex = removedPositions.get(0) * this.config.entryLength;
            removedPositions.add(initialCount); // Add a sentinel at the end to make this easier.
            for (int i = 1; i < removedPositions.size(); i++) {
                int removedPos = removedPositions.get(i);
                int prevRemovedPos = removedPositions.get(i - 1);
                int readIndex = (prevRemovedPos + 1) * this.config.entryLength;
                int readLength = removedPos * this.config.entryLength - readIndex;
                if (readLength == 0) {
                    // Nothing to do now.
                    continue;
                }

                // Copy the data.
                this.data.copyFrom(this.data, readIndex, writeIndex, readLength);
                writeIndex += readLength;
            }

            assert writeIndex == (initialCount - removedPositions.size() + 1) * this.config.entryLength : "unexpected number of bytes remaining";
            shrink(newCount);
        }
    }

    /**
     * Searches for the given Key in this BTreePage.
     *
     * @param key The Key to search for.
     * @return A SearchResult for the lookup.
     */
    SearchResult search(byte[] key) {
        return search(new ByteArraySegment(key), 0);
    }

    /**
     * Searches for the given Key in this BTreePage, starting at the given position.
     *
     * @param key      The Key to search for.
     * @param startPos The starting position.
     * @return A SearchResult for the lookup.
     */
    SearchResult search(ByteArraySegment key, int startPos) {
        // Positions here are not indices into "source", rather they are entry positions, which is why we always need
        // to adjust by using entryLength.
        int endPos = this.data.getLength() / this.config.entryLength;
        Preconditions.checkArgument(startPos >= 0 && startPos <= endPos, "startPos (%s) is greater than the number of items in this page (%s).", startPos, endPos);
        while (startPos < endPos) {
            // Locate the Key in the middle.
            int midPos = startPos + (endPos - startPos) / 2;
            int c = KEY_COMPARATOR.compare(key.array(), key.arrayOffset(),
                    this.data.array(), this.data.arrayOffset() + midPos * this.config.entryLength, this.config.keyLength);
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

    /**
     * Shrinks this BTreePage to only contain the given number of elements.
     *
     * @param itemCount The new number of items in this BTreePage.
     */
    private void shrink(int itemCount) {
        Preconditions.checkArgument(itemCount >= 0 && itemCount <= getCount(), "itemCount must be non-negative and at most the current element count");
        int dataLength = itemCount * this.config.entryLength;
        this.data = new ByteArraySegment(this.contents.array(), this.data.arrayOffset(), dataLength);
        this.footer = new ByteArraySegment(this.contents.array(), this.data.arrayOffset() + this.data.getLength(), FOOTER_LENGTH);
        this.contents = new ByteArraySegment(this.contents.array(), this.contents.arrayOffset(), this.footer.arrayOffset() + this.footer.getLength());
        setCount(itemCount);
        setFooterId(getHeaderId());
    }

    /**
     * Combines the given flags into a single value.
     */
    private byte getFlags(byte... flags) {
        byte result = 0;
        for (byte f : flags) {
            result |= f;
        }
        return result;
    }

    /**
     * Gets this BTreePage's Id from its header.
     */
    private int getHeaderId() {
        return BitConverter.readInt(this.header, ID_OFFSET);
    }

    /**
     * Gets this BTreePage's Id from its footer.
     */
    private int getFooterId() {
        return BitConverter.readInt(this.footer, 0);
    }

    /**
     * Updates the Header to contain the given id.
     */
    private void setHeaderId(int id) {
        BitConverter.writeInt(this.header, ID_OFFSET, id);
    }

    /**
     * Updates the Footer to contain the given id.
     */
    private void setFooterId(int id) {
        BitConverter.writeInt(this.footer, 0, id);
    }

    //endregion

    //region Config

    /**
     * BTreePage Configuration.
     */

    @Getter
    static class Config {
        /**
         * The length, in bytes, for all Keys.
         */
        private final int keyLength;
        /**
         * The length, in bytes, for all Values.
         */
        private final int valueLength;
        /**
         * The length, in bytes, for all Entries (KeyLength + ValueLength).
         */
        private final int entryLength;
        /**
         * Maximum length, in bytes, of any BTreePage (including header, data and footer).
         */
        private final int maxPageSize;
        /**
         * The maximum length, in bytes, of a BTreePage that is the result of a split.
         */
        private final int splitSize;
        /**
         * Whether this is an Index Page or not.
         */
        private final boolean isIndexPage;

        /**
         * Creates a new instance of the BTreePage.Config class.
         *
         * @param keyLength   The length, in bytes, of all Keys.
         * @param valueLength The length, in bytes, of all Values.
         * @param maxPageSize Maximum length, in bytes, of any BTreePage.
         * @param isIndexPage Whether this is an Index Page or not.
         */
        Config(int keyLength, int valueLength, int maxPageSize, boolean isIndexPage) {
            Preconditions.checkArgument(keyLength > 0, "keyLength must be a positive integer.");
            Preconditions.checkArgument(valueLength > 0, "valueLength must be a positive integer.");
            Preconditions.checkArgument(keyLength + valueLength + DATA_OFFSET + FOOTER_LENGTH <= maxPageSize,
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

    /**
     * The result of a Search.
     */
    @Getter
    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    static class SearchResult {
        /**
         * The position this SearchResult points to.
         */
        final int position;

        /**
         * If true, an exact match was found, and getPosition() points to it.
         * If false, no exact match was found, and getPosition() points to the location where it should have been.
         */
        final boolean exactMatch;
    }

    //endregion
}
