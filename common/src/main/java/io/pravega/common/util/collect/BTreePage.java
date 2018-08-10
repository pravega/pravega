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
import io.pravega.common.util.IllegalDataFormatException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Random;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.val;

/**
 * B+Tree Page containing raw data. Wraps around a ByteArraySegment and formats it using a special layout.
 *
 * Format: Header|Data|Footer
 * * Header: FormatVersion(1)|Flags(1)|Id(4)|Count(4)
 * * Data: List{Key(KL)|Value(VL)}
 * * Footer: Id(4)
 *
 * The Header contains:
 * * The current format version
 * * A set of flags that apply to this Page. Currently the only one used is to identify if it is an Index or Leaf page.
 * * A randomly generated Page Identifier.
 * * The number of items in the Page.
 *
 * The Data contains:
 * * A list of Keys and Values, sorted by Key (using ByteArrayComparator). The length of this list is defined in the Header.
 *
 * The Footer contains:
 * * The same Page Identifier as in the Header. When wrapping an existing ByteArraySegment, this value is matched to the
 * one in the Header to ensure the Page was loaded correctly.
 *
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
    @Getter
    private final Config config;
    private int count;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of an empty BTreePage class..
     *
     * @param config Page Configuration.
     */
    BTreePage(@Nonnull Config config) {
        this(config, new ByteArraySegment(new byte[DATA_OFFSET + FOOTER_LENGTH]), false);
        formatHeaderAndFooter(this.count, ID_GENERATOR.nextInt());
    }

    /**
     * Creates a new instance of the BTreePage class wrapping an existing ByteArraySegment.
     *
     * @param config   Page Configuration.
     * @param contents The ByteArraySegment to wrap. Changes to this BTreePage may change the values in the array backing
     *                 this ByteArraySegment.
     * @throws IllegalDataFormatException If the given contents is not a valid BTreePage format.
     */
    BTreePage(@Nonnull Config config, @Nonnull ByteArraySegment contents) {
        this(config, contents, true);
    }

    /**
     * Creates a new instance of the BTreePage class wrapping the given Data Items (no header or footer).
     *
     * @param config Page Configuration.
     * @param count  Number of items in data.
     * @param data   A ByteArraySegment containing a list of Key-Value pairs to include. The contents of this ByteArraySegment
     *               will be copied into a new buffer, so changes to this BTreePage will not affect it.
     */
    private BTreePage(@Nonnull Config config, int count, @Nonnull ByteArraySegment data) {
        this(config, new ByteArraySegment(new byte[DATA_OFFSET + data.getLength() + FOOTER_LENGTH]), false);
        Preconditions.checkArgument(count * config.entryLength == data.getLength(), "Unexpected data length given the count.");
        formatHeaderAndFooter(count, ID_GENERATOR.nextInt());
        this.data.copyFrom(data, 0, data.getLength());
    }

    /**
     * Creates a new instance of the BTreePage class wrapping an existing ByteArraySegment.
     *
     * @param config   Page Configuration.
     * @param contents The ByteArraySegment to wrap. Changes to this BTreePage may change the values in the array backing
     *                 this ByteArraySegment.
     * @param validate If true, will perform validation.
     * @throws IllegalDataFormatException If the given contents is not a valid BTreePage format and validate == true.
     */
    private BTreePage(@Nonnull Config config, @Nonnull ByteArraySegment contents, boolean validate) {
        Preconditions.checkArgument(!contents.isReadOnly(), "Cannot wrap a read-only ByteArraySegment.");
        this.config = Preconditions.checkNotNull(config, "config");
        this.contents = Preconditions.checkNotNull(contents, "contents");
        this.header = contents.subSegment(0, DATA_OFFSET);
        this.data = contents.subSegment(DATA_OFFSET, contents.getLength() - DATA_OFFSET - FOOTER_LENGTH);
        this.footer = contents.subSegment(contents.getLength() - FOOTER_LENGTH, FOOTER_LENGTH);
        if (validate) {
            int headerId = getHeaderId();
            int footerId = getFooterId();
            if (headerId != footerId) {
                throw new IllegalDataFormatException("Invalid Page Format (id mismatch). HeaderId=%s, FooterId=%s.", headerId, footerId);
            }
        }

        // Cache the count value. It's used a lot.
        this.count = BitConverter.readInt(this.header, COUNT_OFFSET);
    }
    /**
     * Formats the Header and Footer of this BTreePage with the given information.
     *
     * @param itemCount The number of items in this BTreePage.
     * @param id        The Id of this BTreePage.
     */
    private void formatHeaderAndFooter(int itemCount, int id) {
        // Header.
        this.header.set(VERSION_OFFSET, CURRENT_VERSION);
        this.header.set(FLAGS_OFFSET, getFlags(this.config.isIndexPage ? FLAG_INDEX_PAGE : FLAG_NONE));
        setHeaderId(id);
        setCount(itemCount);

        // Matching footer.
        setFooterId(id);
    }

    //endregion

    //region Operations

    /**
     * Determines whether the given ByteArraySegment represents an Index Page
     *
     * @param pageContents The ByteArraySegment to check.
     * @return True if Index Page, False if Leaf page.
     * @throws IllegalDataFormatException If the given contents is not a valid BTreePage format.
     */
    static boolean isIndexPage(@Nonnull ByteArraySegment pageContents) {
        // Check ID match.
        int headerId = BitConverter.readInt(pageContents, ID_OFFSET);
        int footerId = BitConverter.readInt(pageContents, pageContents.getLength() - FOOTER_LENGTH);
        if (headerId != footerId) {
            throw new IllegalDataFormatException("Invalid Page Format (id mismatch). HeaderId=%s, FooterId=%s.", headerId, footerId);
        }

        int flags = pageContents.get(FLAGS_OFFSET);
        return (flags & FLAG_INDEX_PAGE) == FLAG_INDEX_PAGE;
    }

    /**
     * Gets a value representing the number of bytes in this BTreePage (header and footer included).
     *
     * @return The number of bytes.
     */
    int getLength() {
        return this.contents.getLength();
    }

    /**
     * Gets a ByteArraySegment representing the value at the given Position.
     *
     * @param pos The position to get the value at.
     * @return A ByteArraySegment containing the value at the given Position. Note that this is a view inside a larger array
     * and any modifications to that array will be reflected in this. If this value needs to be held for
     * longer then it is recommended to get a copy of it (use getCopy()).
     */
    ByteArraySegment getValueAt(int pos) {
        Preconditions.checkElementIndex(pos, getCount(), "pos must be non-negative and smaller than the number of items.");
        return this.data.subSegment(pos * this.config.entryLength + this.config.keyLength, this.config.valueLength);
    }

    /**
     * Gets the Key at the given Position.
     *
     * @param pos The Position to get the Key at.
     * @return A ByteArraySegment containing the Key at the given Position. Note that this is a view inside a larger array
     * and any modifications to that array will be reflected in this. If this value needs to be held for
     * longer then it is recommended to get a copy of it (use getCopy()).
     */
    ByteArraySegment getKeyAt(int pos) {
        Preconditions.checkElementIndex(pos, getCount(), "pos must be non-negative and smaller than the number of items.");
        //return new ByteArraySegment(this.data.subSegment(pos * this.config.entryLength, this.config.keyLength).getCopy());
        return this.data.subSegment(pos * this.config.entryLength, this.config.keyLength);
    }

    /**
     * If necessary, splits the contents of this BTreePage instance into multiple BTreePages. This instance will not be
     * modified as a result of this operation (all new BTreePages will be copies).
     *
     * The resulting pages will be about half full each, and when combined in order, they will contain the same elements
     * as this BTreePage, in the same order.
     *
     * Split Conditions:
     * * Length > MaxPageSize
     *
     * @return If a split is made, an ordered List of BTreePage instances. If no split is necessary (condition is not met),
     * returns null.
     */
    List<BTreePage> splitIfNecessary() {
        if (this.contents.getLength() <= this.config.getMaxPageSize()) {
            // Nothing to do.
            return null;
        }

        // Calculate how many pages to split into. While doing so, take care to account that we may only have whole entries
        // in each page, and not partial ones.
        int maxDataLength = (this.config.getMaxPageSize() - this.header.getLength() - this.footer.getLength()) / this.config.entryLength * this.config.entryLength;
        int remainingPageCount = (int) Math.ceil((double) this.data.getLength() / maxDataLength);

        ArrayList<BTreePage> result = new ArrayList<>(remainingPageCount);
        int readIndex = 0;
        int remainingItems = getCount();
        while (remainingPageCount > 0) {
            // Calculate how many items to include in this split page. This is the average of the remaining items over
            // the remaining page count (this helps smooth out cases when the original getCount() is not divisible by
            // the calculated number of splits).
            int itemsPerPage = remainingItems / remainingPageCount;

            // Copy data over to the new page.
            ByteArraySegment splitPageData = this.data.subSegment(readIndex, itemsPerPage * this.config.entryLength);
            result.add(new BTreePage(this.config, itemsPerPage, splitPageData));

            // Update pointers.
            readIndex += splitPageData.getLength();
            remainingPageCount--;
            remainingItems -= itemsPerPage;
        }

        assert readIndex == this.data.getLength() : "did not copy everything";
        return result;
    }

    /**
     * Updates the contents of this BTreePage with the given entries. Entries whose keys already exist will update the data,
     * while Entries whose keys do not already exist will be inserted.
     *
     * After this method completes, this BTreePage:
     * * May overflow (a split may be necessary)
     * * Will have all entries sorted by Key
     *
     * @param entries The Entries to insert or update. This collection need not be sorted.
     */
    void update(@Nonnull Collection<PageEntry> entries) {
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
            if (e.getKey().getLength() != this.config.keyLength || e.getValue().getLength() != this.config.valueLength) {
                throw new IllegalDataFormatException("Found an entry with unexpected Key or Value length.");
            }

            // Figure out if this entry exists already.
            val searchResult = search(e.getKey(), lastPos);
            if (searchResult.isExactMatch()) {
                // Keys already exists: update in-place.
                setValueAtPosition(searchResult.getPosition(), e.getValue());
            } else {
                // This entry's key does not exist. We need to remember it for later. Since this was not an exact match,
                // binary search returned the position where it should have been.
                int dataIndex = searchResult.getPosition() * this.config.entryLength;
                newEntries.add(new AbstractMap.SimpleImmutableEntry<>(dataIndex, e));
            }

            // Remember the last position so we may resume the next search from there.
            lastPos = searchResult.position;
        }

        if (newEntries.isEmpty()) {
            // Nothing else to change. We've already updated the keys in-place.
            return;
        }

        int newCount = getCount() + newEntries.size();

        // If we have extra entries: allocate new buffer of the correct size and start copying from the old one.
        // We cannot reuse the existing buffer because we need more space.
        val newPage = new BTreePage(this.config, new ByteArraySegment(new byte[DATA_OFFSET + newCount * this.config.entryLength + FOOTER_LENGTH]), false);
        newPage.formatHeaderAndFooter(newCount, getHeaderId());
        int readIndex = 0;
        int writeIndex = 0;
        for (val e : newEntries) {
            int entryIndex = e.getKey();
            if (entryIndex > readIndex) {
                // Copy from source.
                int length = entryIndex - readIndex;
                assert length % this.config.entryLength == 0;
                newPage.data.copyFrom(this.data, readIndex, writeIndex, length);
                writeIndex += length;
            }

            // Write new Entry.
            PageEntry entryContents = e.getValue();
            newPage.setEntryAtIndex(writeIndex, entryContents);
            writeIndex += this.config.entryLength;
            readIndex = entryIndex;
        }

        if (readIndex < this.data.getLength()) {
            // Copy the last part that we may have missed.
            int length = this.data.getLength() - readIndex;
            newPage.data.copyFrom(this.data, readIndex, writeIndex, length);
        }

        // Make sure we swap all the segments with those from the new page. We need to release all pointers to our
        // existing buffers.
        this.header = newPage.header;
        this.data = newPage.data;
        this.contents = newPage.contents;
        this.footer = newPage.footer;
        this.count = newPage.count;
    }

    /**
     * Updates the contents of this BTreePage so that it does not contain any entry with the given Keys anymore.
     *
     * After this method completes, this BTreePage:
     * * May underflow (a merge may be necessary)
     * * Will have all entries sorted by Key
     * * Will reuse the same underlying buffer as before (no new buffers allocated). As such it may underutilize the buffer.
     *
     * @param keys A Collection of Keys to remove. The Keys need not be sorted.
     */
    void delete(@Nonnull Collection<ByteArraySegment> keys) {
        if (keys.isEmpty()) {
            // Nothing to do.
            return;
        }

        // Process all Keys, in order, record the position of any matches.
        int lastPos = 0;
        int initialCount = getCount();
        val removedPositions = new ArrayList<Integer>();
        val keyIterator = keys.stream().sorted(KEY_COMPARATOR::compare).iterator();
        while (keyIterator.hasNext() && removedPositions.size() < initialCount) {
            val key = keyIterator.next();
            if (key.getLength() != this.config.keyLength) {
                throw new IllegalDataFormatException("Found a key with unexpected length.");
            }

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

            // Trim away the data buffer, move the footer back and trim the contents buffer.
            assert writeIndex == (initialCount - removedPositions.size() + 1) * this.config.entryLength : "unexpected number of bytes remaining";
            shrink(newCount);
        }
    }

    /**
     * Gets a ByteArraySegment representing the value mapped to the given Key.
     *
     * @param key The Key to search.
     * @return A ByteArraySegment mapped to the given Key, or null if the Key does not exist. Note that this is a view
     * inside a larger array and any modifications to that array will be reflected in this. If this value needs to be held
     * for longer then it is recommended to get a copy of it (use getCopy()).
     */
    ByteArraySegment searchExact(@Nonnull ByteArraySegment key) {
        val pos = search(key, 0);
        if (!pos.isExactMatch()) {
            // Nothing found.
            return null;
        }

        return getValueAt(pos.getPosition());
    }

    /**
     * Performs a (binary) search for the given Key in this BTreePage and returns its position.
     *
     * @param key      A ByteArraySegment that represents the key to search.
     * @param startPos The starting position (not array index) to begin the search at. Any positions prior to this one
     *                 will be ignored.
     * @return A SearchResult instance with the result of the search.
     */
    SearchResult search(@Nonnull ByteArraySegment key, int startPos) {
        // Positions here are not indices into "source", rather they are entry positions, which is why we always need
        // to adjust by using entryLength.
        int endPos = getCount();
        Preconditions.checkArgument(startPos <= endPos, "startPos must be non-negative and smaller than the number of items.");
        while (startPos < endPos) {
            // Locate the Key in the middle.
            int midPos = startPos + (endPos - startPos) / 2;

            // Compare it to the sought key.
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

        // Return an inexact search result with the position for the key that is right before the sought key.
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
     * Updates the Header of this BTreePage to reflect that it contains the given number of items. This does not perform
     * any resizing.
     *
     * @param itemCount The count to set.
     */
    private void setCount(int itemCount) {
        BitConverter.writeInt(this.header, COUNT_OFFSET, itemCount);
        this.count = itemCount;
    }

    /**
     * Gets the number of items in this BTreePage as reflected in its header.
     */
    int getCount() {
        return this.count;
    }

    List<ByteArraySegment> getAllKeys() {
        int count = getCount();
        ArrayList<ByteArraySegment> result = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            result.add(getKeyAt(i));
        }
        return result;
    }

    /**
     * Sets the Value at the given position.
     *
     * @param pos   The Position to set the value at.
     * @param value A ByteArraySegment representing the value to set.
     */
    private void setValueAtPosition(int pos, ByteArraySegment value) {
        Preconditions.checkElementIndex(pos, getCount(), "pos must be non-negative and smaller than the number of items.");
        Preconditions.checkArgument(value.getLength() == this.config.valueLength, "Given value has incorrect length.");
        this.data.copyFrom(value, pos * this.config.entryLength + this.config.keyLength, value.getLength());
    }

    private void setEntryAtIndex(int dataIndex, PageEntry entry) {
        Preconditions.checkElementIndex(dataIndex, this.data.getLength(), "dataIndex must be non-negative and smaller than the size of the data.");
        Preconditions.checkArgument(entry.getKey().getLength() == this.config.keyLength, "Given entry key has incorrect length.");
        Preconditions.checkArgument(entry.getValue().getLength() == this.config.valueLength, "Given entry value has incorrect length.");

        this.data.copyFrom(entry.getKey(), dataIndex, entry.getKey().getLength());
        this.data.copyFrom(entry.getValue(), dataIndex + this.config.keyLength, entry.getValue().getLength());
    }

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
    int getHeaderId() {
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
    @RequiredArgsConstructor
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
        }
    }

    //endregion

    //region SearchResult

    /**
     * The result of a BTreePage Search.
     */
    @Getter
    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    static class SearchResult {
        /**
         * The resulting position.
         */
        private final int position;
        /**
         * Indicates whether an exact match for the sought key was found. If so, position refers to that key. If not,
         * position refers to the location where the key would have been.
         */
        private final boolean exactMatch;

        @Override
        public String toString() {
            return String.format("%s (%s)", this.position, this.exactMatch ? "E" : "NE");
        }
    }

    //endregion
}
