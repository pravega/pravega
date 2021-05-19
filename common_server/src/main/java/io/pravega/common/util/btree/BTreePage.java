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
package io.pravega.common.util.btree;

import com.google.common.base.Preconditions;
import io.pravega.common.util.BufferViewComparator;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.common.util.IllegalDataFormatException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import javax.annotation.concurrent.NotThreadSafe;
import lombok.Getter;
import lombok.NonNull;
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
     * Format Version related fields. The version itself is the first byte of the serialization. When we will have to
     * support multiple versions, we will need to read this byte and choose the appropriate deserialization approach.
     * We cannot use VersionedSerializer in here - doing so would prevent us from efficiently querying and modifying the
     * page contents itself, as it would force us to load everything in memory (as objects) and then reserialize them.
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
    private static final byte FLAG_INDEX_PAGE = 1; // If set, indicates this is an Index Page; if not, it's a Leaf Page.

    /**
     * Page Id: Randomly generated Integer that is written both in the Header and Footer. This enables us to validate
     * that whatever ByteArraySegment we receive for deserialization has the appropriate length.
     */
    private static final int ID_OFFSET = FLAGS_OFFSET + FLAGS_LENGTH;
    private static final int ID_LENGTH = 4;

    /**
     * Element Count.
     */
    private static final int COUNT_OFFSET = ID_OFFSET + ID_LENGTH;
    private static final int COUNT_LENGTH = 4; // Allows overflowing, but needed in order to do splits.

    /**
     * Data (contents).
     */
    private static final int DATA_OFFSET = COUNT_OFFSET + COUNT_LENGTH;

    /**
     * Footer: Contains just the Page Id, which should match the value written in the Header.
     */
    private static final int FOOTER_LENGTH = ID_LENGTH;

    //endregion

    //region Members

    private static final BufferViewComparator KEY_COMPARATOR = BufferViewComparator.create();
    private static final Random ID_GENERATOR = new Random();

    /**
     * The entire ByteArraySegment that makes up this BTreePage. This includes Header, Data and Footer.
     */
    @Getter
    private ByteArraySegment contents;
    /**
     * The Footer section of the BTreePage ByteArraySegment.
     */
    private ByteArraySegment header;
    /**
     * The Data section of the BTreePage ByteArraySegment.
     */
    private ByteArraySegment data;
    /**
     * The Footer section of the BTreePage ByteArraySegment.
     */
    private ByteArraySegment footer;
    @Getter
    private final Config config;
    /**
     * The number of items in this BTreePage as reflected in its header.
     */
    @Getter
    private int count;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of an empty BTreePage class..
     *
     * @param config Page Configuration.
     */
    BTreePage(Config config) {
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
    BTreePage(Config config, ByteArraySegment contents) {
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
    private BTreePage(Config config, int count, ByteArraySegment data) {
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
    private BTreePage(@NonNull Config config, @NonNull ByteArraySegment contents, boolean validate) {
        this.config = config;
        this.contents = contents;
        this.header = contents.slice(0, DATA_OFFSET);
        this.data = contents.slice(DATA_OFFSET, contents.getLength() - DATA_OFFSET - FOOTER_LENGTH);
        this.footer = contents.slice(contents.getLength() - FOOTER_LENGTH, FOOTER_LENGTH);
        if (validate) {
            int headerId = getHeaderId();
            int footerId = getFooterId();
            if (headerId != footerId) {
                throw new IllegalDataFormatException("Invalid Page Format (id mismatch). HeaderId=%s, FooterId=%s.", headerId, footerId);
            }
        }

        // Cache the count value. It's used a lot.
        this.count = this.header.getInt(COUNT_OFFSET);
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
    static boolean isIndexPage(@NonNull ByteArraySegment pageContents) {
        // Check ID match.
        int headerId = pageContents.getInt(ID_OFFSET);
        int footerId = pageContents.getInt(pageContents.getLength() - FOOTER_LENGTH);
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
        return this.data.slice(pos * this.config.entryLength + this.config.keyLength, this.config.valueLength);
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
        return this.data.slice(pos * this.config.entryLength, this.config.keyLength);
    }

    /**
     * Updates the first PageEntry's key to the given value.
     *
     * @param newKey A ByteArraySegment representing the replacement value for the first key. This must be smaller than
     *               or equal to the current value of the first key (using KEY_COMPARATOR).
     */
    void setFirstKey(ByteArraySegment newKey) {
        Preconditions.checkState(getCount() > 0, "BTreePage is empty. Cannot set first key.");
        Preconditions.checkArgument(newKey.getLength() == this.config.getKeyLength(), "Incorrect key length.");
        Preconditions.checkArgument(KEY_COMPARATOR.compare(newKey, getKeyAt(0)) <= 0,
                "Replacement first Key must be smaller than or equal to the existing first key.");

        this.data.copyFrom(newKey, 0, newKey.getLength());
    }

    /**
     * Gets a PageEntry representing the entry (Key + Value) at the given Position.
     *
     * @param pos The position to get the value at.
     * @return A PageEntry containing the entry at the given Position. Note that the ByteArraySegments returned by this
     * PageEntry's getKey() and getValue() are views inside a larger array and any modifications to that array will be
     * reflected in them. If this value needs to be held for longer then it is recommended to get a copy of it (use getCopy()).
     */
    PageEntry getEntryAt(int pos) {
        Preconditions.checkElementIndex(pos, getCount(), "pos must be non-negative and smaller than the number of items.");
        return new PageEntry(
                this.data.slice(pos * this.config.entryLength, this.config.keyLength),
                this.data.slice(pos * this.config.entryLength + this.config.keyLength, this.config.valueLength));
    }

    /**
     * Gets all the Page Entries between the two indices.
     *
     * @param firstIndex The first index to get Page Entries from (inclusive).
     * @param lastIndex  The last index to get Page Entries to (inclusive).
     * @return A new List containing the desired result.
     */
    List<PageEntry> getEntries(int firstIndex, int lastIndex) {
        Preconditions.checkArgument(firstIndex <= lastIndex, "firstIndex must be smaller than or equal to lastIndex.");
        ArrayList<PageEntry> result = new ArrayList<>();
        for (int i = firstIndex; i <= lastIndex; i++) {
            result.add(getEntryAt(i));
        }

        return result;
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
            ByteArraySegment splitPageData = this.data.slice(readIndex, itemsPerPage * this.config.entryLength);
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
     * @param entries The Entries to insert or update. This List must be sorted by {@link PageEntry#getKey()}.
     * @return A delta (negative, zero or positive) indicating the change in {@link #getCount()} as a result of this update.
     * @throws IllegalDataFormatException If any of the entries do not conform to the Key/Value size constraints.
     * @throws IllegalArgumentException   If the entries are not sorted by {@link PageEntry#getKey()}.
     */
    int update(@NonNull List<PageEntry> entries) {
        if (entries.isEmpty()) {
            // Nothing to do.
            return 0;
        }

        // Apply the in-place updates and collect the new entries to be added.
        val ci = applyUpdates(entries);
        if (ci.changes.isEmpty()) {
            // Nothing else to change. We've already updated the keys in-place.
            return 0;
        }

        val newPage = applyInsertsAndRemovals(ci);

        // Make sure we swap all the segments with those from the new page. We need to release all pointers to our
        // existing buffers.
        this.header = newPage.header;
        this.data = newPage.data;
        this.contents = newPage.contents;
        this.footer = newPage.footer;
        val delta = newPage.count - this.count;
        this.count = newPage.count;
        return delta;
    }

    /**
     * Gets a ByteArraySegment representing the value mapped to the given Key.
     *
     * @param key The Key to search.
     * @return A ByteArraySegment mapped to the given Key, or null if the Key does not exist. Note that this is a view
     * inside a larger array and any modifications to that array will be reflected in this. If this value needs to be held
     * for longer then it is recommended to get a copy of it (use getCopy()).
     */
    ByteArraySegment searchExact(@NonNull ByteArraySegment key) {
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
    SearchResult search(@NonNull ByteArraySegment key, int startPos) {
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

        // Return an inexact search result with the position where the sought key would have been.
        return new SearchResult(startPos, false);
    }

    /**
     * Updates (in-place) the contents of this BTreePage with the given entries for those Keys that already exist. For
     * all the new or deleted Keys, collects them into a List and calculates the offset where they would have to be
     * inserted at or removed from.
     *
     * @param entries A List of PageEntries to update, in sorted order by {@link PageEntry#getKey()}.
     * @return A {@link ChangeInfo} object.
     * @throws IllegalDataFormatException If any of the entries do not conform to the Key/Value size constraints.
     * @throws IllegalArgumentException   If the entries are not sorted by {@link PageEntry#getKey()}.
     */
    private ChangeInfo applyUpdates(List<PageEntry> entries) {
        // Keep track of new keys to be added along with the offset (in the original page) where they would have belonged.
        val changes = new ArrayList<Map.Entry<Integer, PageEntry>>();
        int removeCount = 0;

        // Process all the Entries, in order (by Key).
        int lastPos = 0;
        ByteArraySegment lastKey = null;
        for (val e : entries) {
            if (e.getKey().getLength() != this.config.keyLength || (e.hasValue() && e.getValue().getLength() != this.config.valueLength)) {
                throw new IllegalDataFormatException("Found an entry with unexpected Key or Value length.");
            }

            if (lastKey != null) {
                Preconditions.checkArgument(KEY_COMPARATOR.compare(lastKey, e.getKey()) < 0,
                        "Entries must be sorted by key and no duplicates are allowed.");
            }

            // Figure out if this entry exists already.
            val searchResult = search(e.getKey(), lastPos);
            if (searchResult.isExactMatch()) {
                if (e.hasValue()) {
                    // Key already exists: update in-place.
                    setValueAtPosition(searchResult.getPosition(), e.getValue());
                } else {
                    // Key exists but this is a removal. Record it for later.
                    changes.add(new AbstractMap.SimpleImmutableEntry<>(searchResult.getPosition(), null));
                    removeCount++;
                }
            } else if (e.hasValue()) {
                // This entry's key does not exist and we want to insert it (we don't care if we want to delete an inexistent
                // key). We need to remember it for later. Since this was not an exact match, binary search returned the
                // position where it should have been.
                changes.add(new AbstractMap.SimpleImmutableEntry<>(searchResult.getPosition(), e));
            }

            // Remember the last position so we may resume the next search from there.
            lastPos = searchResult.getPosition();
            lastKey = e.getKey();
        }

        return new ChangeInfo(changes, changes.size() - removeCount, removeCount);
    }

    /**
     * Inserts the new PageEntry instances at the given offsets.
     *
     * @param ci A {@link ChangeInfo} object containing information to change.
     * @return A new BTreePage instance with the updated contents.
     */
    private BTreePage applyInsertsAndRemovals(ChangeInfo ci) {
        int newCount = getCount() + ci.insertCount - ci.deleteCount;

        // Allocate new buffer of the correct size and start copying from the old one.
        val newPage = new BTreePage(this.config, new ByteArraySegment(new byte[DATA_OFFSET + newCount * this.config.entryLength + FOOTER_LENGTH]), false);
        newPage.formatHeaderAndFooter(newCount, getHeaderId());
        int readIndex = 0;
        int writeIndex = 0;
        for (val e : ci.changes) {
            int entryIndex = e.getKey() * this.config.entryLength;
            if (entryIndex > readIndex) {
                // Copy from source.
                int length = entryIndex - readIndex;
                assert length % this.config.entryLength == 0;
                newPage.data.copyFrom(this.data, readIndex, writeIndex, length);
                writeIndex += length;
            }

            // Write new Entry.
            PageEntry entryContents = e.getValue();
            readIndex = entryIndex;
            if (entryContents != null) {
                // Insert new PageEntry.
                newPage.setEntryAtIndex(writeIndex, entryContents);
                writeIndex += this.config.entryLength;
            } else {
                // This PageEntry has been deleted. Skip over it.
                readIndex += this.config.getEntryLength();
            }

        }

        if (readIndex < this.data.getLength()) {
            // Copy the last part that we may have missed.
            int length = this.data.getLength() - readIndex;
            newPage.data.copyFrom(this.data, readIndex, writeIndex, length);
        }

        return newPage;
    }

    /**
     * Updates the Header of this BTreePage to reflect that it contains the given number of items. This does not perform
     * any resizing.
     *
     * @param itemCount The count to set.
     */
    private void setCount(int itemCount) {
        this.header.setInt(COUNT_OFFSET, itemCount);
        this.count = itemCount;
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
        return this.header.getInt(ID_OFFSET);
    }

    /**
     * Gets this BTreePage's Id from its footer.
     */
    private int getFooterId() {
        return this.footer.getInt(0);
    }

    /**
     * Updates the Header to contain the given id.
     */
    private void setHeaderId(int id) {
        this.header.setInt(ID_OFFSET, id);
    }

    /**
     * Updates the Footer to contain the given id.
     */
    private void setFooterId(int id) {
        this.footer.setInt(0, id);
    }

    //endregion

    //region ChangeInfo

    /**
     * Keeps track of updates that would result in structural changes to the Page.
     */
    @RequiredArgsConstructor
    private static class ChangeInfo {
        /**
         * A sorted List of Map.Entry instances (Offset -> PageEntry) indicating the new PageEntry instances to insert or
         * remove and at which offset.
         */
        private final List<Map.Entry<Integer, PageEntry>> changes;
        /**
         * The number of insertions.
         */
        private final int insertCount;
        /**
         * The number of removals.
         */
        private final int deleteCount;
    }

    //endregion

    //region Config

    /**
     * BTreePage Configuration.
     */
    @RequiredArgsConstructor
    @Getter
    static class Config {
        private static final int MAX_PAGE_SIZE = Short.MAX_VALUE; // 2-byte (signed) short.
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
            Preconditions.checkArgument(maxPageSize <= MAX_PAGE_SIZE, "maxPageSize must be at most %s, given %s.", MAX_PAGE_SIZE, maxPageSize);
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
}
