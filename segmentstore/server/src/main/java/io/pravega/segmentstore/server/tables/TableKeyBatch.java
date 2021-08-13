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
package io.pravega.segmentstore.server.tables;

import com.google.common.annotations.VisibleForTesting;
import io.pravega.segmentstore.contracts.tables.TableKey;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * Represents a collection of Items (relating to {@link TableKey}s) that will either all be applied at once or none at all.
 */
@Getter
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
class TableKeyBatch {
    /**
     * All the Items in the TableKeyBatch.
     */
    private final List<Item> items = new ArrayList<>();

    /**
     * The Items in the TableKeyBatch that have a condition on them. This is a subset of Items.
     */
    private final List<Item> versionedItems = new ArrayList<>();

    /**
     * If true, indicates that all the Items in this batch are supposed to be removed (as opposed from being updated).
     */
    private final boolean removal;

    /**
     * The length of this TableKeyBatch, in bytes.
     */
    private int length;

    /**
     * Creates a new instance of the TableKeyBatch class which will insert or update keys.
     */
    @VisibleForTesting
    static TableKeyBatch update() {
        return new TableKeyBatch(false);
    }

    /**
     * Creates a new instance of the TableKeyBatch class that will remove keys.
     */
    @VisibleForTesting
    static TableKeyBatch removal() {
        return new TableKeyBatch(true);
    }

    /**
     * Adds a new Item to this TableKeyBatch.
     *
     * @param key    The {@link TableKey} representing the Key to add.
     * @param hash   The Key Hash corresponding to the Key.
     * @param length The serialized length of this Batch Entry. Note that this will almost always be larger than the size
     *               of the key, as it encompasses the whole Table Entry (which includes the value and other metadata as well).
     */
    void add(TableKey key, UUID hash, int length) {
        Item item = new Item(key, hash, this.length);
        this.items.add(item);
        this.length += length;
        if (key.hasVersion()) {
            this.versionedItems.add(item);
        }
    }

    /**
     * Gets a value indicating whether is TableKeyBatch should be treated as a Conditional Update. This is true if at least
     * one of the Items added has a Condition set.
     *
     * @return True if Conditional Update, False otherwise.
     */
    boolean isConditional() {
        return this.versionedItems.size() > 0;
    }

    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    @Getter
    static class Item {
        /**
         * The {@link TableKey} this TableKeyBatch Item refers to.
         */
        private final TableKey key;

        /**
         * The Key Hash that was computed for the key.
         */
        private final UUID hash;

        /**
         * The offset within the TableKeyBatch where this Item is located (NOTE: this is not the Segment Offset!).
         */
        private final int offset;

        @Override
        public String toString() {
            return String.format("Offset = %s, Hash = %s, Key = %s", this.offset, this.hash, this.key);
        }
    }
}
