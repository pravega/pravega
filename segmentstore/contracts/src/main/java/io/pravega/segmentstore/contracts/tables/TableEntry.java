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
package io.pravega.segmentstore.contracts.tables;

import io.pravega.common.util.BufferView;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

/**
 * An Entry in a Table Segment, made up of a Key and a Value, with optional Version.
 */
@Getter
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class TableEntry {
    /**
     * The Key.
     */
    private final TableKey key;

    /**
     * The Value (data) of the entry.
     */
    private final BufferView value;
    /**
     * Creates a new instance of the TableEntry class with no desired version.
     *
     * @param key   The Key.
     * @param value The Value.
     *
     * @return the TableEntry that was created
     */
    public static TableEntry unversioned(@NonNull BufferView key, @NonNull BufferView value) {
        return new TableEntry(TableKey.unversioned(key), value);
    }

    /**
     * Creates a new instance of the TableEntry class that indicates the Key must not previously exist.
     *
     * @param key   The Key.
     * @param value The Value.
     *
     * @return newly created TableEntry if one for the key does not already exist.
     *
     */
    public static TableEntry notExists(@NonNull BufferView key, @NonNull BufferView value) {
        return new TableEntry(TableKey.notExists(key), value);
    }

    /**
     * Creates a new instance of the TableEntry class that indicates the Key must not previously exist.
     *
     * @param key   The Key.
     *
     * @return newly created TableEntry if one for the key does not already exist.
     *
     */
    public static TableEntry notExists(@NonNull BufferView key) {
        return new TableEntry(TableKey.notExists(key), null);
    }

    /**
     * Creates a new instance of the TableEntry class with a specified version.
     *
     * @param key   The Key.
     * @param value The Value.
     * @param version The desired version.
     *
     * @return new instance of Table Entry with a specified version
     */
    public static TableEntry versioned(@NonNull BufferView key, @NonNull BufferView value, long version) {
        return new TableEntry(TableKey.versioned(key, version), value);
    }

    @Override
    public String toString() {
        return String.format("%s -> %s", this.key, this.value);
    }

    @Override
    public int hashCode() {
        return this.key.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof TableEntry) {
            TableEntry other = (TableEntry) obj;
            return this.key.equals(other.key)
                    && ((this.value == null && other.value == null)
                    || (this.value != null && other.value != null && this.value.equals(other.getValue())));

        }

        return false;
    }
}
