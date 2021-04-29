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
package io.pravega.client.tables;

import java.nio.ByteBuffer;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

/**
 * A {@link KeyValueTable} Entry.
 */
@Getter
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
@ToString
public class TableEntry {
    /**
     * The {@link TableKey}.
     *
     * @return The content of {@link TableKey}.
     */
    @NonNull
    private final TableKey key;

    /**
     * The Value.
     *
     * @return The value associated to the {@link TableKey}.
     */
    private final ByteBuffer value;

    /**
     * Gets a value indicating whether this {@link TableEntry} has a specified {@link Version}.
     *
     * If versioned, then the following methods on {@link KeyValueTable} will be conditioned on this {@link TableEntry}'s
     * {@link TableKey} matching the server-side version with that provided in this instance:
     * - {@link KeyValueTable#put(TableEntry)}
     * - {@link KeyValueTable#putAll(Iterable)}
     *
     * @return True if versioned, false otherwise.
     */
    public boolean isVersioned() {
        return this.key.isVersioned();
    }

    /**
     * Creates a new {@link TableEntry} with a Key made of only a Primary Key, with no specific version.
     *
     * @param primaryKey The Entry Key (Primary Key only).
     * @param value      The Entry Value.
     * @return An unversioned {@link TableEntry} (version set to {@link Version#NO_VERSION}).
     */
    public static TableEntry anyVersion(ByteBuffer primaryKey, ByteBuffer value) {
        return anyVersion(primaryKey, null, value);
    }

    /**
     * Creates a new {@link TableEntry} with a Key made of a Primary Key and Secondary Key, with no specific version.
     *
     * @param primaryKey   The Entry Key's Primary Key.
     * @param secondaryKey The Entry Key's Secondary Key.
     * @param value        The Entry Value.
     * @return An unversioned {@link TableEntry} (version set to {@link Version#NO_VERSION}).
     */
    public static TableEntry anyVersion(ByteBuffer primaryKey, ByteBuffer secondaryKey, ByteBuffer value) {
        return new TableEntry(TableKey.anyVersion(primaryKey, secondaryKey), value);
    }

    /**
     * Creates a new {@link TableEntry} with a Key made of only a Primary Key, with a version that indicates the key
     * must not exist.
     *
     * @param primaryKey The Entry Key (Primary Key only).
     * @param value      The Entry Value.
     * @return A {@link TableEntry} with a version set to {@link Version#NOT_EXISTS}.
     */
    public static TableEntry absent(ByteBuffer primaryKey, ByteBuffer value) {
        return absent(primaryKey, null, value);
    }

    /**
     * Creates a new {@link TableEntry} with a Key made of a Primary Key and Secondary Keys, with a version that indicates
     * the key must not exist.
     *
     * @param primaryKey   The Entry Key's Primary Key.
     * @param secondaryKey The Entry Key's Secondary Key.
     * @param value        The Entry Value.
     * @return A {@link TableEntry} with a version set to {@link Version#NOT_EXISTS}.
     */
    public static TableEntry absent(ByteBuffer primaryKey, ByteBuffer secondaryKey, ByteBuffer value) {
        return new TableEntry(TableKey.absent(primaryKey, secondaryKey), value);
    }

    /**
     * Creates a new {@link TableEntry} with a Key made of only a Primary Key, with a specific key version.
     *
     * @param primaryKey The Entry Key (Primary Key only).
     * @param value      The Entry Value.
     * @param version    The {@link Version} to use.
     * @return A {@link TableEntry}.
     */
    public static TableEntry versioned(ByteBuffer primaryKey, Version version, ByteBuffer value) {
        return versioned(primaryKey, null, version, value);
    }

    /**
     * Creates a new {@link TableEntry} with a Key made of a Primary Key and Secondary Key, with a specific key version.
     *
     * @param primaryKey   The Entry Key's Primary Key.
     * @param secondaryKey The Entry Key's Secondary Key.
     * @param value        The Entry Value.
     * @param version      The {@link Version} to use.
     * @return A {@link TableEntry}.
     */
    public static TableEntry versioned(ByteBuffer primaryKey, ByteBuffer secondaryKey, Version version, ByteBuffer value) {
        return new TableEntry(TableKey.versioned(primaryKey, secondaryKey, version), value);
    }
}
