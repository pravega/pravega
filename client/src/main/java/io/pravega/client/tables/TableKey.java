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
 * A {@link KeyValueTable} Key with a {@link Version}.
 */
@Getter
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
@ToString
public class TableKey {
    /**
     * The Primary Key.
     *
     * @param primaryKey Primary Key..
     * @return Primary Key.
     */
    @NonNull
    private final ByteBuffer primaryKey;

    /**
     * The Secondary Key (Optional).
     *
     * @param version Secondary Key.
     * @return Secondary Key.
     */
    private final ByteBuffer secondaryKey;

    /**
     * The {@link Version}. If null, any updates for this Key will be unconditional. See {@link KeyValueTable} for
     * details on conditional updates.
     *
     * @param version Version associated with the key.
     * @return Version associated with the key.
     */
    @NonNull
    private final Version version;

    /**
     * Gets a value indicating whether this {@link TableKey} has a specified {@link Version}.
     *
     * If versioned, then the following methods on {@link KeyValueTable} will be conditioned on this {@link TableKey}
     * matching the server-side version with that provided in this instance:
     * - {@link KeyValueTable#remove(TableKey)}
     * - {@link KeyValueTable#removeAll(Iterable)}
     *
     * @return True if versioned, false otherwise.
     */
    public boolean isVersioned() {
        return this.version.equals(Version.NO_VERSION);
    }

    /**
     * Creates a new {@link TableKey} made of only a Primary Key, with no specific version.
     *
     * @param primaryKey The Primary Key.
     * @return An unversioned {@link TableKey} made of only a Primary Key (version set to {@link Version#NO_VERSION}).
     */
    public static TableKey unversioned(ByteBuffer primaryKey) {
        return unversioned(primaryKey, null);
    }

    /**
     * Creates a new {@link TableKey} made of a Primary Key and Secondary Key, with no specific version.
     *
     * @param primaryKey   The Primary Key.
     * @param secondaryKey The Secondary Key.
     * @return An unversioned {@link TableKey} made of a Primary Key and Secondary Key (version set to
     * {@link Version#NO_VERSION}).
     */
    public static TableKey unversioned(ByteBuffer primaryKey, ByteBuffer secondaryKey) {
        return versioned(primaryKey, secondaryKey, Version.NO_VERSION);
    }

    /**
     * Creates a new {@link TableKey} made of only a Primary Key with a version that indicates the key must not exist.
     *
     * @param primaryKey The Primary Key.
     * @return A {@link TableKey} made of a Primary Key with a version set to {@link Version#NOT_EXISTS}.
     */
    public static TableKey notExists(ByteBuffer primaryKey) {
        return notExists(primaryKey, null);
    }

    /**
     * Creates a new {@link TableKey} made of a Primary Key and Secondary Key, with a version that indicates the key
     * must not exist.
     *
     * @param primaryKey   The Primary Key.
     * @param secondaryKey The Secondary Key.
     * @return A {@link TableKey} made of a Primary Key and Secondary Key with a version set to {@link Version#NOT_EXISTS}.
     */
    public static TableKey notExists(ByteBuffer primaryKey, ByteBuffer secondaryKey) {
        return versioned(primaryKey, secondaryKey, Version.NOT_EXISTS);
    }

    /**
     * Creates a new {@link TableKey} made of only a Primary Key, with a specific version.
     *
     * @param primaryKey The Primary Key.
     * @param version    The {@link Version}.
     * @return A {@link TableKey} made of a Primary Key with the specified version.
     */
    public static TableKey versioned(ByteBuffer primaryKey, Version version) {
        return versioned(primaryKey, null, version);
    }

    /**
     * Creates a new {@link TableKey} made of a Primary Key and Secondary Key, with a specific version.
     *
     * @param primaryKey   The Primary Key.
     * @param secondaryKey The Secondary Key.
     * @param version      The {@link Version}.
     * @return A {@link TableKey} made of a Primary Key and Secondary key with the specified version.
     */
    public static TableKey versioned(ByteBuffer primaryKey, ByteBuffer secondaryKey, Version version) {
        return new TableKey(primaryKey, secondaryKey, version);
    }
}
