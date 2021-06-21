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

import com.google.common.base.Preconditions;
import io.pravega.common.util.BufferView;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

/**
 * Represents a Key in a Table Segment, with optional version.
 */
@Getter
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class TableKey {
    /**
     * Version value that indicates no versioning is desired.
     */
    public static final long NO_VERSION = Long.MIN_VALUE;

    /**
     * Version value that indicates the Key must not previously exist.
     */
    public static final long NOT_EXISTS = -1L;

    /**
     * The Key.
     */
    private final BufferView key;

    /**
     * The Version of the Key.
     */
    private final long version;

    /**
     * Creates a new instance of the TableKey class with no desired version.
     *
     * @param key The Key.
     *
     * @return new TableKey instance that is unversioned
     */
    public static TableKey unversioned(@NonNull BufferView key) {
        return new TableKey(key, NO_VERSION);
    }

    /**
     * Creates a new instance of the TableKey class that indicates the Key must not previously exist.
     *
     * @param key The Key.
     *
     * @return new instance of Table Key as long as key does not already exist
     *
     */
    public static TableKey notExists(@NonNull BufferView key) {
        return new TableKey(key, NOT_EXISTS);
    }

    /**
     * Creates a new instance of the TableKey class with a specified version.
     *
     * @param key     The Key.
     * @param version The desired version.
     *
     * @return new TableKey with specified version
     */
    public static TableKey versioned(@NonNull BufferView key, long version) {
        Preconditions.checkArgument(version >= 0 || version == NOT_EXISTS || version == NO_VERSION, "Version must be a non-negative number.");
        return new TableKey(key, version);
    }

    /**
     * Gets a value indicating whether this TableKey has a Version defined.
     *
     * @return True if a version is defined, false otherwise. If False, the result of getVersion() is undefined.
     */
    public boolean hasVersion() {
        return this.version != NO_VERSION;
    }

    @Override
    public String toString() {
        return String.format("{%s} %s", hasVersion() ? this.version : "*", this.key);
    }


    @Override
    public int hashCode() {
        return this.key.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof TableKey) {
            TableKey other = (TableKey) obj;
            return this.version == other.version && this.key.equals(other.key);

        }

        return false;
    }

}
