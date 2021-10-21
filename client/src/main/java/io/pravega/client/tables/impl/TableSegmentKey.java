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
package io.pravega.client.tables.impl;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

/**
 * Key for a {@link TableSegment}.
 */
@Getter
@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
public class TableSegmentKey {
    /**
     * A {@link ByteBuf} representing the contents of this Key.
     */
    @NonNull
    private final ByteBuf key;
    /**
     * A {@link  TableSegmentKeyVersion} representing the Version of this Key.
     */
    @NonNull
    private final TableSegmentKeyVersion version;

    //region Constructors

    /**
     * Creates a new {@link TableSegmentKey} with no specific version.
     *
     * @param key A {@link ByteBuf} representing the key.
     * @return An unversioned {@link TableSegmentKey}.
     */
    public static TableSegmentKey unversioned(ByteBuf key) {
        return new TableSegmentKey(key, TableSegmentKeyVersion.NO_VERSION);
    }

    /**
     * Creates a new {@link TableSegmentKey} with no specific version.
     *
     * @param key A byte array representing the key. This will be wrapped in a {@link ByteBuf}.
     * @return An unversioned {@link TableSegmentKey}.
     */
    public static TableSegmentKey unversioned(byte[] key) {
        return unversioned(Unpooled.wrappedBuffer(key));
    }

    /**
     * Creates a new {@link TableSegmentKey} with a specific version.
     *
     * @param key     A {@link ByteBuf} representing the key.
     * @param version The version to set.
     * @return An versioned {@link TableSegmentKey}.
     */
    public static TableSegmentKey versioned(ByteBuf key, long version) {
        return new TableSegmentKey(key, TableSegmentKeyVersion.from(version));
    }

    /**
     * Creates a new {@link TableSegmentKey} with a specific version.
     *
     * @param key     A byte array representing the key. This will be wrapped in a {@link ByteBuf}.
     * @param version The version to set.
     * @return An versioned {@link TableSegmentKey}.
     */
    public static TableSegmentKey versioned(byte[] key, long version) {
        return versioned(Unpooled.wrappedBuffer(key), version);
    }

    /**
     * Creates a new {@link TableSegmentKey} with a version that indicates the key must not exist.
     *
     * @param key A {@link ByteBuf} representing the key.
     * @return An {@link TableSegmentKey} with a version set to {@link TableSegmentKeyVersion#NOT_EXISTS}.
     */
    public static TableSegmentKey notExists(ByteBuf key) {
        return new TableSegmentKey(key, TableSegmentKeyVersion.NOT_EXISTS);
    }

    /**
     * Creates a new {@link TableSegmentKey} with a version that indicates the key must not exist.
     *
     * @param key A byte array representing the key. This will be wrapped in a {@link ByteBuf}.
     * @return An {@link TableSegmentKey} with a version set to {@link TableSegmentKeyVersion#NOT_EXISTS}.
     */
    public static TableSegmentKey notExists(byte[] key) {
        return notExists(Unpooled.wrappedBuffer(key));
    }

    //endregion

    /**
     * Gets a value indicating whether this key exists/should exist.
     *
     * @return True if the key exists/should exist, false otherwise.
     */
    boolean exists() {
        return getVersion().getSegmentVersion() != TableSegmentKeyVersion.NOT_EXISTS.getSegmentVersion();
    }
}
