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
 * Entry in a {@link TableSegment}.
 */
@Getter
@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
public class TableSegmentEntry {
    /**
     * A {@link TableSegmentKey} representing the Key of this Table Entry.
     */
    @NonNull
    private final TableSegmentKey key;
    /**
     * A {@link ByteBuf} representing the contents (value) of this Table Entry. May be null if
     * {@link #getKey()} indicates that it does not exist.
     */
    private final ByteBuf value;

    //region Constructors

    /**
     * Creates a new {@link TableSegmentEntry} with no specific version. When used with {@link TableSegment#put}, this
     * {@link TableSegmentEntry} will be treated as an unconditional update.
     *
     * @param key   A {@link ByteBuf} representing the contents of the Table Entry Key.
     * @param value A {@link ByteBuf} representing the Table Entry Value.
     * @return An unversioned {@link TableSegmentEntry}.
     */
    public static TableSegmentEntry unversioned(ByteBuf key, ByteBuf value) {
        return new TableSegmentEntry(new TableSegmentKey(key, TableSegmentKeyVersion.NO_VERSION), value);
    }

    /**
     * Creates a new {@link TableSegmentEntry} with no specific version. When used with {@link TableSegment#put}, this
     * {@link TableSegmentEntry} will be treated as an unconditional update.
     *
     * @param key   A byte array representing the contents of the Table Key. This will be wrapped in a {@link ByteBuf}.
     * @param value A byte array representing the contents Table Entry Value. This will be wrapped in a {@link ByteBuf}.
     * @return An unversioned {@link TableSegmentEntry}.
     */
    public static TableSegmentEntry unversioned(byte[] key, byte[] value) {
        return unversioned(Unpooled.wrappedBuffer(key), Unpooled.wrappedBuffer(value));
    }

    /**
     * Creates a new {@link TableSegmentEntry} with a specific version. When used with {@link TableSegment#put}, this
     * {@link TableSegmentEntry} will be treated as a conditional update, conditioned on the key existing and having the
     * specified version.
     *
     * @param key     A {@link ByteBuf} representing the contents of the Table Entry Key.
     * @param value   A {@link ByteBuf} representing the Table Entry Value.
     * @param version The version to set. Consider using {@link #unversioned}, {@link #notExists} or {@link #notFound}
     *                for special-case versions.
     * @return A versioned {@link TableSegmentEntry}.
     */
    public static TableSegmentEntry versioned(ByteBuf key, ByteBuf value, long version) {
        return new TableSegmentEntry(new TableSegmentKey(key, TableSegmentKeyVersion.from(version)), value);
    }

    /**
     * Creates a new {@link TableSegmentEntry} with a specific version. When used with {@link TableSegment#put}, this
     * {@link TableSegmentEntry} will be treated as a conditional update, conditioned on the key existing and having the
     * specified version.
     *
     * @param key     A byte array representing the contents of the Table Key. This will be wrapped in a {@link ByteBuf}.
     * @param value   A byte array representing the contents Table Entry Value. This will be wrapped in a {@link ByteBuf}.
     * @param version The version to set. Consider using {@link #unversioned}, {@link #notExists} or {@link #notFound}
     *                for special-case versions.
     * @return A versioned {@link TableSegmentEntry}.
     */
    public static TableSegmentEntry versioned(byte[] key, byte[] value, long version) {
        return versioned(Unpooled.wrappedBuffer(key), Unpooled.wrappedBuffer(value), version);
    }

    /**
     * Creates a new {@link TableSegmentEntry} with a version that indicates the key must not exist. When used with
     * {@link TableSegment#put}, this {@link TableSegmentEntry} will be treated as a conditional update, conditioned on
     * the key not existing.
     *
     * @param key   A {@link ByteBuf} representing the contents of the Table Entry Key.
     * @param value A {@link ByteBuf} representing the Table Entry Value.
     * @return A {@link TableSegmentEntry} with a version set to {@link TableSegmentKeyVersion#NOT_EXISTS}.
     */
    public static TableSegmentEntry notExists(ByteBuf key, ByteBuf value) {
        return new TableSegmentEntry(new TableSegmentKey(key, TableSegmentKeyVersion.NOT_EXISTS), value);
    }

    /**
     * Creates a new {@link TableSegmentEntry} with a version that indicates the key must not exist. When used with
     * {@link TableSegment#put}, this {@link TableSegmentEntry} will be treated as a conditional update, conditioned on
     * the key not existing.
     *
     * @param key   A byte array representing the contents of the Table Key. This will be wrapped in a {@link ByteBuf}.
     * @param value A byte array representing the contents Table Entry Value. This will be wrapped in a {@link ByteBuf}.
     * @return A {@link TableSegmentEntry} with a version set to {@link TableSegmentKeyVersion#NOT_EXISTS}.
     */
    public static TableSegmentEntry notExists(byte[] key, byte[] value) {
        return notExists(Unpooled.wrappedBuffer(key), Unpooled.wrappedBuffer(value));
    }

    /**
     * Creates a new {@link TableSegmentEntry} for a response that indicates it does not exist. This is usually invoked
     * internally from the {@link TableSegment} implementation to construct the response.
     *
     * This {@link TableSegmentEntry} should not be used as an argument for {@link TableSegment#put} calls.
     *
     * @param key A {@link ByteBuf} representing the contents of the Table Entry Key.
     * @return A {@link TableSegmentEntry} with a version set to {@link TableSegmentKeyVersion#NOT_EXISTS} and a
     * {@link #getValue()} set to null.
     */
    public static TableSegmentEntry notFound(ByteBuf key) {
        return new TableSegmentEntry(TableSegmentKey.notExists(key), null);
    }

    //endregion
}
