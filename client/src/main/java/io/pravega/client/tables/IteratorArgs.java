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

import edu.umd.cs.findbugs.annotations.Nullable;
import java.nio.ByteBuffer;
import lombok.NonNull;

/**
 * Defines a generic argument for a {@link KeyValueTable} iterator ({@link KeyValueTable#keyIterator} or
 * {@link KeyValueTable#entryIterator}).
 */
public interface IteratorArgs {
    /**
     * Serializes this {@link IteratorArgs} to a {@link ByteBuffer}. This can later be used to create a new
     * {@link IteratorArgs} using {@link #fromBytes(ByteBuffer)}.
     *
     * @return A {@link ByteBuffer}.
     */
    ByteBuffer toBytes();

    /**
     * Creates a new {@link IteratorArgs} from the given {@link ByteBuffer}.
     *
     * @param buffer A {@link ByteBuffer} created via {@link #toBytes()}.
     * @return A {@link IteratorArgs}.
     */
    static IteratorArgs fromBytes(ByteBuffer buffer) {
        throw new UnsupportedOperationException("serialization not implemented yet");
    }

    /**
     * Creates a new {@link IteratorArgs} instance for an iterator that returns {@link TableKey}/{@link TableEntry}
     * instances with the same Primary Key (see {@link TableKey#getPrimaryKey()}. Depending on the arguments provided,
     * this may iterate through all {@link TableKey}/{@link TableEntry} instances matching the given Primary Key or a
     * sub-range.
     *
     * @param primaryKey       A {@link ByteBuffer} representing the full Primary Key to search.
     * @param fromSecondaryKey A {@link ByteBuffer} that indicates the Secondary Key (see {@link TableKey#getSecondaryKey()})
     *                         to begin the iteration at. If not provided (null), the iterator will begin with the first
     *                         {@link TableKey}/{@link TableEntry} that has the given Primary Key. This argument is
     *                         inclusive.
     * @param toSecondaryKey   A {@link ByteBuffer} that indicates the Secondary Key (see {@link TableKey#getSecondaryKey()})
     *                         to end the iteration at. If not provided (null), the iterator will end with the last
     *                         {@link TableKey}/{@link TableEntry} that has the given Primary Key. This argument is
     *                         inclusive.
     * @return A new {@link IteratorArgs} that can be passed to {@link KeyValueTable#keyIterator} or to
     * {@link KeyValueTable#entryIterator}.
     */
    static IteratorArgs forPrimaryKey(@NonNull ByteBuffer primaryKey, @Nullable ByteBuffer fromSecondaryKey, @Nullable ByteBuffer toSecondaryKey) {
        throw new UnsupportedOperationException("PrimaryKey range iterators not supported.");
    }

    /**
     * Creates a new {@link IteratorArgs} instance for an iterator that returns {@link TableKey}/{@link TableEntry}
     * instances with the same Primary Key (see {@link TableKey#getPrimaryKey()} and all Secondary keys (see
     * {@link TableKey#getSecondaryKey()}) that begin with the given prefix. Depending on the arguments provided, this
     * may iterate through all {@link TableKey}/{@link TableEntry} instances matching the given Primary Key or a sub-range.
     *
     * @param primaryKey         A {@link ByteBuffer} representing the full Primary Key to search.
     * @param secondaryKeyPrefix A {@link ByteBuffer} that indicates the Secondary Key prefix for all {@link TableKey}/
     *                           {@link TableEntry} instances to return. If not provided (null), the iterator will list
     *                           all {@link TableKey}/{@link TableEntry} instances that match the given Primary Key.
     * @return A new {@link IteratorArgs} that can be passed to {@link KeyValueTable#keyIterator} or to
     * {@link KeyValueTable#entryIterator}.
     */
    static IteratorArgs forPrimaryKey(@NonNull ByteBuffer primaryKey, @Nullable ByteBuffer secondaryKeyPrefix) {
        throw new UnsupportedOperationException("PrimaryKey prefix iterators not supported.");
    }

    /**
     * Creates a new {@link IteratorArgs} instance for an iterator that returns {@link TableKey}/{@link TableEntry}
     * instances with Primary Keys (see {@link TableKey#getPrimaryKey()} between the two values. Depending on the arguments
     * provided, this may iterate through all {@link TableKey}/{@link TableEntry} instances within the {@link KeyValueTable}
     * or a sub-range.
     *
     * @param fromPrimaryKey A {@link ByteBuffer} that indicates the Primary Key (see {@link TableKey#getPrimaryKey()})
     *                       to begin the iteration at. If not provided (null), the iterator will begin with the first
     *                       {@link TableKey}/{@link TableEntry} in the {@link KeyValueTable}. This argument is inclusive.
     * @param toPrimaryKey   A {@link ByteBuffer} that indicates the Primary Key (see {@link TableKey#getPrimaryKey()})
     *                       to end the iteration at. If not provided (null), the iterator will end with the last
     *                       {@link TableKey}/{@link TableEntry} in the {@link KeyValueTable}. This argument is inclusive.
     * @return A new {@link IteratorArgs} that can be passed to {@link KeyValueTable#keyIterator} or to
     * {@link KeyValueTable#entryIterator}.
     */
    static IteratorArgs forRange(@Nullable ByteBuffer fromPrimaryKey, @Nullable ByteBuffer toPrimaryKey) {
        throw new UnsupportedOperationException("Global iterators not supported.");
    }

    /**
     * Creates a new {@link IteratorArgs} instance for an iterator that returns {@link TableKey}/{@link TableEntry}
     * instances with Primary Keys (see {@link TableKey#getPrimaryKey()} that begin with the given prefix. Depending on
     * the arguments provided, this may iterate through all {@link TableKey}/{@link TableEntry} instances within the
     * {@link KeyValueTable} or a sub-range.
     *
     * @param primaryKeyPrefix A {@link ByteBuffer} that indicates the Primary Key prefix for all {@link TableKey}/
     *                         {@link TableEntry} instances to return. If not provided (null), the iterator will list
     *                         all {@link TableKey}/{@link TableEntry} instances in the {@link KeyValueTable}.
     * @return A new {@link IteratorArgs} that can be passed to {@link KeyValueTable#keyIterator} or to
     * {@link KeyValueTable#entryIterator}.
     */
    static IteratorArgs forPrefix(@Nullable ByteBuffer primaryKeyPrefix) {
        throw new UnsupportedOperationException("Global iterators not supported.");
    }
}
