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

import com.google.common.annotations.Beta;
import edu.umd.cs.findbugs.annotations.Nullable;
import io.pravega.common.util.AsyncIterator;
import java.nio.ByteBuffer;
import lombok.NonNull;

/**
 * Defines a preconfigured Iterator for a {@link KeyValueTable}, which can be used to iterate over {@link TableKey} or
 * {@link TableEntry} instances.
 *
 * See {@link Builder} for configuring, and {@link #keys()} and {@link #entries()} for running.
 */
@Beta
public interface KeyValueTableIterator {
    /**
     * Creates a new Iterator for {@link TableKey}s in the associated {@link KeyValueTable}. This is preferred to
     * {@link #entries()} if all that is needed is the {@link TableKey}s as less I/O is involved both server-side and
     * between the server and client.
     *
     * @return An {@link AsyncIterator} that can be used to iterate over Keys in this {@link KeyValueTable}. The returned
     * {@link AsyncIterator} guarantees the serialization of invocations to {@link AsyncIterator#getNext()}.
     * See {@link AsyncIterator#asSequential}.
     */
    AsyncIterator<IteratorItem<TableKey>> keys();

    /**
     * Creates a new Iterator over {@link TableEntry} instances in the associated {@link KeyValueTable}. This should be
     * used if both the Keys and their associated Values are needed and is preferred to using {@link #keys()} to get the
     * Keys and then issuing {@link KeyValueTable#get}/{@link KeyValueTable#getAll} to retrieve the Values.
     *
     * @return An {@link AsyncIterator} that can be used to iterate over Entries in this {@link KeyValueTable}. The
     * returned {@link AsyncIterator} guarantees the serialization of invocations to {@link AsyncIterator#getNext()}.
     * See {@link AsyncIterator#asSequential}.
     */
    AsyncIterator<IteratorItem<TableEntry>> entries();

    /**
     * Defines a Builder for a {@link KeyValueTableIterator}.
     */
    interface Builder {
        /**
         * Sets the maximum number of items to return with each call to {@link AsyncIterator#getNext()}.
         *
         * @param maxIterationSize The maximum number of items to return with each call to {@link AsyncIterator#getNext()}.
         * @return This instance.
         */
        KeyValueTableIterator.Builder maxIterationSize(int maxIterationSize);

        /**
         * Creates a new {@link KeyValueTableIterator} that returns {@link TableKey}/{@link TableEntry} instances with
         * the same Primary Key (see {@link TableKey#getPrimaryKey()}. This will iterate through all
         * {@link TableKey}/{@link TableEntry} instances matching the given Primary Key.
         *
         * @param primaryKey A {@link ByteBuffer} representing the full Primary Key to search.
         * @return A new {@link KeyValueTableIterator} that can be used for the iteration.
         */
        KeyValueTableIterator forPrimaryKey(@NonNull ByteBuffer primaryKey);

        /**
         * Creates a new {@link KeyValueTableIterator} that returns {@link TableKey}/{@link TableEntry} instances with
         * the same Primary Key (see {@link TableKey#getPrimaryKey()}. Depending on the arguments provided, this may
         * iterate through all {@link TableKey}/{@link TableEntry} instances matching the given Primary Key or a
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
         * @return A new {@link KeyValueTableIterator} that can be used for the iteration.
         */
        KeyValueTableIterator forPrimaryKey(@NonNull ByteBuffer primaryKey, @Nullable ByteBuffer fromSecondaryKey, @Nullable ByteBuffer toSecondaryKey);

        /**
         * Creates a new {@link KeyValueTableIterator} that returns {@link TableKey}/{@link TableEntry} instances with
         * the same Primary Key (see {@link TableKey#getPrimaryKey()} and all Secondary keys (see
         * {@link TableKey#getSecondaryKey()}) that begin with the given prefix. Depending on the arguments provided, this
         * may iterate through all {@link TableKey}/{@link TableEntry} instances matching the given Primary Key or a sub-range.
         *
         * @param primaryKey         A {@link ByteBuffer} representing the full Primary Key to search.
         * @param secondaryKeyPrefix A {@link ByteBuffer} that indicates the Secondary Key prefix for all {@link TableKey}/
         *                           {@link TableEntry} instances to return. If not provided (null), the iterator will list
         *                           all {@link TableKey}/{@link TableEntry} instances that match the given Primary Key.
         * @return A new {@link KeyValueTableIterator} that can be used for the iteration.
         */
        KeyValueTableIterator forPrimaryKey(@NonNull ByteBuffer primaryKey, @Nullable ByteBuffer secondaryKeyPrefix);

        /**
         * Creates a new {@link KeyValueTableIterator} that returns {@link TableKey}/{@link TableEntry} instances with
         * Primary Keys (see {@link TableKey#getPrimaryKey()} between the two values. Depending on the arguments provided,
         * this may iterate through all {@link TableKey}/{@link TableEntry} instances within the {@link KeyValueTable}
         * or a sub-range.
         *
         * @param fromPrimaryKey A {@link ByteBuffer} that indicates the Primary Key (see {@link TableKey#getPrimaryKey()})
         *                       to begin the iteration at. If not provided (null), the iterator will begin with the first
         *                       {@link TableKey}/{@link TableEntry} in the {@link KeyValueTable}. This argument is inclusive.
         * @param toPrimaryKey   A {@link ByteBuffer} that indicates the Primary Key (see {@link TableKey#getPrimaryKey()})
         *                       to end the iteration at. If not provided (null), the iterator will end with the last
         *                       {@link TableKey}/{@link TableEntry} in the {@link KeyValueTable}. This argument is inclusive.
         * @return A new {@link KeyValueTableIterator} that can be used for the iteration.
         */
        KeyValueTableIterator forRange(@Nullable ByteBuffer fromPrimaryKey, @Nullable ByteBuffer toPrimaryKey);

        /**
         * Creates a new {@link KeyValueTableIterator} that returns {@link TableKey}/{@link TableEntry} instances with
         * Primary Keys (see {@link TableKey#getPrimaryKey()} that begin with the given prefix. Depending on the arguments
         * provided, this may iterate through all {@link TableKey}/{@link TableEntry} instances within the
         * {@link KeyValueTable} or a sub-range.
         *
         * @param primaryKeyPrefix A {@link ByteBuffer} that indicates the Primary Key prefix for all {@link TableKey}/
         *                         {@link TableEntry} instances to return. If not provided (null), the iterator will list
         *                         all {@link TableKey}/{@link TableEntry} instances in the {@link KeyValueTable}.
         * @return A new {@link KeyValueTableIterator} that can be used for the iteration.
         */
        KeyValueTableIterator forPrefix(@Nullable ByteBuffer primaryKeyPrefix);

        /**
         * Creates a new {@link KeyValueTableIterator} that returns all the {@link TableKey}/{@link TableEntry} instances
         * in the associated {@link KeyValueTable}.
         *
         * @return A new {@link KeyValueTableIterator} that can be used for the iteration.
         */
        KeyValueTableIterator all();
    }
}
