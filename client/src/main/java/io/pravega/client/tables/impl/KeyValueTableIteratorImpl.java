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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.pravega.client.tables.IteratorItem;
import io.pravega.client.tables.KeyValueTableConfiguration;
import io.pravega.client.tables.KeyValueTableIterator;
import io.pravega.client.tables.TableEntry;
import io.pravega.client.tables.TableKey;
import io.pravega.common.util.AsyncIterator;
import java.nio.ByteBuffer;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;

/**
 * {@link KeyValueTableIterator} implementation.
 */
@RequiredArgsConstructor
@Getter
class KeyValueTableIteratorImpl implements KeyValueTableIterator {
    //region Members

    @NonNull
    private final ByteBuffer fromPrimaryKey;
    @NonNull
    private final ByteBuffer fromSecondaryKey;
    @NonNull
    private final ByteBuffer toPrimaryKey;
    @NonNull
    private final ByteBuffer toSecondaryKey;
    @Getter
    private final int maxIterationSize;

    //endregion

    //region KeyValueTableIterator Implementation

    /**
     * Gets a value indicating whether it is guaranteed that this {@link KeyValueTableIterator} applies to a single
     * segment (partition) or not.
     *
     * @return True if guaranteed to be on a single segment, false otherwise.
     */
    boolean isSingleSegment() {
        return this.fromPrimaryKey.equals(this.toPrimaryKey);
    }

    @Override
    public AsyncIterator<IteratorItem<TableKey>> keys() {
        throw new UnsupportedOperationException();
    }

    @Override
    public AsyncIterator<IteratorItem<TableEntry>> entries() {
        throw new UnsupportedOperationException();
    }

    //endregion

    //region Builder

    @RequiredArgsConstructor
    static class Builder implements KeyValueTableIterator.Builder {
        @VisibleForTesting
        static final byte MIN_BYTE = (byte) 0;
        @VisibleForTesting
        static final byte MAX_BYTE = (byte) 0xFF;
        private final KeyValueTableConfiguration kvtConfig;
        private int maxIterationSize = 10;

        @Override
        public KeyValueTableIterator.Builder maxIterationSize(int size) {
            Preconditions.checkArgument(size > 0, "size must be a positive integer");
            this.maxIterationSize = size;
            return this;
        }

        @Override
        public KeyValueTableIteratorImpl forPrimaryKey(@NonNull ByteBuffer primaryKey) {
            return forPrimaryKey(primaryKey, null, null);
        }

        @Override
        public KeyValueTableIteratorImpl forPrimaryKey(@NonNull ByteBuffer primaryKey, ByteBuffer fromSecondaryKey, ByteBuffer toSecondaryKey) {
            validateExact(primaryKey, this.kvtConfig.getPrimaryKeyLength(), "Primary Key");
            validateExact(fromSecondaryKey, this.kvtConfig.getSecondaryKeyLength(), "From Secondary Key");
            validateExact(toSecondaryKey, this.kvtConfig.getSecondaryKeyLength(), "To Secondary Key");

            // If these are null, pad() will replace them with appropriately sized buffers.
            fromSecondaryKey = pad(fromSecondaryKey, MIN_BYTE, this.kvtConfig.getSecondaryKeyLength());
            toSecondaryKey = pad(toSecondaryKey, MAX_BYTE, this.kvtConfig.getSecondaryKeyLength());
            return new KeyValueTableIteratorImpl(primaryKey, fromSecondaryKey, primaryKey, toSecondaryKey, this.maxIterationSize);
        }

        @Override
        public KeyValueTableIteratorImpl forPrimaryKey(@NonNull ByteBuffer primaryKey, ByteBuffer secondaryKeyPrefix) {
            validateExact(primaryKey, this.kvtConfig.getPrimaryKeyLength(), "Primary Key");
            validateAtMost(secondaryKeyPrefix, this.kvtConfig.getSecondaryKeyLength(), "Secondary Key Prefix");

            // If secondaryKeyPrefix is null, pad() will replace it with the effective Min/Max values, as needed.
            val fromSecondaryKey = pad(secondaryKeyPrefix, (byte) 0, this.kvtConfig.getSecondaryKeyLength());
            val toSecondaryKey = pad(secondaryKeyPrefix, (byte) 0xFF, this.kvtConfig.getSecondaryKeyLength());
            return new KeyValueTableIteratorImpl(primaryKey, fromSecondaryKey, primaryKey, toSecondaryKey, this.maxIterationSize);
        }

        @Override
        public KeyValueTableIteratorImpl forRange(ByteBuffer fromPrimaryKey, ByteBuffer toPrimaryKey) {
            validateExact(fromPrimaryKey, this.kvtConfig.getPrimaryKeyLength(), "From Primary Key");
            validateExact(toPrimaryKey, this.kvtConfig.getPrimaryKeyLength(), "To Primary Key");

            // If these are null, pad() will replace them with appropriately sized buffers.
            fromPrimaryKey = pad(fromPrimaryKey, MIN_BYTE, this.kvtConfig.getPrimaryKeyLength());
            toPrimaryKey = pad(toPrimaryKey, MAX_BYTE, this.kvtConfig.getPrimaryKeyLength());

            // SecondaryKeys must be the full range in this case, otherwise the resulting iterator will have non-contiguous ranges.
            val fromSecondaryKey = pad(null, MIN_BYTE, this.kvtConfig.getSecondaryKeyLength());
            val toSecondaryKey = pad(null, MAX_BYTE, this.kvtConfig.getSecondaryKeyLength());
            return new KeyValueTableIteratorImpl(fromPrimaryKey, fromSecondaryKey, toPrimaryKey, toSecondaryKey, this.maxIterationSize);
        }

        @Override
        public KeyValueTableIteratorImpl forPrefix(ByteBuffer primaryKeyPrefix) {
            validateAtMost(primaryKeyPrefix, this.kvtConfig.getPrimaryKeyLength(), "Primary Key Prefix");

            val fromPrimaryKey = pad(primaryKeyPrefix, MIN_BYTE, this.kvtConfig.getPrimaryKeyLength());
            val toPrimaryKey = pad(primaryKeyPrefix, MAX_BYTE, this.kvtConfig.getPrimaryKeyLength());

            // SecondaryKeys must be the full range in this case, otherwise the resulting iterator will have non-contiguous ranges.
            val fromSecondaryKey = pad(null, MIN_BYTE, this.kvtConfig.getSecondaryKeyLength());
            val toSecondaryKey = pad(null, MAX_BYTE, this.kvtConfig.getSecondaryKeyLength());
            return new KeyValueTableIteratorImpl(fromPrimaryKey, fromSecondaryKey, toPrimaryKey, toSecondaryKey, this.maxIterationSize);
        }

        @Override
        public KeyValueTableIteratorImpl all() {
            return forRange(null, null);
        }

        private ByteBuffer pad(ByteBuffer key, byte value, int size) {
            byte[] result;
            int startOffset;
            if (key == null) {
                result = new byte[size];
                startOffset = 0;
            } else {
                if (key.remaining() == size) {
                    // Already at the right size.
                    return key.duplicate();
                }
                result = new byte[size];
                startOffset = key.remaining();
                key.duplicate().get(result, 0, startOffset);
            }

            for (int i = startOffset; i < size; i++) {
                result[i] = value;
            }
            return ByteBuffer.wrap(result);
        }

        private void validateExact(@Nullable ByteBuffer key, int expected, String name) {
            if (key != null) {
                Preconditions.checkArgument(expected == key.remaining(), "%s length must be %s; given %s.", name, expected, key.remaining());
            }
        }

        private void validateAtMost(ByteBuffer key, int maxLength, String name) {
            if (key != null) {
                Preconditions.checkArgument(key.remaining() <= maxLength, "%s length must be at most %s; given %s.", name, maxLength, key.remaining());
            }
        }
    }

    //endregion
}

