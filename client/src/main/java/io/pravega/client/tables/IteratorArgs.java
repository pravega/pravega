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

    // All keys with same PK. Optional sub-ranges within those PK.
    static IteratorArgs forPrimaryKey(@NonNull ByteBuffer primaryKey, @Nullable ByteBuffer fromSecondaryKey, @Nullable ByteBuffer toSecondaryKey) {
        throw new UnsupportedOperationException("PrimaryKey range iterators not supported.");
    }

    static IteratorArgs forPrimaryKey(@NonNull ByteBuffer primaryKey, @Nullable ByteBuffer secondaryKeyPrefix) {
        throw new UnsupportedOperationException("PrimaryKey prefix iterators not supported.");
    }

    // Range iterator between 2 PKs (Optional)
    static IteratorArgs forRange(@Nullable ByteBuffer fromPrimaryKey, @Nullable ByteBuffer toPrimaryKey) {
        throw new UnsupportedOperationException("Global iterators not supported.");
    }

    // Prefix iterator for all keys with given prefix for primary key (Optional)
    static IteratorArgs forPrefix(@Nullable ByteBuffer primaryKeyPrefix) {
        throw new UnsupportedOperationException("Global iterators not supported.");
    }
}
