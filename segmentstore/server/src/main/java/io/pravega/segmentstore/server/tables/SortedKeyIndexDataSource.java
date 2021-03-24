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
package io.pravega.segmentstore.server.tables;

import io.pravega.common.util.BufferView;
import io.pravega.segmentstore.contracts.tables.TableEntry;
import io.pravega.segmentstore.contracts.tables.TableKey;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.Data;
import lombok.NonNull;

/**
 * Data source for {@link ContainerSortedKeyIndex} and {@link SegmentSortedKeyIndexImpl}.
 */
@Data
class SortedKeyIndexDataSource {
    static final KeyTranslator EXTERNAL_TRANSLATOR = KeyTranslator.partitioned((byte) 'E');
    static final KeyTranslator INTERNAL_TRANSLATOR = KeyTranslator.partitioned((byte) 'I');

    /**
     * A Function that will be invoked when a Table Segment's Sorted Key Index nodes need to be persisted.
     */
    @NonNull
    private final Update update;
    /**
     * A Function that will be invoked when a Table Segment's Sorted Key Index nodes need to be deleted.
     */
    @NonNull
    private final Delete delete;
    /**
     * A Function that will be invoked when a Table Segment's Sorted Key Index nodes need to be retrieved.
     */
    @NonNull
    private final Read read;

    /**
     * Gets a value indicating whether the given Key should be excluded from indexing or not.
     *
     * @param keyContents The Key contents.
     * @return True if the key is an internal key (exclude), false otherwise.
     */
    boolean isKeyExcluded(BufferView keyContents) {
        return INTERNAL_TRANSLATOR.isInternal(keyContents);
    }

    @FunctionalInterface
    public interface Update {
        CompletableFuture<?> apply(String segmentName, List<TableEntry> entries, Duration timeout);
    }

    @FunctionalInterface
    public interface Delete {
        CompletableFuture<?> apply(String segmentName, Collection<TableKey> keys, Duration timeout);
    }

    @FunctionalInterface
    public interface Read {
        CompletableFuture<List<TableEntry>> apply(String segmentName, List<BufferView> keys, Duration timeout);
    }
}
