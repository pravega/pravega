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

import io.pravega.common.util.AsyncIterator;
import io.pravega.common.util.BufferView;
import java.time.Duration;
import lombok.Builder;
import lombok.Data;
import lombok.NonNull;

/**
 * Arguments for {@link TableStore#keyIterator} and {@link TableStore#entryIterator(String, IteratorArgs)}.
 */
@Data
@Builder
public class IteratorArgs {
    /**
     * (Optional) A token that indicates the current state of the iterator. This is used for Hash Table Segments.
     */
    private final BufferView continuationToken;
    /**
     * (Optional) Where the iterator should start at. This is used in conjunction with {@link #getTo()} for
     * Fixed-Key-Length Table Segments.
     */
    private final BufferView from;

    /**
     * (Optional) Where the iterator should end at. This is used in conjunction with {@link #getTo()} for
     * Fixed-Key-Length Table Segments.
     */
    private final BufferView to;
    /**
     * Timeout for each invocation to {@link AsyncIterator#getNext()}.
     */
    @NonNull
    private Duration fetchTimeout;
}
