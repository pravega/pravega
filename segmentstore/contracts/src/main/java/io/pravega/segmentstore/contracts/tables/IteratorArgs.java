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

import com.google.common.annotations.Beta;
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
     * EXPERIMENTAL!
     * (Optional) A filter to apply to all returned Iterator Entries. If specified, only those entries whose keys begin
     * with this prefix will be included.
     * This option only applies to Sorted Table Segments (see {@link TableStore}. An attempt to use it on a non-Sorted
     * Table Segment will result in an {@link IllegalArgumentException}.
     */
    @Beta
    private final BufferView prefixFilter;
    /**
     * (Optional) The serialized form of the State. This can be obtained from {@link IteratorItem#getState()}.
     * If provided, the iteration will resume from where it left off, otherwise it will start from the beginning.
     */
    private BufferView serializedState;
    /**
     * Timeout for each invocation to {@link AsyncIterator#getNext()}.
     */
    @NonNull
    private Duration fetchTimeout;
}
