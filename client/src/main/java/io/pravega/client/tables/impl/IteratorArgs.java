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
import io.pravega.client.tables.IteratorItem;
import io.pravega.client.tables.IteratorState;
import io.pravega.common.util.AsyncIterator;
import lombok.Builder;
import lombok.Data;

/**
 * Arguments to {@link TableSegment#keyIterator} and {@link TableSegment#entryIterator}.
 */
@Data
@Builder
class IteratorArgs {
    /**
     * Optional. If specified, all items returned by {@link AsyncIterator#getNext()} will have {@link TableSegmentKey}s
     * that begin with the specified prefix.
     */
    private final ByteBuf keyPrefixFilter;
    /**
     * The maximum number of items to return with each call to {@link AsyncIterator#getNext()}.
     */
    private final int maxItemsAtOnce;
    /**
     * Optional. A continuation token that can be used to resume a previously interrupted iteration. This can be obtained
     * by invoking {@link IteratorItem#getState()}. A null value will create an iterator that lists all keys.
     */
    private final IteratorState state;
}
