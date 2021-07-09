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
import io.pravega.common.ObjectClosedException;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.AsyncIterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import lombok.NonNull;

/**
 * Iterator over Table Segment Keys or Values.
 *
 * @param <T> Type of item iterated over. Usually {@link TableSegmentKey} or {@link TableSegmentEntry}.
 */
class TableSegmentIterator<T> implements AsyncIterator<IteratorItem<T>> {
    private final Function<SegmentIteratorArgs, CompletableFuture<IteratorItem<T>>> fetchNext;
    private final Function<T, ByteBuf> getKey;
    private final AtomicReference<SegmentIteratorArgs> args;

    TableSegmentIterator(@NonNull Function<SegmentIteratorArgs, CompletableFuture<IteratorItem<T>>> fetchNext,
                         @NonNull Function<T, ByteBuf> getKey, @NonNull SegmentIteratorArgs args) {
        this.fetchNext = fetchNext;
        this.getKey = getKey;
        this.args = new AtomicReference<>(args);
    }

    @Override
    public CompletableFuture<IteratorItem<T>> getNext() {
        if (this.args.get() == null) {
            // We are done.
            return CompletableFuture.completedFuture(null);
        }

        CompletableFuture<IteratorItem<T>> result = this.fetchNext.apply(this.args.get())
                .thenApply(r -> {
                    if (r == null) {
                        // We are done.
                        this.args.set(null);
                        return null;
                    }

                    ByteBuf lastKey = r.getItems().isEmpty() ? null : this.getKey.apply(r.getItems().get(r.getItems().size() - 1));
                    this.args.set(this.args.get().next(lastKey));
                    return r;
                });
        Futures.exceptionListener(result, ObjectClosedException.class, ex -> this.args.set(null));
        return result;
    }
}