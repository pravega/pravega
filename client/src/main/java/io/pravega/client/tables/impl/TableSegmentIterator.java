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

import io.pravega.client.tables.IteratorItem;
import io.pravega.client.tables.IteratorState;
import io.pravega.common.ObjectClosedException;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.AsyncIterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import lombok.NonNull;

/**
 * Iterator over Table Segment Keys or Values.
 *
 * @param <T> Type of item iterated over. Usually {@link TableSegmentKey} or {@link TableSegmentEntry}.
 */
class TableSegmentIterator<T> implements AsyncIterator<IteratorItem<T>> {
    private final Function<IteratorState, CompletableFuture<IteratorItem<T>>> fetchNext;
    private final AtomicReference<IteratorState> state;
    private final AtomicBoolean closed;

    TableSegmentIterator(@NonNull Function<IteratorState, CompletableFuture<IteratorItem<T>>> fetchNext, IteratorState initialState) {
        this.fetchNext = fetchNext;
        this.state = new AtomicReference<>(initialState);
        this.closed = new AtomicBoolean(false);
    }

    @Override
    public CompletableFuture<IteratorItem<T>> getNext() {
        if (this.closed.get()) {
            // We are done.
            return CompletableFuture.completedFuture(null);
        }

        CompletableFuture<IteratorItem<T>> result = this.fetchNext.apply(this.state.get())
                .thenApply(r -> {
                    if (r == null) {
                        // We are done.
                        this.state.set(null);
                        this.closed.set(true);
                        return null;
                    }

                    this.state.set(r.getState());
                    return r;
                });
        Futures.exceptionListener(result, ObjectClosedException.class, ex -> this.closed.set(true));
        return result;
    }
}