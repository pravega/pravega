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
package io.pravega.common.util;

import com.google.common.base.Preconditions;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.BiFunction;
import javax.annotation.concurrent.NotThreadSafe;
import lombok.NonNull;

/**
 * Fetches items in batches and presents them in the form of an {@link Iterator}. Useful for cases when items need to be
 * processed in sequence but there are not enough resources to load all of them at once in memory.
 *
 * @param <T> Type of the items.
 */
@NotThreadSafe
public class BufferedIterator<T> implements Iterator<T> {
    //region Members

    private final BiFunction<Long, Long, Iterator<T>> getItemRange;
    private final long firstIndex;
    private final long lastIndex;
    private final int batchSize;
    private long nextIndex;
    private Iterator<T> currentEntries;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the {@link BufferedIterator} class.
     *
     * @param getItemRange A {@link BiFunction} that returns a range of items that will be buffered next. The first
     *                     argument is the first index to fetch (inclusive) and the second argument is the last index to
     *                     fetch (inclusive). This method will always be invoked in sequence with non-overlapping ranges.
     *                     This method will never be passed invalid args {@literal (Arg1 > Arg2)} and it should never return an empty
     *                     iterator.
     * @param firstIndex   The index of the first item to return (inclusive).
     * @param lastIndex    The index of the last item to return (inclusive).
     * @param batchSize    The size of the batch (number of items to fetch at once).
     */
    public BufferedIterator(@NonNull BiFunction<Long, Long, Iterator<T>> getItemRange, long firstIndex, long lastIndex, int batchSize) {
        Preconditions.checkArgument(batchSize > 0, "batchSize must be a positive integer.");
        this.getItemRange = getItemRange;
        this.firstIndex = firstIndex;
        this.lastIndex = lastIndex;
        this.batchSize = batchSize;
        this.nextIndex = firstIndex;
        this.currentEntries = null;
    }

    //endregion

    //region Iterator Implementation

    @Override
    public boolean hasNext() {
        return this.currentEntries != null || this.nextIndex <= this.lastIndex;
    }

    @Override
    public T next() {
        while (hasNext()) {
            if (this.currentEntries != null && this.currentEntries.hasNext()) {
                // We have an active set of entries to read from.
                T result = this.currentEntries.next();
                if (!this.currentEntries.hasNext()) {
                    // After returning this entry, we're done with this set.
                    this.currentEntries = null;
                }

                return result;
            }

            // Fetch the next set of entries to read from.
            long readFrom = this.nextIndex;
            long readTo = Math.min(readFrom + this.batchSize, this.lastIndex);
            this.nextIndex = readTo + 1;
            assert readFrom <= readTo;
            this.currentEntries = this.getItemRange.apply(readFrom, readTo);
            Preconditions.checkState(this.currentEntries.hasNext(), "getItemRange returned empty iterator.");
        }

        throw new NoSuchElementException();
    }

    //endregion
}
