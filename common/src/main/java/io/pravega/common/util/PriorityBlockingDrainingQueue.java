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
import java.util.ArrayDeque;
import java.util.Queue;
import lombok.val;

/**
 * {@link AbstractDrainingQueue} implementation with multiple priority levels. Supports up to {@link Byte#MAX_VALUE}
 * priority levels, with Priority 0 being the highest.
 * <p>
 * Important notes:
 * - {@link #poll(int)} and {@link #take(int)} will return items with the highest available priority and will never mix
 * items with different priorities. That means that, even if there are more items (with lower priority), those will not
 * be included in the result even if the requested number of items exceeds what we can return. These (lower priority) items
 * may be retrieved using a subsequent call (assuming no higher priority items are added in the meantime).
 *
 * @param <T> Type of item,
 */
public class PriorityBlockingDrainingQueue<T extends PriorityBlockingDrainingQueue.Item> extends AbstractDrainingQueue<T> {
    //region Members.
    private final SimpleDeque[] queues;
    private int firstIndex;
    private int size;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the {@link PriorityBlockingDrainingQueue} class.
     *
     * @param maxPriorityValue Maximum allowed priority value.
     */
    public PriorityBlockingDrainingQueue(byte maxPriorityValue) {
        Preconditions.checkArgument(maxPriorityValue >= 0, "maxPriorityLevel must be a value between 0 and %s.", Byte.MAX_VALUE);
        this.queues = new SimpleDeque[maxPriorityValue + 1];
        this.firstIndex = 0;
        this.size = 0;
    }

    //endregion

    //region AbstractDrainingQueue Implementation

    @Override
    protected void addInternal(T item) {
        byte p = item.getPriorityValue();
        Preconditions.checkArgument(p >= 0 && p < this.queues.length,
                "Item.getPriority() must be a value between 0 (inclusive) and %s (exclusive).", this.queues.length);
        getOrCreateQueue(p).addLast(item);
        this.size++;
        if (this.firstIndex > p) {
            this.firstIndex = p;
        }
    }

    @Override
    protected int sizeInternal() {
        return this.size;
    }

    @Override
    protected T peekInternal() {
        if (this.size == 0) {
            return null;
        }

        int fi = getFirstIndex();
        assert fi >= 0 : "size !=0 but firstIndex < 0";
        return getQueue(fi).peekFirst();
    }

    @Override
    protected Queue<T> fetch(int maxCount) {
        if (this.size == 0) {
            return new ArrayDeque<>(0);
        }

        int fi = getFirstIndex();
        assert fi >= 0 : "size !=0 but firstIndex < 0";
        val q = getQueue(fi);
        val result = q.pollFirst(maxCount);
        this.size -= result.size();
        return result;

    }

    //endregion

    //region Helper methods.

    private int getFirstIndex() {
        for (; this.firstIndex < this.queues.length; this.firstIndex++) {
            val q = getQueue(this.firstIndex);
            if (q != null && !q.isEmpty()) {
                return this.firstIndex;
            }
        }

        assert this.size > 0;
        this.firstIndex = 0;
        return -1;
    }

    @SuppressWarnings("unchecked")
    private SimpleDeque<T> getQueue(int index) {
        return this.queues[index];
    }

    @SuppressWarnings("unchecked")
    private SimpleDeque<T> getOrCreateQueue(int index) {
        SimpleDeque<T> q = this.queues[index];
        if (q == null) {
            q = new SimpleDeque<>();
            this.queues[index] = q;
        }
        return q;
    }

    //endregion

    /**
     * Defines an Item that can be added to a {@link PriorityBlockingDrainingQueue}.
     */
    public interface Item {
        /**
         * Gets a value indicating the priority of this item (0 is highest priority).
         *
         * @return The priority.
         */
        byte getPriorityValue();
    }
}
