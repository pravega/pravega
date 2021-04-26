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

import java.util.Queue;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Single-priority implementation for {@link AbstractDrainingQueue}.
 *
 * All items in this queue are treated fairly (there is no priority level assigned or accepted for queue items).
 *
 * @param <T> The type of the items in the queue.
 */
@ThreadSafe
public class BlockingDrainingQueue<T> extends AbstractDrainingQueue<T> {
    //region Members

    private final SimpleDeque<T> contents;

    ///endregion

    // region Constructor

    /**
     * Creates a new instance of the BlockingDrainingQueue class.
     */
    public BlockingDrainingQueue() {
        this.contents = new SimpleDeque<>();
    }

    //endregion

    //region AbstractDrainingQueue Implementation

    @Override
    protected void addInternal(T item) {
        this.contents.addLast(item);
    }

    @Override
    protected int sizeInternal() {
        return this.contents.size();
    }

    @Override
    protected T peekInternal() {
        return this.contents.peekFirst();
    }

    @Override
    protected Queue<T> fetch(int maxCount) {
        return this.contents.pollFirst(maxCount);
    }

    // endregion
}
