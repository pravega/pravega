/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.util;

import java.util.ArrayDeque;
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

    private final ArrayDeque<T> contents;

    ///endregion

    // region Constructor

    /**
     * Creates a new instance of the BlockingDrainingQueue class.
     */
    public BlockingDrainingQueue() {
        this.contents = new ArrayDeque<>();
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
        int count = Math.min(maxCount, this.contents.size());
        ArrayDeque<T> result = new ArrayDeque<>(count);
        while (result.size() < count) {
            result.addLast(this.contents.pollFirst());
        }
        return result;
    }

    // endregion
}
