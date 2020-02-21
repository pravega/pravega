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

public interface CompositeArrayView extends BufferView {

    /**
     * Gets the value at the specified index.
     *
     * @param index The index to query.
     * @return Byte indicating the value at the given index.
     * @throws ArrayIndexOutOfBoundsException If index is invalid.
     */
    byte get(int index);

    void set(int offset, byte value);

    void copyFrom(ArrayView source, int targetOffset, int length);

    @Override
    CompositeArrayView slice(int offset, int length);

    void collect(Collector collectArray);

    @FunctionalInterface
    interface Collector {
        void accept(byte[] array, int arrayOffset, int length);
    }
}
