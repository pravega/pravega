/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.io;

import io.pravega.common.util.BufferView;
import java.io.IOException;

/**
 * Defines an object that can accept {@link BufferView}s. This can be applied to various {@link java.io.OutputStream}s
 * in order to more efficiently make memory-to-memory buffer transfers without the need for extra buffer allocations or
 * byte-by-byte copies.
 */
public interface BufferViewSink {
    /**
     * Includes the given {@link BufferView}.
     *
     * @param buffer The {@link BufferView} to include.
     * @throws IOException If an IO Exception occurred.
     */
    void writeBuffer(BufferView buffer) throws IOException;
}
