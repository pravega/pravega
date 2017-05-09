/*
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.io;

import java.io.ByteArrayOutputStream;

import io.pravega.common.util.ByteArraySegment;

/**
 * A ByteArrayOutputStream that exposes the contents as a ByteArraySegment, without requiring a memory copy.
 */
public class EnhancedByteArrayOutputStream extends ByteArrayOutputStream {
    /**
     * Returns a readonly ByteArraySegment wrapping the current buffer of the ByteArrayOutputStream.
     *
     * @return A readonly ByteArraySegment from the current buffer of the ByteArrayOutputStream.
     */
    public ByteArraySegment getData() {
        return new ByteArraySegment(this.buf, 0, this.count, true);
    }
}
