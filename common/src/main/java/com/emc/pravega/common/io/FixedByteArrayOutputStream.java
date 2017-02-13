/**
 *  Copyright (c) 2017 Dell Inc. or its subsidiaries. All Rights Reserved
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.emc.pravega.common.io;

import com.emc.pravega.common.Exceptions;
import com.google.common.base.Preconditions;

import java.io.IOException;
import java.io.OutputStream;

/**
 * OutputStream that writes to a fixed-size buffer (this is needed because ByteArrayOutputStream auto-grows the buffer).
 */
public class FixedByteArrayOutputStream extends OutputStream {
    //region Members

    private final byte[] array;
    private final int offset;
    private final int length;
    private int position;
    private boolean isClosed;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the FixedByteArrayOutputStream class.
     *
     * @param array  The array to wrap.
     * @param offset The offset to start the OutputStream at.
     * @param length The maximum length of the OutputStream.
     * @throws NullPointerException           If array is null.
     * @throws ArrayIndexOutOfBoundsException If offset and/or length are invalid.
     */
    public FixedByteArrayOutputStream(byte[] array, int offset, int length) {
        Preconditions.checkNotNull(array, "array");
        Exceptions.checkArrayRange(offset, length, array.length, "offset", "length");

        this.array = array;
        this.offset = offset;
        this.length = length;
        this.position = 0;
    }

    //endregion

    //region OutputStream Implementation

    @Override
    public void write(int b) throws IOException {
        if (this.isClosed) {
            throw new IOException("OutputStream is closed.");
        }

        if (this.position >= this.length) {
            throw new IOException("Buffer capacity exceeded.");
        }

        this.array[this.offset + this.position] = (byte) b;
        this.position++;
    }

    @Override
    public void close() {
        this.isClosed = true;
    }

    //endregion
}
