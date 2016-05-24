package com.emc.logservice.core;

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
        if (array == null) {
            throw new NullPointerException("array");
        }

        if (offset < 0 || offset >= array.length) {
            throw new ArrayIndexOutOfBoundsException("offset must be non-negative and less than the size of the array.");
        }

        if (length < 0 || offset + length > array.length) {
            throw new ArrayIndexOutOfBoundsException("length must be non-negative and offset+length must be less than the size of the array.");
        }

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
