package com.emc.logservice.core;

import java.io.IOException;
import java.io.OutputStream;

/**
 * OutputStream that writes to a fixed-size buffer (ByteArrayOutputStream auto-grows the buffer).
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

    public FixedByteArrayOutputStream(byte[] array, int offset, int length) {
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
