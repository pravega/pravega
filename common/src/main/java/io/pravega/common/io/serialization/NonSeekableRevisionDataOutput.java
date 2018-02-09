/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.io.serialization;

import io.pravega.common.util.BitConverter;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import javax.annotation.concurrent.NotThreadSafe;
import lombok.Getter;

/**
 * A RevisionDataOutput implementation that wraps a Non-Seekable OutputStream. A Non-Seekable OutputStream is an OutputStream
 * that only supports "append semantics", which means it cannot write a value at an arbitrary position.
 */
@NotThreadSafe
class NonSeekableRevisionDataOutput implements RevisionDataOutput.CloseableRevisionDataOutput {
    //region Private

    @Getter
    private final OutputStream baseStream;
    private final DataOutputStream dataOutputStream;
    private boolean lengthWritten;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the NonSeekableRevisionDataOutput class.
     *
     * @param outputStream The OutputStream to wrap.
     */
    NonSeekableRevisionDataOutput(OutputStream outputStream) {
        this.baseStream = outputStream;
        this.dataOutputStream = new DataOutputStream(this.baseStream);
        this.lengthWritten = false;
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() throws IOException {
        if (!this.lengthWritten) {
            // Ensure that we write a length even we haven't written anything.
            length(0);
        }
    }

    //endregion

    //region RevisionDataOutput Implementation

    @Override
    public boolean requiresExplicitLength() {
        return true;
    }

    @Override
    public void length(int length) throws IOException {
        if (!this.lengthWritten) {
            BitConverter.writeInt(this.baseStream, length);
            this.lengthWritten = true;
        }
    }

    //endregion

    //region DataOutput Implementation

    @Override
    public void write(int i) throws IOException {
        ensureLengthWritten();
        this.dataOutputStream.write(i);
    }

    @Override
    public void write(byte[] bytes) throws IOException {
        ensureLengthWritten();
        this.dataOutputStream.write(bytes);
    }

    @Override
    public void write(byte[] bytes, int i, int i1) throws IOException {
        ensureLengthWritten();
        this.dataOutputStream.write(bytes, i, i1);
    }

    @Override
    public void writeBoolean(boolean b) throws IOException {
        ensureLengthWritten();
        this.dataOutputStream.writeBoolean(b);
    }

    @Override
    public void writeByte(int i) throws IOException {
        ensureLengthWritten();
        this.dataOutputStream.writeByte(i);
    }

    @Override
    public void writeShort(int i) throws IOException {
        ensureLengthWritten();
        this.dataOutputStream.writeShort(i);
    }

    @Override
    public void writeChar(int i) throws IOException {
        ensureLengthWritten();
        this.dataOutputStream.writeChar(i);
    }

    @Override
    public void writeInt(int i) throws IOException {
        ensureLengthWritten();
        this.dataOutputStream.writeInt(i);
    }

    @Override
    public void writeLong(long l) throws IOException {
        ensureLengthWritten();
        this.dataOutputStream.writeLong(l);
    }

    @Override
    public void writeFloat(float v) throws IOException {
        ensureLengthWritten();
        this.dataOutputStream.writeFloat(v);
    }

    @Override
    public void writeDouble(double v) throws IOException {
        ensureLengthWritten();
        this.dataOutputStream.writeDouble(v);
    }

    @Override
    public void writeBytes(String s) throws IOException {
        ensureLengthWritten();
        this.dataOutputStream.writeBytes(s);
    }

    @Override
    public void writeChars(String s) throws IOException {
        ensureLengthWritten();
        this.dataOutputStream.writeChars(s);
    }

    @Override
    public void writeUTF(String s) throws IOException {
        ensureLengthWritten();
        this.dataOutputStream.writeUTF(s);
    }

    private void ensureLengthWritten() {
        if (!this.lengthWritten) {
            throw new IllegalStateException("Length must be declared prior to writing anything.");
        }
    }

    //endregion
}
