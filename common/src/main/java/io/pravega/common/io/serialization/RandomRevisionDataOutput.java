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
 * A RevisionDataOutput implementation that wraps a Seekable OutputStream. A Seekable OutputStream is an OutputStream
 * that supports writing a value at an arbitrary offset within it.
 */
@NotThreadSafe
class RandomRevisionDataOutput extends DataOutputStream implements RevisionDataOutput.CloseableRevisionDataOutput {
    //region Members

    @Getter
    private final OutputStream baseStream;
    private final int initialPosition;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the RandomRevisionDataOutput class. Upon a successful call to this constructor, 4 bytes
     * will have been written to the OutputStream representing a placeholder for the length. These 4 bytes will be populated
     * upon closing this OutputStream.
     *
     * @param outputStream The OutputStream to wrap.
     * @throws IOException If an IO Exception occurred.
     */
    RandomRevisionDataOutput(OutputStream outputStream) throws IOException {
        super(outputStream);
        this.baseStream = outputStream;

        // Pre-allocate 4 bytes so we can write the length later, but remember this position.
        this.initialPosition = ((RandomOutput) outputStream).size();
        BitConverter.writeInt(outputStream, 0);
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() throws IOException {
        // Calculate the number of bytes written, making sure to exclude the bytes for the length encoding.
        RandomOutput ros = (RandomOutput) this.baseStream;
        int length = ros.size() - this.initialPosition - Integer.BYTES;

        // Write the length at the appropriate position.
        BitConverter.writeInt(ros.subStream(this.initialPosition, Integer.BYTES), length);
    }

    //endregion

    //region RevisionDataOutput Implementation

    @Override
    public boolean requiresExplicitLength() {
        return false;
    }

    @Override
    public void length(int length) throws IOException {
        // Nothing to do.
    }

    //endregion
}
