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

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import javax.annotation.concurrent.NotThreadSafe;
import lombok.Getter;
import lombok.val;

@NotThreadSafe
class RandomRevisionDataOutput extends DataOutputStream implements RevisionDataOutput.CloseableRevisionDataOutput {
    @Getter
    private final OutputStream baseStream;
    private final int initialPosition;

    RandomRevisionDataOutput(OutputStream outputStream) throws IOException {
        super(outputStream);
        this.baseStream = outputStream;
        this.initialPosition = ((RandomOutputStream) outputStream).size();

        // Pre-allocate 4 bytes so we can write the length later.
        writeInt(0);
    }

    @Override
    public void close() throws IOException {
        RandomOutputStream ros = (RandomOutputStream) this.baseStream;
        int length = ros.size() - this.initialPosition;
        val lengthSubStream = new DataOutputStream(ros.subStream(this.initialPosition, Integer.BYTES));
        lengthSubStream.writeInt(length);
    }

    @Override
    public void length(int length) throws IOException {
        // Nothing to do.
    }
}
