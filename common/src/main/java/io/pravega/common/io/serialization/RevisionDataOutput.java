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

import java.io.DataOutput;
import java.io.IOException;
import java.io.OutputStream;

public interface RevisionDataOutput extends DataOutput {
    void length(int length) throws IOException;

    OutputStream getBaseStream();

    static CloseableRevisionDataOutput wrap(OutputStream outputStream) throws IOException {
        if (outputStream instanceof RandomOutputStream) {
            return new RandomRevisionDataOutput(outputStream);
        } else {
            return new NonSeekableRevisionDataOutput(outputStream);
        }
    }

    interface CloseableRevisionDataOutput extends RevisionDataOutput, AutoCloseable {
        @Override
        void close() throws IOException;
    }
}
