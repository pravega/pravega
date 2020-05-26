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

import io.pravega.common.hash.HashHelper;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import lombok.SneakyThrows;

/**
 * Base implementation of {@link BufferView}, providing common functionality.
 * Derived classes may override these methods with more efficient implementations tailored to their data types.
 */
public abstract class AbstractBufferView implements BufferView {
    protected static final HashHelper HASH = HashHelper.seededWith(AbstractBufferView.class.getName());
    private Integer hashCode = null;

    @Override
    public int hashCode() {
        if (this.hashCode == null) {
            HashHelper.HashBuilder builder = HASH.newBuilder();
            collect(builder::put);
            this.hashCode = builder.getAsInt();
        }
        return this.hashCode;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof BufferView) {
            return equals((BufferView) obj);
        }
        return false;
    }

    @SneakyThrows(IOException.class)
    public boolean equals(BufferView other) {
        int l = getLength();
        if (l != other.getLength()) {
            return false;
        }

        if (l > 0) {
            InputStream thisReader = getReader();
            InputStream otherReader = other.getReader();
            for (int i = 0; i < l; i++) {
                if (thisReader.read() != otherReader.read()) {
                    return false;
                }
            }
        }

        return true;
    }

    @Override
    public <ExceptionT extends Exception> void collect(Collector<ExceptionT> collectBuffer) throws ExceptionT {
        for (ByteBuffer bb : getContents()) {
            collectBuffer.accept(bb);
        }
    }
}
