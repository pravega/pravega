/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.io;

import com.google.common.base.Preconditions;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import lombok.Getter;

public class BoundedInputStream extends FilterInputStream {
    @Getter
    private final int bound;
    @Getter
    private int remaining;

    public BoundedInputStream(InputStream inputStream, int bound) {
        super(inputStream);
        this.bound = bound;
        this.remaining = bound;
    }

    @Override
    public void close() throws IOException {
        // Skip over the remaining bytes. Do not close the underlying InputStream.
        if (this.remaining > 0) {
            int toSkip = this.remaining;
            long skipped = skip(toSkip);
            if (skipped != toSkip) {
                throw new SerializationException(String.format("Read %d fewer byte(s) than expected only able to skip %d.", toSkip, skipped));
            }
        } else if (this.remaining < 0) {
            throw new SerializationException(String.format("Read more bytes than expected (%d).", -this.remaining));
        }
    }

    public BoundedInputStream subStream(int length) {
        Preconditions.checkArgument(length <= this.remaining, "Too large.");
        this.remaining -= length;
        return new BoundedInputStream(this.in, length);
    }

    @Override
    public int read() throws IOException {
        // Do not allow reading more than we should.
        int r = this.remaining > 0 ? super.read() : -1;
        if (r >= 0) {
            this.remaining--;
        }

        return r;
    }

    @Override
    public int read(byte[] buffer, int offset, int length) throws IOException {
        int readLength = Math.min(length, this.remaining);
        int r = this.in.read(buffer, offset, readLength);
        if (r > 0) {
            this.remaining -= r;
        } else if (length > 0 && this.remaining <= 0) {
            // We have reached our bound.
            return -1;
        }
        return r;
    }

    @Override
    public long skip(long count) throws IOException {
        long r = this.in.skip(Math.min(count, this.remaining));
        this.remaining -= r;
        return r;
    }

    @Override
    public int available() throws IOException {
        return Math.min(this.in.available(), this.remaining);
    }
}
