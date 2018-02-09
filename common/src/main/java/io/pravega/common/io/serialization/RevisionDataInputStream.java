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

import com.google.common.annotations.VisibleForTesting;
import io.pravega.common.io.SerializationException;
import io.pravega.common.util.BitConverter;
import java.io.DataInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * A DataInputStream that is used for deserializing Serialization Revisions. Instances of this class should be used to
 * read data that was serialized using an instance of RevisionDataOutput (i.e., NonSeekableRevisionDataOutput or
 * RandomRevisionDataOutput).
 */
class RevisionDataInputStream extends DataInputStream implements RevisionDataInput {
    //region Constructor

    /**
     * Creates a new instance of the RevisionDataInputStream class.
     *
     * @param inputStream The InputStream to wrap.
     */
    private RevisionDataInputStream(BoundedInputStream inputStream) {
        super(inputStream);
    }

    /**
     * Creates a new instance of the RevisionDataInputStream class. Upon a successful call to this method, 4 bytes
     * will have been read from the InputStream representing the expected length of the serialization.
     *
     * @param inputStream The InputStream to wrap.
     * @throws IOException If an IO Exception occurred.
     */
    static RevisionDataInputStream wrap(InputStream inputStream) throws IOException {
        int bound = BitConverter.readInt(inputStream);
        return new RevisionDataInputStream(new BoundedInputStream(inputStream, bound));
    }

    //endregion

    //region Properties

    @Override
    public InputStream getBaseStream() {
        return this;
    }

    /**
     * Gets a value representing the length of this InputStream, in bytes, excluding the 4 bytes required for encoding
     * the length.
     */
    @VisibleForTesting
    int getLength() {
        return ((BoundedInputStream) this.in).bound;
    }

    //endregion

    //region BoundedInputStream Implementation

    /**
     * InputStream wrapper that counts how many bytes were read and prevents over-reading.
     */
    private static class BoundedInputStream extends FilterInputStream {
        private final int bound;
        private int relativePosition;

        BoundedInputStream(InputStream inputStream, int bound) {
            super(inputStream);
            this.bound = bound;
            this.relativePosition = 0;
        }

        @Override
        public void close() throws IOException {
            // Skip over the remaining bytes. Do not close the underlying InputStream.
            if (this.relativePosition < this.bound) {
                long toSkip = this.bound - this.relativePosition;
                long skipped = skip(toSkip);
                assert skipped == toSkip : "Unable to skip all bytes. Expected " + toSkip + ", actual " + skipped;
            } else if (this.relativePosition > this.bound) {
                throw new SerializationException(String.format("Read more bytes than expected. Expected %d, actual %d.", this.bound, relativePosition));
            }
        }

        @Override
        public int read() throws IOException {
            if (this.relativePosition >= this.bound) {
                // Do not allow reading more than we should.
                return -1;
            }

            int r = super.read();
            if (r >= 0) {
                this.relativePosition++;
            }
            return r;
        }

        @Override
        public int read(byte[] buffer, int offset, int length) throws IOException {
            length = Math.min(length, this.bound - this.relativePosition);
            int r = this.in.read(buffer, offset, length);
            if (r >= 0) {
                this.relativePosition += r;
            }
            return r;
        }

        @Override
        public long skip(long count) throws IOException {
            count = (int) Math.min(count, this.bound - this.relativePosition);
            long r = this.in.skip(count);
            this.relativePosition += r;
            return r;
        }

        @Override
        public int available() throws IOException {
            return Math.min(this.in.available(), this.bound - this.relativePosition);
        }
    }

    //endregion
}