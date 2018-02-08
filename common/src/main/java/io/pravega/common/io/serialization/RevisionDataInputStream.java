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

import io.pravega.common.io.SerializationException;
import java.io.DataInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import lombok.Getter;

/**
 * A DataInputStream that is used for deserializing Serialization Revisions. Instances of this class should be used to
 * read data that was serialized using an instance of RevisionDataOutput (i.e., NonSeekableRevisionDataOutput or
 * RandomRevisionDataOutput).
 */
class RevisionDataInputStream extends DataInputStream implements RevisionDataInput {
    //region Members

    private final int length;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the RevisionDataInputStream class. Upon a successful call to this constructor, 4 bytes
     * will have been read from the InputStream representing the expected length of the serialization.
     *
     * @param inputStream The InputStream to wrap.
     * @throws IOException If an IO Exception occurred.
     */
    RevisionDataInputStream(InputStream inputStream) throws IOException {
        super(new CountableInputStream(inputStream));
        this.length = readInt();
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() throws IOException {
        // We purposefully do not call super.close() as that will auto-close the underlying InputStream, which is not
        // what we want.
        skipRemaining();
    }

    //endregion

    //region RevisionDataInput Implementation

    @Override
    public InputStream getBaseStream() {
        return this;
    }

    /**
     * Skips remaining unread bytes in this serialization.
     *
     * @throws IOException If an IO Exception occurred.
     */
    void skipRemaining() throws IOException {
        long rp = ((CountableInputStream) this.in).getRelativePosition();
        if (rp < this.length) {
            long skipped = skip(this.length - rp);
            assert skipped == this.length - rp : "Unable to skip all bytes.";
        } else if (rp > this.length) {
            throw new SerializationException(String.format("Read more bytes than expected. Expected %d, actual %d.", this.length, rp));
        }
    }

    //endregion

    //region CountableInputStream Implementation

    /**
     * InputStream wrapper that counts how many bytes were read.
     */
    private static class CountableInputStream extends FilterInputStream {
        @Getter
        private long relativePosition;

        CountableInputStream(InputStream inputStream) {
            super(inputStream);
            this.relativePosition = 0;
        }

        @Override
        public int read() throws IOException {
            int r = super.read();
            if (r >= 0) {
                this.relativePosition++;
            }
            return r;
        }

        @Override
        public int read(byte[] buffer) throws IOException {
            int r = this.read(buffer, 0, buffer.length);
            if (r >= 0) {
                this.relativePosition += r;
            }
            return r;
        }

        @Override
        public int read(byte[] buffer, int offset, int length) throws IOException {
            int r = this.in.read(buffer, offset, length);
            if (r >= 0) {
                this.relativePosition += r;
            }
            return r;
        }

        @Override
        public long skip(long count) throws IOException {
            long r = this.in.skip(count);
            this.relativePosition += r;
            return r;
        }
    }

    //endregion
}