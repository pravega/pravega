/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.common.io;

import com.google.common.base.Preconditions;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import javax.annotation.concurrent.NotThreadSafe;
import lombok.Getter;

/**
 * A wrapper for an InputStream that allows reading up to a specified number of bytes.
 */
@NotThreadSafe
public class BoundedInputStream extends FilterInputStream {
    //region Members

    @Getter
    private final int bound;
    @Getter
    private int remaining;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the BoundedInputStream class.
     *
     * @param inputStream The InputStream to wrap.
     * @param bound       The maximum number of bytes that can be read with this instance.
     */
    public BoundedInputStream(InputStream inputStream, int bound) {
        super(inputStream);
        Preconditions.checkArgument(bound >= 0, "bound must be a non-negative integer.");
        this.bound = bound;
        this.remaining = bound;
    }

    //endregion

    //region InputStream Implementation

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

    @Override
    public int read() throws IOException {
        // Do not allow reading more than we should.
        int r = this.remaining > 0 ? this.in.read() : -1;
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

    @Override
    public boolean markSupported() {
        return false;
    }

    /**
     * Creates a new BoundedInputStream wrapping the same InputStream as this one, starting at the current position, with
     * the given bound.
     * 
     * NOTE: both this instance and the result of this method should not be both used at the same time to read from the
     * InputStream. When this method returns, this instance's remaining count will be reduced by the given bound (that's
     * since upon closing, the BoundedInputStream will auto-advance to its bound position).
     *
     * @param bound The bound of the sub-stream.
     * @return A new instance of a BoundedInputStream.
     */
    public BoundedInputStream subStream(int bound) {
        Preconditions.checkArgument(bound >= 0 && bound <= this.remaining,
                "bound must be a non-negative integer and less than or equal to the remaining length.");
        this.remaining -= bound;
        return new BoundedInputStream(this.in, bound);
    }

    //endregion
}
