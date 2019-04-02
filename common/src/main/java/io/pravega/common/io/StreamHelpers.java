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
import io.pravega.common.Exceptions;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

/**
 * Miscellaneous operations on Streams.
 */
public final class StreamHelpers {
    /**
     * Reads at most 'maxLength' bytes from the given input stream, as long as the stream still has data to serve.
     *
     * @param stream      The InputStream to read from.
     * @param target      The target array to write data to.
     * @param startOffset The offset within the target array to start writing data to.
     * @param maxLength   The maximum number of bytes to copy.
     * @return The number of bytes copied.
     * @throws IOException If unable to read from the given stream.
     */
    public static int readAll(InputStream stream, byte[] target, int startOffset, int maxLength) throws IOException {
        Preconditions.checkNotNull(stream, "stream");
        Preconditions.checkNotNull(stream, "target");
        Preconditions.checkElementIndex(startOffset, target.length, "startOffset");
        Exceptions.checkArgument(maxLength >= 0, "maxLength", "maxLength must be a non-negative number.");

        int totalBytesRead = 0;
        while (totalBytesRead < maxLength) {
            int bytesRead = stream.read(target, startOffset + totalBytesRead, maxLength - totalBytesRead);
            if (bytesRead < 0) {
                // End of stream/
                break;
            }

            totalBytesRead += bytesRead;
        }

        return totalBytesRead;
    }

    /**
     * Reads a number of bytes from the given InputStream and returns it as the given byte array.
     *
     * @param source The InputStream to read.
     * @param length The number of bytes to read.
     * @return A byte array containing the contents of the Stream.
     * @throws EOFException If the number of bytes remaining in the InputStream is less than length.
     * @throws IOException  If unable to read from the given InputStream.
     */
    public static byte[] readAll(InputStream source, int length) throws IOException {
        byte[] ret = new byte[length];
        int readBytes = readAll(source, ret, 0, ret.length);
        if (readBytes < ret.length) {
            throw new EOFException(String.format(
                    "Was only able to read %d bytes, which is less than the requested length of %d.", readBytes, ret.length));
        }

        return ret;
    }
}
