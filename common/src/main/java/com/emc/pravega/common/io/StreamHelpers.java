/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.common.io;

import com.emc.pravega.common.Exceptions;
import com.google.common.base.Preconditions;

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
}
