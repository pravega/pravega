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
import io.pravega.common.Exceptions;
import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import org.slf4j.Logger;

/**
 * Miscellaneous operations on Streams.
 */
public final class StreamHelpers {
    /**
     * Reads at most 'maxLength' bytes from the given input stream, as long as the stream still has data to serve.
     * A note about performance:
     * - This uses the default implementation of {@link InputStream#read(byte[], int, int)}, which means in most cases
     * it will copy byte-by-byte into the target array, which is rather inefficient.
     * - See https://github.com/pravega/pravega/issues/2924 for more details.
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
        Preconditions.checkNotNull(target, "target");
        Preconditions.checkElementIndex(startOffset, target.length, "startOffset");
        Exceptions.checkArgument(maxLength >= 0, "maxLength", "must be a non-negative number.");

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
     * @throws IOException If unable to read from the given InputStream. Throws {@link EOFException} if the number of bytes
     * remaining in the InputStream is less than length.
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
    
    /**
     * Closes a stream, logging a warning if it fails.
     * 
     * @param toClose the stream/socket etc to close
     * @param log the logger to log a warning with.
     * @param message the message to log in the event of an exception
     * @param args template args for the message.
     */
    public static void closeQuietly(Closeable toClose, Logger log, String message, Object... args) {
        try {
            toClose.close();
        } catch (IOException e) {
            log.warn(message, args);
        }
    }
    
}
