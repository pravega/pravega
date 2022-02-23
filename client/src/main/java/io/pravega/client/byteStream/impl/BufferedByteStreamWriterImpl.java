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
package io.pravega.client.byteStream.impl;

import com.google.common.annotations.VisibleForTesting;
import io.pravega.client.byteStream.ByteStreamWriter;
import io.pravega.client.stream.impl.PendingEvent;
import io.pravega.common.util.ByteBufferUtils;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import javax.annotation.concurrent.GuardedBy;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Synchronized;

/**
 * This class buffers individual calls to {@link #write(int)} so that we don't make a separate RPC per byte.
 * It attempts to do this in a Lazy way, not allocating buffer space unless it is needed. 
 */
@RequiredArgsConstructor
public class BufferedByteStreamWriterImpl extends ByteStreamWriter {

    @VisibleForTesting
    public static final int BUFFER_SIZE = 4096;
    @NonNull
    private final ByteStreamWriterImpl out;

    @GuardedBy("$lock")
    private ByteBuffer buffer = null;
    
    @Override
    @Synchronized
    public void write(int b) throws IOException {
        if (buffer == null) {
            buffer = ByteBuffer.allocate(BUFFER_SIZE);
        }
        buffer.put((byte) b);
        if (!buffer.hasRemaining()) {
            commitBuffer();
        }
    }

    @Override
    @Synchronized
    public void write(ByteBuffer src) throws IOException {
        commitBuffer();
        int position = src.position();
        while (src.hasRemaining()) {
            int length = Math.min(PendingEvent.MAX_WRITE_SIZE, src.remaining());
            ByteBuffer slice = ByteBufferUtils.slice(src, position, length);
            out.write(slice);
            position += length;
            src.position(position);
        }
    }

    @Override
    @Synchronized
    public void write(byte[] b, int off, int len) throws IOException {
        write(ByteBuffer.wrap(b, off, len));
    }

    private void commitBuffer() throws IOException {
        if (buffer != null) {
            buffer.flip();
            if (buffer.hasRemaining()) {
                out.write(buffer);
                buffer = null;
            }
        }
    }
    
    @Override
    @Synchronized
    public void close() throws IOException {
        commitBuffer();
        out.close();
    }

    @Override
    @Synchronized
    public void flush() throws IOException {
        commitBuffer();
        out.flush();
    }

    @Override
    @Synchronized
    public CompletableFuture<Void> flushAsync() throws IOException {
        commitBuffer();
        return out.flushAsync();
    }

    @Override
    @Synchronized
    public void closeAndSeal() throws IOException {
        commitBuffer();
        out.closeAndSeal();
    }

    @Override
    public long fetchHeadOffset() {
        return out.fetchHeadOffset();
    }

    @Override
    public long fetchTailOffset() {
        return out.fetchTailOffset();
    }

    @Override
    public void truncateDataBefore(long offset) {
        out.truncateDataBefore(offset);
    }

}
