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
package io.pravega.client.byteStream;

import io.pravega.client.state.RevisionedStreamClient;
import io.pravega.client.stream.EventStreamWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

/**
 * Allows for writing raw bytes directly to a segment. This is intended as low level building block
 * for creating higher level components. As such it can break things.
 * 
 * This class does not frame, attach headers, or otherwise modify the bytes written to it in any
 * way. So unlike {@link EventStreamWriter} or {@link RevisionedStreamClient} the data written
 * cannot be split apart when read. As such, any bytes written by this API can ONLY be read using
 * {@link ByteStreamReader}. Similarly, unless some sort of framing is added it is probably an error
 * to have multiple ByteStreamWriters write to the same segment as this will result in interleaved
 * data.
 * 
 * The methods on this class are non-blocking unless otherwise specified. As such data passed to a
 * {@link #write(byte[])} call cannot be assumed to be persisted until a flush has been called.
 * 
 * It is safe to invoke methods on this class from multiple threads but doing so will not result in
 * an increase in performance.
 */
public abstract class ByteStreamWriter extends OutputStream {

    /**
     * Similar to {@link #write(byte[], int, int)}
     * 
     * Writes the provided data to the segment. The data is buffered internally to avoid blocking.
     * As such it cannot be assumed to be durably stored until a flush completes.
     * 
     * It is intended that this method not block, but it may in the event that the server becomes
     * disconnected for sufficiently long or is sufficiently slow that that backlog of data to be
     * written becomes a memory issue. 
     * 
     * @param src The bytes to write.
     * @throws IOException If for any reason an error occurs writing the data, including if the
     *             stream is sealed.
     */
    public abstract void write(ByteBuffer src) throws IOException;

    /**
     * Writes the provided data to the segment. The data is buffered internally to avoid blocking.
     * As such it cannot be assumed to be durably stored until a flush completes.
     * 
     * It is intended that this method not block, but it may in the event that the server becomes
     * disconnected for sufficiently long or is sufficiently slow that that backlog of data to be
     * written becomes a memory issue. 
     * 
     * @see java.io.OutputStream#write(byte[], int, int)
     */
    @Override
    public abstract void write(byte[] b, int off, int len) throws IOException;

    /**
     * Flushes the buffer and closes the writer. If there is data to flush, this is a blocking
     * method.
     * 
     * @see java.io.OutputStream#close()
     */
    @Override
    public abstract void close() throws IOException;

    /**
     * Blocks until all data written has been durably persisted.
     * 
     * @see java.io.OutputStream#flush()
     * @throws IOException If for any reason the flush fails including if the stream is sealed.
     */
    @Override
    public abstract void flush() throws IOException;

    /**
     * The future will complete successfully when all data which was passed to
     * the write prior to calling this method has been persisted, and will complete exceptionally if that
     * is not possible such as for example if the segment is sealed.
     *
     * @see java.io.OutputStream#flush()
     * @return The future related to last write
     * @throws IOException If for any reason the flush fails including if the stream is sealed.
     */
    public abstract CompletableFuture<Void> flushAsync() throws IOException;


    /**
     * Closes the writer similar to {@link #close()} but also seals it so that no future writes can
     * ever be made.
     * 
     * @throws IOException If for any reason the flush fails including if the stream is sealed.
     */
    public abstract void closeAndSeal() throws IOException;

    /**
     * This makes a synchronous RPC call to the server to obtain the current head of the stream.
     * @return The current head offset
     */
    public abstract long fetchHeadOffset();

    /**
     * This makes a synchronous RPC call to the server to obtain the total number of bytes written
     * to the segment in its history. This is the sum total of the bytes written in all calls to
     * {@link #write(byte[])} that have been flushed. It does not include data that was passed to
     * {@link #write(byte[])} but which has not yet been persisted.
     * @return The tail offset
     */
    public abstract long fetchTailOffset();

    /**
     * This makes a synchronous RPC call to the server to truncate the segment at the provided offset.
     * @param offset The truncation offset.
     */
    public abstract void truncateDataBefore(long offset);
    
}
