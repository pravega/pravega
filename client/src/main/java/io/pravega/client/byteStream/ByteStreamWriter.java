/**
 * Copyright (c) 2018 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.byteStream;

import io.pravega.client.state.RevisionedStreamClient;
import io.pravega.client.stream.EventStreamWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * Allows for writing raw bytes directly to a segment. This is intended as low level building block
 * for creating higher level components and is not appropriate for most applications to use
 * directly.
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
     * written becomes a memory issue. If this behavior is undesirable the method
     * {@link #setThrowBeforeBlocking(boolean)} can be used to make this call throw an exception
     * instead of blocking.
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
     * Closes the writer similar to {@link #close()} but also seals it so that no future writes can
     * ever be made.
     * 
     * @throws IOException If for any reason the flush fails including if the stream is sealed.
     */
    public abstract void closeAndSeal() throws IOException;

    /**
     * Similar to {@link #flush()} but does not block the calling thread, and instead returns a
     * future that will be completed when the flush is done.
     */
    //TODO:
    //public abstract CompletableFuture<Void> flushAsync();

    /**
     * This makes a synchronous RPC call to the server to obtain the total number of bytes written
     * to the segment in its history. This is the sum total of the bytes written in all calls to
     * {@link #write(byte[])} that have been flushed. It does not include data that was passed to
     * {@link #write(byte[])} but which has not yet been persisted.
     */
    public abstract long fetchOffset();
    
}
