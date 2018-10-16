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

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousChannel;
import java.util.concurrent.CompletableFuture;

/**
 * Allows for reading raw bytes from a segment. This class is designed such that it can be used with
 * or without blocking. To avoid blocking use the {@link #onDataAvailable()} method to make sure to
 * only call {@link #read(byte[])} when there is data {@link #available()}.
 *
 * It is safe to invoke methods on this class from multiple threads, but doing so will not increase
 * performance.
 */
public abstract class ByteStreamReader extends InputStream implements AsynchronousChannel, AutoCloseable {

    /**
     * Returns the current byte offset in the segment. This call does not block.
     */
    public abstract long getOffset();

    /**
     * Jumps to the provided offset. Future read calls will read from this position. This makes a
     * synchronous RPC to the server to validate the offset provided.
     * 
     * @param offset The offset to jump to.
     * @throws InvalidOffsetException If the offset provided does not exist in the segment.
     */
    public abstract void jumpToOffset(long offset) throws InvalidOffsetException;

    /**
     * Returns the number of bytes that can be read without blocking. If the number returned is > 0
     * then a call to {@link #read(byte[]))} will return data from memory without blocking. If the
     * number returned is 0 then {@link #read(byte[]))} will block If -1 is returned this indicates
     * the end of the stream has been reached and a call to {@link #read(byte[])} will return -1.
     * 
     * @see java.io.InputStream#available()
     */
    @Override
    public abstract int available();

    /**
     * This make a RPC to the server to fetch the highest offset in the segment.
     */
    public abstract long fetchTailOffset();

    /**
     * Don't call this. It is very wasteful.
     */
    @Override
    public abstract int read() throws IOException;

    /**
     * See {@link InputStream#read(byte[])}. This is equivlent to calling
     * {@code read(b, 0, b.length) }
     * 
     * Will only block if {@link #available()} is 0.
     */
    @Override
    public abstract int read(byte b[]) throws IOException;

    /**
     * If {@link #available()} is non-zero will read bytes out of a in-memory buffer into the
     * provided array. If {@link #available()} is zero will wait for additional data to arrive and
     * then fill the provided array. This method will only block if {@link #available()} is 0.
     * 
     * @return The number of bytes copied into the provided buffer. Or -1 if the stream is sealed
     *         and there are no more bytes to read.
     * @see java.io.InputStream#read(byte[], int, int)
     */
    @Override
    public abstract int read(byte b[], int off, int len) throws IOException;

    /**
     * Similar to {@link #read(byte[], int, int)} but takes a byteBuffer.
     */
    public abstract int read(ByteBuffer dst) throws IOException;

    /**
     * This method skips forward by the provided number of bytes. This method is non-blocking but
     * may not be able to skip n bytes.
     * 
     * @see java.io.InputStream#skip(long) in such a case it will return the number of bytes it
     *      skipped. It may be preferable to call {@link #jumpToOffset(long)} for large jumps are
     *      that does not have this property.
     */
    @Override
    public abstract long skip(long n);

    /**
     * @see java.io.InputStream#close()
     */
    @Override
    public abstract void close();

    /**
     * Returns a future that will be completed when there is data available to be read. The Integer
     * in the result will be the number of bytes {@link #available()} or -1 if the reader has
     * reached the end of a sealed segment.
     */
    public abstract CompletableFuture<Integer> onDataAvailable();

}
