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
import javax.annotation.concurrent.ThreadSafe;

/**
 * Allows for reading raw bytes from a segment. This class is designed such that it can be used with
 * or without blocking. To avoid blocking use the {@link #onDataAvailable()} method to make sure to
 * only call {@link #read(byte[])} when there is data {@link #available()}.
 *
 * It is safe to invoke methods on this class from multiple threads, but doing so will not increase
 * performance.
 */
@ThreadSafe
public abstract class ByteStreamReader extends InputStream implements AsynchronousChannel, AutoCloseable {

    /**
     * Returns the current byte offset in the segment. This call does not block.
     */
    public abstract long getOffset();

    /**
     * Seeks to the provided offset (It can be anywhere in the segment). Future read calls will read
     * from this offset. Future reads will proceed from this offset.
     * 
     * @param offset The offset to seek to.
     */
    public abstract void seekToOffset(long offset);

    /**
     * Returns the number of bytes that can be read without blocking. If the number returned is > 0
     * then a call to {@link #read(byte[]))} will return data from memory without blocking. If the
     * number returned is 0 then {@link #read(byte[]))} will block. If -1 is returned this indicates
     * the end of the stream has been reached and a call to {@link #read(byte[])} will return -1.
     * 
     * @see java.io.InputStream#available()
     */
    @Override
    public abstract int available();

    /**
     * This make an RPC to the server to fetch the offset at which new bytes would be written. This
     * is the same as the length of the segment (assuming no truncation). This offset can also be
     * passed to {@link #seekToOffset(long)} to only read bytes from this point forward.
     */
    public abstract long fetchTailOffset();

    /**
     * Reads a single byte. 
     * Avoid this API if possible as it is very wasteful.
     * {@see InputStream#read()}. 
     */
    @Override
    public abstract int read() throws IOException;

    /**
     * This is equivalent to calling {@code read(b, 0, b.length) }
     * 
     * Will only block if {@link #available()} is 0.
     * {@see InputStream#read(byte[]))}. 
     */
    @Override
    public abstract int read(byte[] b) throws IOException;

    /**
     * If {@link #available()} is non-zero, this method will read bytes from an in-memory buffer into the
     * provided array. If {@link #available()} is zero will wait for additional data to arrive and
     * then fill the provided array. This method will only block if {@link #available()} is 0. In
     * which case it will block until some data arrives and return that. (Which may or may not fill
     * the provided buffer)
     * 
     * @return The number of bytes copied into the provided buffer. Or -1 if the segment is sealed
     *         and there are no more bytes to read.
     * @see java.io.InputStream#read(byte[], int, int)
     */
    @Override
    public abstract int read(byte[] b, int off, int len) throws IOException;

    /**
     * Similar to {@link #read(byte[], int, int)} but takes a byteBuffer.
     * 
     * @param dst the destination buffer to read into.
     * @throws IOException If the stream cannot be read from for any reason including if truncation
     *             has deleted the data.
     */
    public abstract int read(ByteBuffer dst) throws IOException;

    /**
     * This method attempts to skip forward by the provided number of bytes. If it is not possible
     * to skip forward `n` bytes (because there are less than `n` bytes remaining, it will skip as
     * many as possible and return the number skipped.
     * 
     * This method is not affected by truncation.
     * 
     * @throws IOException Thrown if an IOError occurs while attempting to obtain the length of the
     *             stream.
     */
    @Override
    public abstract long skip(long n) throws IOException;

    /**
     * Closes the reader. 
     * This may block on an ongoing {@link #read(ByteBuffer)} request if there is one.
     * @see java.io.InputStream#close()
     */
    @Override
    public abstract void close();

    /**
     * Returns a future that will be completed when there is data available to be read. The Integer
     * in the result will be the number of bytes {@link #available()} or -1 if the reader has
     * reached the end of a sealed segment.
     * 
     * @return A the number of bytes {@link #available()}
     */
    public abstract CompletableFuture<Integer> onDataAvailable();

}
