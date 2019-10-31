/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.logs;

import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
import io.pravega.common.SimpleMovingAverage;
import io.pravega.common.io.SerializationException;
import io.pravega.common.util.ByteArraySegment;
import java.io.IOException;
import java.io.OutputStream;
import java.util.function.Consumer;
import javax.annotation.concurrent.NotThreadSafe;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * An OutputStream that abstracts writing to Data Frames. Allows writing arbitrary bytes, and seamlessly transitions
 * from one Data Frame to another if the previous Data Frame was full.
 *
 * Data written with this class can be read back using DataFrameInputStream.
 */
@NotThreadSafe
class DataFrameOutputStream extends OutputStream {
    //region Members

    private final Consumer<DataFrame> dataFrameCompleteCallback;
    private DataFrame currentFrame;
    private boolean hasDataInCurrentFrame;
    @Getter
    private boolean closed;
    private final BufferFactory bufferFactory;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the DataFrameOutputStream class.
     *
     * @param maxDataFrameSize          The maximum size, in bytes, of a Data Frame.
     * @param dataFrameCompleteCallback A callback that will be invoked when a Data Frame is full.
     * @throws IllegalArgumentException If maxDataFrameSize is not a positive integer.
     * @throws NullPointerException     If any of the arguments are null.
     */
    DataFrameOutputStream(int maxDataFrameSize, Consumer<DataFrame> dataFrameCompleteCallback) {
        Exceptions.checkArgument(maxDataFrameSize > DataFrame.MIN_ENTRY_LENGTH_NEEDED, "maxDataFrameSize",
                "Must be a at least %s.", DataFrame.MIN_ENTRY_LENGTH_NEEDED);

        this.bufferFactory = new BufferFactory(maxDataFrameSize);
        this.dataFrameCompleteCallback = Preconditions.checkNotNull(dataFrameCompleteCallback, "dataFrameCompleteCallback");
    }

    //endregion

    //region OutputStream Implementation

    @Override
    public void write(int b) throws IOException {
        Exceptions.checkNotClosed(this.closed, this);
        Preconditions.checkState(this.currentFrame != null, "No current frame exists. Most likely no record is started.");

        int attemptCount = 0;
        int totalBytesWritten = 0;
        while (totalBytesWritten == 0 && attemptCount < 2) {
            // We attempt to write 1 byte. If append() says it wrote 0 bytes, it means the current frame is full. Seal it and create a new one.
            totalBytesWritten += this.currentFrame.append((byte) b);
            if (totalBytesWritten == 0) {
                this.currentFrame.endEntry(false); // Close the current entry, and indicate it is not the last one of the record.
                flush();
                createNewFrame();
                startNewRecordInCurrentFrame(false);
            }

            attemptCount++;
        }

        if (totalBytesWritten == 0) {
            throw new SerializationException("Unable to make progress in serializing to DataFrame.");
        }
    }

    @Override
    public void write(byte[] data, int offset, int length) throws IOException {
        Exceptions.checkNotClosed(this.closed, this);
        Preconditions.checkState(this.currentFrame != null, "No current frame exists. Most likely no record is started.");

        int totalBytesWritten = 0;
        int attemptsWithNoProgress = 0;
        while (totalBytesWritten < length) {
            int bytesWritten = this.currentFrame.append(new ByteArraySegment(data, offset + totalBytesWritten, length - totalBytesWritten));
            attemptsWithNoProgress = bytesWritten == 0 ? attemptsWithNoProgress + 1 : 0;
            if (attemptsWithNoProgress > 1) {
                // We had two consecutive attempts to write to a frame with no progress made.
                throw new IOException("Unable to make progress in serializing to DataFrame.");
            }

            // Update positions.
            totalBytesWritten += bytesWritten;
            if (totalBytesWritten < length) {
                // We were only able to write this partially because the current frame is full. Seal it and create a new one.
                this.currentFrame.endEntry(false);
                flush();
                createNewFrame();
                startNewRecordInCurrentFrame(false);
            }
        }
    }

    /**
     * Seals the current frame (if any), and invokes the dataFrameCompleteCallback with the finished frame.
     * If the dataFrameCompleteCallback failed (and threw an exception), the current frame will be sealed and therefore
     * the DataFrameOutputStream may not be usable.
     */
    @Override
    public void flush() {
        Exceptions.checkNotClosed(this.closed, this);
        if (!this.hasDataInCurrentFrame) {
            // Nothing to do.
            return;
        }

        // Seal the current frame for appends.
        this.currentFrame.seal();

        // Invoke the callback. At the end of this, the frame is committed so we can get rid of it.
        if (!this.currentFrame.isEmpty()) {
            // Only flush something if it's not empty.
            this.bufferFactory.markUsed(this.currentFrame.getLength());
            this.dataFrameCompleteCallback.accept(this.currentFrame);
        }

        reset();
    }

    @Override
    public void close() {
        // drop current frame and stop accepting any new operation after this.
        if (!this.closed) {
            this.closed = true;
            this.currentFrame = null;
        }
    }

    //endregion

    //region DataFrameOutputStream Implementation

    /**
     * Indicates to the stream that a new record is about to be started. All subsequent writes will belong to this record.
     * A record may span multiple data frames (and thus have multiple DataFrame entries), but the DataFrameOutputStream
     * abstracts all of that.
     *
     * @throws IOException If we are unable to start a new record.
     */
    void startNewRecord() throws IOException {
        Exceptions.checkNotClosed(this.closed, this);

        // If there is any data in the current frame, seal it and ship it.
        if (this.currentFrame == null) {
            // No active frame, create a new one.
            createNewFrame();
            startNewRecordInCurrentFrame(true);
        } else if (!this.currentFrame.startNewEntry(true)) {
            // Current Frame is full. Need to seal it and start a new one.
            flush();
            createNewFrame();
            startNewRecordInCurrentFrame(true);
        }
    }

    /**
     * Indicates to the stream that the currently open record is now ended.
     */
    void endRecord() {
        Exceptions.checkNotClosed(this.closed, this);
        if (this.currentFrame != null) {
            this.currentFrame.endEntry(true);
        }
    }

    /**
     * Indicates to the stream that the currently open record is discarded. If the record spans multiple frames (and thus
     * has multiple DataFrame Entries), the already committed entries will not be discarded. Instead, the DataFrameReader
     * will detect that such a record was discarded and skip over it upon reading.
     */
    void discardRecord() {
        Exceptions.checkNotClosed(this.closed, this);
        if (this.currentFrame != null) {
            this.currentFrame.discardEntry();
        }
    }

    /**
     * Discards all the data currently accumulated in the current frame.
     */
    void reset() {
        Exceptions.checkNotClosed(this.closed, this);
        this.currentFrame = null;
        this.hasDataInCurrentFrame = false;
    }

    /**
     * Releases any buffers that may be lingering around and are no longer needed.
     */
    void releaseBuffer() {
        Exceptions.checkNotClosed(this.closed, this);
        this.bufferFactory.reset();
    }

    private void createNewFrame() {
        Preconditions.checkState(this.currentFrame == null || this.currentFrame.isSealed(), "Cannot create a new frame if we currently have a non-sealed frame.");

        this.currentFrame = new DataFrame(this.bufferFactory.next());
        this.hasDataInCurrentFrame = false;
    }

    private void startNewRecordInCurrentFrame(boolean firstRecordEntry) throws SerializationException {
        if (!this.currentFrame.startNewEntry(firstRecordEntry)) {
            throw new SerializationException("Unable to start a new record.");
        }

        this.hasDataInCurrentFrame = true;
    }

    //endregion

    /**
     * Buffer Factory for use with DataFrames.
     */
    @RequiredArgsConstructor
    @NotThreadSafe
    private static class BufferFactory {
        private static final int MIN_LENGTH = 1024; // Min amount of space remaining in the buffer when trying to reuse it.
        private final SimpleMovingAverage lastBuffers = new SimpleMovingAverage(10);
        private final int maxLength;
        private byte[] current;
        private int currentUsed;

        /**
         * Gets a ByteArraySegment that can be used as a DataFrame buffer, which wraps a physical buffer (byte array).
         * Tries to reuse the last used physical buffer as much as possible if space allows, otherwise a new byte array
         * will be allocated.
         *
         * @return The ByteArraySegment to use.
         */
        ByteArraySegment next() {
            if (this.current == null) {
                this.current = new byte[this.maxLength];
                this.currentUsed = 0;
            }

            return new ByteArraySegment(this.current, this.currentUsed, this.current.length - this.currentUsed);
        }

        /**
         * Indicates that the given number of bytes have been used in the given buffer.
         *
         * @param length The number of bytes used.
         */
        void markUsed(int length) {
            this.currentUsed += length;
            this.lastBuffers.add(length);
            int minLength = (int) Math.max(MIN_LENGTH, this.lastBuffers.getAverage(0));

            if (this.current != null && (this.current.length - this.currentUsed < minLength)) {
                this.current = null;
            }
        }

        /**
         * Releases the current buffer (if any) and resets the stats. After this method is called, the first call to next()
         * will allocate a new buffer.
         */
        void reset() {
            this.current = null;
            this.lastBuffers.reset();
        }
    }


}

